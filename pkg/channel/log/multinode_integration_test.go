package log

import (
	"context"
	"fmt"
	"net"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/channel/isr"
	isrnode "github.com/WuKongIM/WuKongIM/pkg/channel/node"
	isrnodetransport "github.com/WuKongIM/WuKongIM/pkg/channel/transport"
	"github.com/WuKongIM/WuKongIM/pkg/transport"
	"github.com/stretchr/testify/require"
)

func TestThreeNodeClusterAppendCommitsBeforeAckAndSurvivesFollowerRestart(t *testing.T) {
	harness := newThreeNodeChannelHarness(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	result, err := harness.leader.cluster.Append(ctx, AppendRequest{
		ChannelID:   harness.key.ChannelID,
		ChannelType: harness.key.ChannelType,
		Message: Message{
			ChannelID:   harness.key.ChannelID,
			ChannelType: harness.key.ChannelType,
			FromUID:     "sender",
			ClientMsgNo: "three-node-1",
			Payload:     []byte("hello durable cluster"),
		},
	})
	require.NoError(t, err)
	require.Equal(t, uint64(1), result.MessageSeq)
	require.NotZero(t, result.MessageID)

	status, err := harness.leader.cluster.Status(harness.key)
	require.NoError(t, err)
	require.Equal(t, uint64(1), status.HW)
	require.Equal(t, uint64(1), status.CommittedSeq)

	for _, node := range harness.nodes {
		msg := waitForCommittedMessage(t, node.db.ForChannel(harness.key), result.MessageSeq, 5*time.Second)
		require.Equal(t, result.MessageSeq, msg.MessageSeq)
		require.Equal(t, []byte("hello durable cluster"), msg.Payload)
	}

	restarted := harness.restartFollower(t, 2)
	defer restarted.close(t)

	reloadedReplica, err := NewReplica(restarted.db.ForChannel(harness.key), isr.NodeID(restarted.id), func() time.Time {
		return time.Now()
	})
	require.NoError(t, err)

	restartedStatus := reloadedReplica.Status()
	require.Equal(t, uint64(1), restartedStatus.HW)
	require.Equal(t, uint64(1), restartedStatus.LEO)

	restartedMsg, err := restarted.db.ForChannel(harness.key).LoadMsg(result.MessageSeq)
	require.NoError(t, err)
	require.Equal(t, []byte("hello durable cluster"), restartedMsg.Payload)
}

func TestThreeNodeClusterHarnessRestartNodeReopensData(t *testing.T) {
	harness := newThreeNodeChannelHarness(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	result, err := harness.leader.cluster.Append(ctx, AppendRequest{
		ChannelID:   harness.key.ChannelID,
		ChannelType: harness.key.ChannelType,
		Message: Message{
			ChannelID:   harness.key.ChannelID,
			ChannelType: harness.key.ChannelType,
			FromUID:     "sender",
			ClientMsgNo: "restart-harness-1",
			Payload:     []byte("persisted before restart"),
		},
	})
	require.NoError(t, err)

	harness.stopNode(t, 2)
	restarted := harness.restartNode(t, 2)

	require.Equal(t, harness.specs[2].dir, restarted.dir)
	require.Equal(t, harness.addrs[2], restarted.addr)

	msg := waitForCommittedMessage(t, restarted.db.ForChannel(harness.key), result.MessageSeq, 5*time.Second)
	require.Equal(t, []byte("persisted before restart"), msg.Payload)
}

func TestThreeNodeClusterBlocksCommitUntilMinISRRecovers(t *testing.T) {
	harness := newThreeNodeChannelHarness(t)

	harness.stopNode(t, 2)

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, err := harness.leader.cluster.Append(ctx, AppendRequest{
		ChannelID:   harness.key.ChannelID,
		ChannelType: harness.key.ChannelType,
		Message: Message{
			ChannelID:   harness.key.ChannelID,
			ChannelType: harness.key.ChannelType,
			FromUID:     "sender",
			ClientMsgNo: "minisr-timeout-1",
			Payload:     []byte("commit after isr recovers"),
		},
	})
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.GreaterOrEqual(t, time.Since(start), 250*time.Millisecond)

	status, err := harness.leader.cluster.Status(harness.key)
	require.NoError(t, err)
	require.Equal(t, uint64(0), status.HW)
	require.Equal(t, uint64(0), status.CommittedSeq)

	assertCommittedMessageAbsent(t, harness.nodes[1].db.ForChannel(harness.key), 1)
	assertCommittedMessageAbsent(t, harness.nodes[3].db.ForChannel(harness.key), 1)

	harness.restartNode(t, 2)

	for _, node := range harness.nodes {
		msg := waitForCommittedMessage(t, node.db.ForChannel(harness.key), 1, 5*time.Second)
		require.Equal(t, []byte("commit after isr recovers"), msg.Payload)
	}
}

func TestFollowerRestartCatchesUpAfterLeaderProgress(t *testing.T) {
	harness := newThreeNodeChannelHarnessWithMinISR(t, 2)

	harness.stopNode(t, 2)

	const writes = 5
	for i := 1; i <= writes; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		payload := []byte(fmt.Sprintf("payload-%d", i))
		res, err := harness.leader.cluster.Append(ctx, AppendRequest{
			ChannelID:   harness.key.ChannelID,
			ChannelType: harness.key.ChannelType,
			Message: Message{
				ChannelID:   harness.key.ChannelID,
				ChannelType: harness.key.ChannelType,
				FromUID:     "sender",
				ClientMsgNo: fmt.Sprintf("catchup-%d", i),
				Payload:     payload,
			},
		})
		cancel()
		require.NoError(t, err)
		require.Equal(t, uint64(i), res.MessageSeq)
	}

	restarted := harness.restartNode(t, 2)

	for seq := uint64(1); seq <= writes; seq++ {
		msg := waitForCommittedMessage(t, restarted.db.ForChannel(harness.key), seq, 5*time.Second)
		require.Equal(t, []byte(fmt.Sprintf("payload-%d", seq)), msg.Payload)
	}

	reloadedReplica, err := NewReplica(restarted.db.ForChannel(harness.key), isr.NodeID(restarted.id), func() time.Time {
		return time.Now()
	})
	require.NoError(t, err)

	restartedStatus := reloadedReplica.Status()
	require.Equal(t, uint64(writes), restartedStatus.HW)
	require.Equal(t, uint64(writes), restartedStatus.LEO)
}

type threeNodeChannelHarness struct {
	key         ChannelKey
	channelMeta ChannelMeta
	groupMeta   isr.GroupMeta
	addrs       map[uint64]string
	specs       map[NodeID]channelNodeSpec
	nodes       map[NodeID]*threeNodeChannelNode
	leader      *threeNodeChannelNode
}

type threeNodeChannelNode struct {
	id      NodeID
	dir     string
	addr    string
	db      *DB
	runtime isrnode.Runtime
	cluster *cluster
	server  *transport.Server
	pool    *transport.Pool
	client  *transport.Client
}

type channelNodeSpec struct {
	dir string
}

func newThreeNodeChannelHarness(t *testing.T) *threeNodeChannelHarness {
	t.Helper()
	return newThreeNodeChannelHarnessWithMinISR(t, 3)
}

func newThreeNodeChannelHarnessWithMinISR(t *testing.T, minISR int) *threeNodeChannelHarness {
	t.Helper()

	addrs := reserveNodeAddrs(t, 3)
	key := ChannelKey{ChannelID: "three-node-durable", ChannelType: 1}
	channelMeta := ChannelMeta{
		ChannelID:    key.ChannelID,
		ChannelType:  key.ChannelType,
		ChannelEpoch: 9,
		LeaderEpoch:  4,
		Replicas:     []NodeID{1, 2, 3},
		ISR:          []NodeID{1, 2, 3},
		Leader:       1,
		MinISR:       minISR,
		Status:       ChannelStatusActive,
		Features: ChannelFeatures{
			MessageSeqFormat: MessageSeqFormatLegacyU32,
		},
	}
	groupMeta := isr.GroupMeta{
		GroupKey:   GroupKeyForChannel(key),
		Epoch:      channelMeta.LeaderEpoch,
		Leader:     isr.NodeID(channelMeta.Leader),
		Replicas:   []isr.NodeID{1, 2, 3},
		ISR:        []isr.NodeID{1, 2, 3},
		MinISR:     channelMeta.MinISR,
		LeaseUntil: time.Now().Add(time.Minute),
	}

	harness := &threeNodeChannelHarness{
		key:         key,
		channelMeta: channelMeta,
		groupMeta:   groupMeta,
		addrs:       addrs,
		specs:       make(map[NodeID]channelNodeSpec, 3),
		nodes:       make(map[NodeID]*threeNodeChannelNode, 3),
	}
	root := t.TempDir()
	for nodeID := 1; nodeID <= 3; nodeID++ {
		id := NodeID(nodeID)
		dir := filepath.Join(root, fmt.Sprintf("node-%d", nodeID))
		harness.specs[id] = channelNodeSpec{dir: dir}

		node := newThreeNodeChannelNode(t, id, addrs, dir, key)
		require.NoError(t, node.runtime.EnsureGroup(groupMeta))
		require.NoError(t, node.cluster.ApplyMeta(channelMeta))
		harness.nodes[node.id] = node
		if node.id == channelMeta.Leader {
			harness.leader = node
		}
	}
	require.NotNil(t, harness.leader)
	return harness
}

func (h *threeNodeChannelHarness) runningNodes() []*threeNodeChannelNode {
	nodes := make([]*threeNodeChannelNode, 0, len(h.nodes))
	for _, nodeID := range []NodeID{1, 2, 3} {
		if node := h.nodes[nodeID]; node != nil {
			nodes = append(nodes, node)
		}
	}
	return nodes
}

func newThreeNodeChannelNode(t *testing.T, nodeID NodeID, addrs map[uint64]string, dir string, key ChannelKey) *threeNodeChannelNode {
	t.Helper()

	db, err := Open(dir)
	require.NoError(t, err)

	server := transport.NewServer()
	mux := transport.NewRPCMux()
	server.HandleRPCMux(mux)
	require.NoError(t, server.Start(addrs[uint64(nodeID)]))

	pool := transport.NewPool(staticDiscovery{addrs: addrs}, 1, 5*time.Second)
	client := transport.NewClient(pool)
	bridge := newTestISRBridge()

	runtime, err := isrnode.New(isrnode.Config{
		LocalNode: isr.NodeID(nodeID),
		ReplicaFactory: singleChannelReplicaFactory{
			db:        db,
			key:       key,
			localNode: isr.NodeID(nodeID),
		},
		GenerationStore:  newTestGenerationStore(),
		Transport:        bridge,
		PeerSessions:     bridge,
		AutoRunScheduler: true,
		Limits: isrnode.Limits{
			MaxFetchInflightPeer: 1,
			MaxSnapshotInflight:  1,
		},
		Tombstones: isrnode.TombstonePolicy{
			TombstoneTTL: time.Minute,
		},
	})
	require.NoError(t, err)

	adapter, err := isrnodetransport.New(isrnodetransport.Options{
		LocalNode:    isr.NodeID(nodeID),
		Client:       client,
		RPCMux:       mux,
		FetchService: runtime,
	})
	require.NoError(t, err)
	bridge.bind(adapter)

	clusterPort, err := New(Config{
		Runtime:    testChannelLogRuntime{runtime: runtime},
		Log:        db,
		States:     db.StateStoreFactory(),
		MessageIDs: &fakeMessageIDGenerator{},
	})
	require.NoError(t, err)

	node := &threeNodeChannelNode{
		id:      nodeID,
		dir:     dir,
		addr:    addrs[uint64(nodeID)],
		db:      db,
		runtime: runtime,
		cluster: clusterPort.(*cluster),
		server:  server,
		pool:    pool,
		client:  client,
	}
	t.Cleanup(func() {
		node.close(t)
	})
	return node
}

func (h *threeNodeChannelHarness) restartFollower(t *testing.T, nodeID NodeID) *threeNodeChannelNode {
	t.Helper()

	h.stopNode(t, nodeID)
	return h.restartNode(t, nodeID)
}

func (h *threeNodeChannelHarness) stopNode(t *testing.T, nodeID NodeID) {
	t.Helper()

	node := h.nodes[nodeID]
	require.NotNil(t, node, "node %d is not running", nodeID)
	if node.runtime != nil {
		require.NoError(t, node.runtime.RemoveGroup(h.groupMeta.GroupKey))
	}
	node.close(t)
	h.nodes[nodeID] = nil
	if h.leader != nil && h.leader.id == nodeID {
		h.leader = nil
	}
}

func (h *threeNodeChannelHarness) restartNode(t *testing.T, nodeID NodeID) *threeNodeChannelNode {
	t.Helper()

	require.Nil(t, h.nodes[nodeID], "node %d is already running", nodeID)

	spec, ok := h.specs[nodeID]
	require.True(t, ok, "missing spec for node %d", nodeID)

	node := newThreeNodeChannelNode(t, nodeID, h.addrs, spec.dir, h.key)
	require.NoError(t, node.runtime.EnsureGroup(h.groupMeta))
	require.NoError(t, node.cluster.ApplyMeta(h.channelMeta))

	h.nodes[nodeID] = node
	if node.id == h.channelMeta.Leader {
		h.leader = node
	}
	return node
}

func (n *threeNodeChannelNode) close(t *testing.T) {
	t.Helper()

	if n.client != nil {
		n.client.Stop()
		n.client = nil
	}
	if n.pool != nil {
		n.pool.Close()
		n.pool = nil
	}
	if n.server != nil {
		n.server.Stop()
		n.server = nil
	}
	if n.db != nil {
		require.NoError(t, n.db.Close())
		n.db = nil
	}
	n.runtime = nil
	n.cluster = nil
}

type singleChannelReplicaFactory struct {
	db        *DB
	key       ChannelKey
	localNode isr.NodeID
}

func (f singleChannelReplicaFactory) New(cfg isrnode.GroupConfig) (isr.Replica, error) {
	return NewReplica(f.db.ForChannel(f.key), f.localNode, func() time.Time {
		return time.Now()
	})
}

type testGenerationStore struct {
	mu     sync.Mutex
	values map[isr.GroupKey]uint64
}

func newTestGenerationStore() *testGenerationStore {
	return &testGenerationStore{values: make(map[isr.GroupKey]uint64)}
}

func (s *testGenerationStore) Load(groupKey isr.GroupKey) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.values[groupKey], nil
}

func (s *testGenerationStore) Store(groupKey isr.GroupKey, generation uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.values[groupKey] = generation
	return nil
}

type testISRBridge struct {
	mu      sync.RWMutex
	adapter *isrnodetransport.Adapter
	handler func(isrnode.Envelope)
}

type noopPeerSession struct{}

func newTestISRBridge() *testISRBridge {
	return &testISRBridge{}
}

func (t *testISRBridge) Send(peer isr.NodeID, env isrnode.Envelope) error {
	t.mu.RLock()
	adapter := t.adapter
	t.mu.RUnlock()
	if adapter == nil {
		return fmt.Errorf("channellog test: data plane not ready")
	}
	return t.adapter.Send(peer, env)
}

func (t *testISRBridge) RegisterHandler(fn func(isrnode.Envelope)) {
	t.mu.Lock()
	t.handler = fn
	adapter := t.adapter
	t.mu.Unlock()
	if adapter != nil {
		adapter.RegisterHandler(fn)
	}
}

func (t *testISRBridge) Session(peer isr.NodeID) isrnode.PeerSession {
	t.mu.RLock()
	adapter := t.adapter
	t.mu.RUnlock()
	if adapter == nil {
		return noopPeerSession{}
	}
	return adapter.SessionManager().Session(peer)
}

func (t *testISRBridge) bind(adapter *isrnodetransport.Adapter) {
	t.mu.Lock()
	t.adapter = adapter
	handler := t.handler
	t.mu.Unlock()
	if handler != nil {
		adapter.RegisterHandler(handler)
	}
}

func (noopPeerSession) Send(isrnode.Envelope) error {
	return fmt.Errorf("channellog test: data plane not ready")
}

func (noopPeerSession) TryBatch(isrnode.Envelope) bool {
	return false
}

func (noopPeerSession) Flush() error {
	return nil
}

func (noopPeerSession) Backpressure() isrnode.BackpressureState {
	return isrnode.BackpressureState{}
}

func (noopPeerSession) Close() error {
	return nil
}

type testChannelLogRuntime struct {
	runtime isrnode.Runtime
}

func (r testChannelLogRuntime) Group(groupKey isr.GroupKey) (GroupHandle, bool) {
	group, ok := r.runtime.Group(groupKey)
	if !ok {
		return nil, false
	}
	return testGroupHandle{group: group}, true
}

type testGroupHandle struct {
	group isrnode.GroupHandle
}

func (h testGroupHandle) Append(ctx context.Context, records []isr.Record) (isr.CommitResult, error) {
	return h.group.Append(ctx, records)
}

func (h testGroupHandle) Status() isr.ReplicaState {
	return h.group.Status()
}

type staticDiscovery struct {
	addrs map[uint64]string
}

func (d staticDiscovery) Resolve(nodeID uint64) (string, error) {
	addr, ok := d.addrs[nodeID]
	if !ok {
		return "", transport.ErrNodeNotFound
	}
	return addr, nil
}

func reserveNodeAddrs(t *testing.T, count int) map[uint64]string {
	t.Helper()

	addrs := make(map[uint64]string, count)
	for i := 0; i < count; i++ {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		addrs[uint64(i+1)] = ln.Addr().String()
		require.NoError(t, ln.Close())
	}
	return addrs
}

func waitForCommittedMessage(t *testing.T, store *Store, seq uint64, timeout time.Duration) Message {
	t.Helper()

	var msg Message
	require.Eventually(t, func() bool {
		loaded, err := store.LoadMsg(seq)
		if err != nil {
			return false
		}
		msg = loaded
		return true
	}, timeout, 10*time.Millisecond)
	return msg
}

func assertCommittedMessageAbsent(t *testing.T, store *Store, seq uint64) {
	t.Helper()

	_, err := store.LoadMsg(seq)
	require.ErrorIs(t, err, ErrMessageNotFound)
}
