package channellog

import (
	"context"
	"fmt"
	"net"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/replication/isr"
	"github.com/WuKongIM/WuKongIM/pkg/replication/isrnode"
	"github.com/WuKongIM/WuKongIM/pkg/replication/isrnodetransport"
	"github.com/WuKongIM/WuKongIM/pkg/transport/nodetransport"
	"github.com/stretchr/testify/require"
)

func TestThreeNodeClusterSendCommitsBeforeAckAndSurvivesFollowerRestart(t *testing.T) {
	harness := newThreeNodeChannelHarness(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	result, err := harness.leader.cluster.Send(ctx, SendRequest{
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

type threeNodeChannelHarness struct {
	key         ChannelKey
	channelMeta ChannelMeta
	groupMeta   isr.GroupMeta
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
	server  *nodetransport.Server
	pool    *nodetransport.Pool
	client  *nodetransport.Client
}

func newThreeNodeChannelHarness(t *testing.T) *threeNodeChannelHarness {
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
		MinISR:       3,
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
		nodes:       make(map[NodeID]*threeNodeChannelNode, 3),
	}
	root := t.TempDir()
	for nodeID := 1; nodeID <= 3; nodeID++ {
		node := newThreeNodeChannelNode(t, NodeID(nodeID), addrs, filepath.Join(root, fmt.Sprintf("node-%d", nodeID)), key)
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

func newThreeNodeChannelNode(t *testing.T, nodeID NodeID, addrs map[uint64]string, dir string, key ChannelKey) *threeNodeChannelNode {
	t.Helper()

	db, err := Open(dir)
	require.NoError(t, err)

	server := nodetransport.NewServer()
	mux := nodetransport.NewRPCMux()
	server.HandleRPCMux(mux)
	require.NoError(t, server.Start(addrs[uint64(nodeID)]))

	pool := nodetransport.NewPool(staticDiscovery{addrs: addrs}, 1, 5*time.Second)
	client := nodetransport.NewClient(pool)
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

	node := h.nodes[nodeID]
	require.NotNil(t, node)
	node.close(t)

	db, err := Open(node.dir)
	require.NoError(t, err)

	return &threeNodeChannelNode{
		id:   node.id,
		dir:  node.dir,
		addr: node.addr,
		db:   db,
	}
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
		return "", nodetransport.ErrNodeNotFound
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

func waitForCommittedMessage(t *testing.T, store *Store, seq uint64, timeout time.Duration) ChannelMessage {
	t.Helper()

	var msg ChannelMessage
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
