package controllerraft

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/WuKongIM/WuKongIM/pkg/cluster/groupcontroller"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/raftcluster"
	"github.com/WuKongIM/WuKongIM/pkg/replication/multiraft"
	"github.com/WuKongIM/WuKongIM/pkg/storage/controllermeta"
	"github.com/WuKongIM/WuKongIM/pkg/storage/raftstorage"
	"github.com/WuKongIM/WuKongIM/pkg/transport/nodetransport"
	raft "go.etcd.io/raft/v3"
)

func TestServiceBootstrapsOnlyOnSmallestDerivedPeer(t *testing.T) {
	env := newTestEnv(t, []uint64{1, 2, 3})
	defer env.stopAll()

	counts := env.captureBootstrapCalls(t)

	env.startNode(t, 2, nil)
	env.startNode(t, 3, nil)
	require.Equal(t, map[uint64]int{}, counts.snapshot())

	env.startNode(t, 1, nil)
	env.waitForLeader(t, []uint64{1, 2, 3})

	require.Equal(t, map[uint64]int{1: 1}, counts.snapshot())
}

func TestServiceRestartUsesPersistedMembershipInsteadOfConfigDrift(t *testing.T) {
	env := newTestEnv(t, []uint64{1, 2, 3})
	defer env.stopAll()

	counts := env.captureBootstrapCalls(t)

	env.startNode(t, 1, nil)
	env.startNode(t, 2, nil)
	env.startNode(t, 3, nil)
	env.waitForLeader(t, []uint64{1, 2, 3})

	state := env.mustInitialState(t, 1)
	require.Equal(t, []uint64{1, 2, 3}, sortedPeers(state.ConfState.Voters))
	require.Equal(t, map[uint64]int{1: 1}, counts.snapshot())

	env.restartNode(t, 1, []Peer{})
	env.waitForLeader(t, []uint64{1, 2, 3})

	restarted := env.mustInitialState(t, 1)
	require.Equal(t, []uint64{1, 2, 3}, sortedPeers(restarted.ConfState.Voters))
	require.Equal(t, map[uint64]int{1: 1}, counts.snapshot())
}

func TestServiceRestartTrustsPersistedMembershipWhenConfigOmitsLocalNode(t *testing.T) {
	env := newTestEnv(t, []uint64{1, 2, 3})
	defer env.stopAll()

	counts := env.captureBootstrapCalls(t)

	env.startNode(t, 1, nil)
	env.startNode(t, 2, nil)
	env.startNode(t, 3, nil)
	env.waitForLeader(t, []uint64{1, 2, 3})

	state := env.mustInitialState(t, 1)
	require.Equal(t, []uint64{1, 2, 3}, sortedPeers(state.ConfState.Voters))
	require.Equal(t, map[uint64]int{1: 1}, counts.snapshot())

	env.restartNode(t, 1, []Peer{
		{NodeID: 2, Addr: env.addrOf(2)},
		{NodeID: 3, Addr: env.addrOf(3)},
	})
	env.waitForLeader(t, []uint64{1, 2, 3})

	restarted := env.mustInitialState(t, 1)
	require.Equal(t, []uint64{1, 2, 3}, sortedPeers(restarted.ConfState.Voters))
	require.Equal(t, map[uint64]int{1: 1}, counts.snapshot())
}

func TestServiceStartReturnsBootstrapFailureWithoutDeadlock(t *testing.T) {
	env := newTestEnv(t, []uint64{1})
	defer env.stopAll()

	sentinel := errors.New("bootstrap failed")
	original := rawNodeBootstrap
	rawNodeBootstrap = func(_ uint64, _ *raft.RawNode, _ []raft.Peer) error {
		return sentinel
	}
	t.Cleanup(func() {
		rawNodeBootstrap = original
	})

	errCh := make(chan error, 1)
	go func() {
		errCh <- env.startNodeErr(t, 1, nil)
	}()

	select {
	case err := <-errCh:
		require.ErrorIs(t, err, sentinel)
	case <-time.After(2 * time.Second):
		t.Fatal("Start hung on bootstrap failure")
	}
}

func TestServiceProposeReturnsRunLoopErrorAfterExit(t *testing.T) {
	sentinel := errors.New("run loop failed")
	doneCh := make(chan struct{})
	close(doneCh)

	service := &Service{
		started:   true,
		stopCh:    make(chan struct{}),
		doneCh:    doneCh,
		proposeCh: make(chan proposalRequest),
		err:       sentinel,
	}

	err := service.Propose(context.Background(), groupcontroller.Command{})
	require.ErrorIs(t, err, sentinel)
}

type testEnv struct {
	root     string
	addrs    map[uint64]string
	allPeers []Peer
	nodes    map[uint64]*testNode
}

type testNode struct {
	id       uint64
	dir      string
	addr     string
	server   *nodetransport.Server
	rpcMux   *nodetransport.RPCMux
	pool     *nodetransport.Pool
	logDB    *raftstorage.DB
	meta     *controllermeta.Store
	sm       *groupcontroller.StateMachine
	service  *Service
	allPeers []Peer
}

type bootstrapCounts struct {
	mu     sync.Mutex
	counts map[uint64]int
}

func newTestEnv(t *testing.T, nodeIDs []uint64) *testEnv {
	t.Helper()

	root := t.TempDir()
	addrs := reserveNodeAddrs(t, len(nodeIDs))
	peers := make([]Peer, 0, len(nodeIDs))
	nodes := make(map[uint64]*testNode, len(nodeIDs))
	addrMap := make(map[uint64]string, len(nodeIDs))

	for i, nodeID := range nodeIDs {
		addrMap[nodeID] = addrs[i]
		peers = append(peers, Peer{
			NodeID: nodeID,
			Addr:   addrs[i],
		})
	}

	for _, nodeID := range nodeIDs {
		nodes[nodeID] = &testNode{
			id:       nodeID,
			dir:      filepath.Join(root, fmt.Sprintf("n%d", nodeID)),
			addr:     addrMap[nodeID],
			allPeers: append([]Peer(nil), peers...),
		}
	}

	return &testEnv{
		root:     root,
		addrs:    addrMap,
		allPeers: peers,
		nodes:    nodes,
	}
}

func (e *testEnv) startNode(t *testing.T, nodeID uint64, peers []Peer) {
	t.Helper()
	require.NoError(t, e.startNodeErr(t, nodeID, peers))
}

func (e *testEnv) startNodeErr(t *testing.T, nodeID uint64, peers []Peer) error {
	t.Helper()
	node := e.nodes[nodeID]
	require.NotNil(t, node)
	require.Nil(t, node.service)

	if peers == nil {
		peers = node.allPeers
	}
	require.NoError(t, os.MkdirAll(node.dir, 0o755))

	node.server = nodetransport.NewServer()
	node.rpcMux = nodetransport.NewRPCMux()
	node.server.HandleRPCMux(node.rpcMux)
	require.NoError(t, node.server.Start(node.addr))

	discoveryPeers := make([]raftcluster.NodeConfig, 0, len(node.allPeers))
	for _, peer := range node.allPeers {
		discoveryPeers = append(discoveryPeers, raftcluster.NodeConfig{
			NodeID: multiraft.NodeID(peer.NodeID),
			Addr:   peer.Addr,
		})
	}
	node.pool = nodetransport.NewPool(raftcluster.NewStaticDiscovery(discoveryPeers), 2, 5*time.Second)

	var err error
	node.logDB, err = raftstorage.Open(filepath.Join(node.dir, "controller-raft"))
	require.NoError(t, err)
	node.meta, err = controllermeta.Open(filepath.Join(node.dir, "controller-meta"))
	require.NoError(t, err)
	node.sm = groupcontroller.NewStateMachine(node.meta, groupcontroller.StateMachineConfig{})

	node.service = &Service{cfg: Config{
		NodeID:         nodeID,
		Peers:          append([]Peer(nil), peers...),
		AllowBootstrap: true,
		LogDB:          node.logDB,
		StateMachine:   node.sm,
		Server:         node.server,
		RPCMux:         node.rpcMux,
		Pool:           node.pool,
	}}
	if err := node.service.Start(context.Background()); err != nil {
		e.stopNode(nodeID)
		return err
	}
	return nil
}

func (e *testEnv) restartNode(t *testing.T, nodeID uint64, peers []Peer) {
	t.Helper()
	e.stopNode(nodeID)
	e.startNode(t, nodeID, peers)
}

func (e *testEnv) stopAll() {
	for _, nodeID := range sortedPeersFromMap(e.nodes) {
		e.stopNode(nodeID)
	}
}

func (e *testEnv) stopNode(nodeID uint64) {
	node := e.nodes[nodeID]
	if node == nil {
		return
	}
	if node.service != nil {
		_ = node.service.Stop()
		node.service = nil
	}
	if node.pool != nil {
		node.pool.Close()
		node.pool = nil
	}
	if node.server != nil {
		node.server.Stop()
		node.server = nil
	}
	if node.logDB != nil {
		_ = node.logDB.Close()
		node.logDB = nil
	}
	if node.meta != nil {
		_ = node.meta.Close()
		node.meta = nil
	}
	node.rpcMux = nil
	node.sm = nil
}

func (e *testEnv) waitForLeader(t *testing.T, nodeIDs []uint64) uint64 {
	t.Helper()

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		var leader uint64
		allAgree := true
		for _, nodeID := range nodeIDs {
			node := e.nodes[nodeID]
			if node == nil || node.service == nil {
				allAgree = false
				break
			}
			got := node.service.LeaderID()
			if got == 0 {
				allAgree = false
				break
			}
			if leader == 0 {
				leader = got
				continue
			}
			if leader != got {
				allAgree = false
				break
			}
		}
		if allAgree && leader != 0 {
			return leader
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("leader was not observed for nodes %v", nodeIDs)
	return 0
}

func (e *testEnv) mustInitialState(t *testing.T, nodeID uint64) multiraft.BootstrapState {
	t.Helper()
	node := e.nodes[nodeID]
	require.NotNil(t, node)
	require.NotNil(t, node.logDB)

	state, err := node.logDB.ForGroup(controllerGroupStorageID).InitialState(context.Background())
	require.NoError(t, err)
	return state
}

func (e *testEnv) addrOf(nodeID uint64) string {
	return e.addrs[nodeID]
}

func (e *testEnv) captureBootstrapCalls(t *testing.T) *bootstrapCounts {
	t.Helper()

	counts := &bootstrapCounts{counts: make(map[uint64]int)}
	original := rawNodeBootstrap
	rawNodeBootstrap = func(nodeID uint64, rawNode *raft.RawNode, peers []raft.Peer) error {
		counts.record(nodeID)
		return original(nodeID, rawNode, peers)
	}
	t.Cleanup(func() {
		rawNodeBootstrap = original
	})
	return counts
}

func (c *bootstrapCounts) record(nodeID uint64) {
	c.mu.Lock()
	c.counts[nodeID]++
	c.mu.Unlock()
}

func (c *bootstrapCounts) snapshot() map[uint64]int {
	c.mu.Lock()
	defer c.mu.Unlock()

	out := make(map[uint64]int, len(c.counts))
	for nodeID, count := range c.counts {
		out[nodeID] = count
	}
	return out
}

func sortedPeers(peers []uint64) []uint64 {
	out := append([]uint64(nil), peers...)
	sort.Slice(out, func(i, j int) bool {
		return out[i] < out[j]
	})
	return out
}

func sortedPeersFromMap(nodes map[uint64]*testNode) []uint64 {
	out := make([]uint64, 0, len(nodes))
	for nodeID := range nodes {
		out = append(out, nodeID)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i] < out[j]
	})
	return out
}

func reserveNodeAddrs(t *testing.T, n int) []string {
	t.Helper()

	listeners := make([]net.Listener, 0, n)
	addrs := make([]string, 0, n)
	for i := 0; i < n; i++ {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		listeners = append(listeners, ln)
		addrs = append(addrs, ln.Addr().String())
	}
	for _, ln := range listeners {
		require.NoError(t, ln.Close())
	}
	return addrs
}
