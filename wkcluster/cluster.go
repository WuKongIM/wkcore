package wkcluster

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/WuKongIM/wraft/multiraft"
	"github.com/WuKongIM/wraft/raftstore"
	"github.com/WuKongIM/wraft/wkdb"
	"github.com/WuKongIM/wraft/wkfsm"
	raft "go.etcd.io/raft/v3"
)

type Cluster struct {
	cfg       Config
	runtime   *multiraft.Runtime
	transport *Transport
	discovery Discovery
	router    *Router
	forwarder *Forwarder
	db        *wkdb.DB
	raftDB    *raftstore.DB
	// stopped is checked at the start of write operations to reject new work
	// during shutdown. There is an inherent TOCTOU gap between the check and
	// the actual proposal, but this is acceptable: the raft layer will reject
	// proposals on a stopped runtime, so at worst we do slightly more work
	// before returning an error.
	stopped atomic.Bool
}

func NewCluster(cfg Config) (*Cluster, error) {
	cfg.applyDefaults()
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return &Cluster{cfg: cfg}, nil
}

func (c *Cluster) Start() error {
	// 1. Open databases
	var err error
	c.db, err = wkdb.Open(c.cfg.dataDir())
	if err != nil {
		return fmt.Errorf("open wkdb: %w", err)
	}
	c.raftDB, err = raftstore.Open(c.cfg.RaftDataDir)
	if err != nil {
		_ = c.db.Close()
		return fmt.Errorf("open raftstore: %w", err)
	}

	// 2. Discovery
	c.discovery = NewStaticDiscovery(c.cfg.Nodes)

	// 3. Transport
	c.transport = NewTransport(c.cfg.NodeID, c.discovery, c.cfg.PoolSize, c.cfg.DialTimeout, c.cfg.ForwardTimeout)
	if err := c.transport.Start(c.cfg.ListenAddr); err != nil {
		_ = c.raftDB.Close()
		_ = c.db.Close()
		return fmt.Errorf("start transport: %w", err)
	}

	// 4. Runtime
	c.runtime, err = multiraft.New(multiraft.Options{
		NodeID:       c.cfg.NodeID,
		TickInterval: c.cfg.TickInterval,
		Workers:      c.cfg.RaftWorkers,
		Transport:    c.transport,
		Raft: multiraft.RaftOptions{
			ElectionTick:  c.cfg.ElectionTick,
			HeartbeatTick: c.cfg.HeartbeatTick,
		},
	})
	if err != nil {
		c.transport.Stop()
		_ = c.raftDB.Close()
		_ = c.db.Close()
		return fmt.Errorf("create runtime: %w", err)
	}
	c.transport.SetRuntime(c.runtime)

	// 5. Router and Forwarder
	c.router = NewRouter(c.cfg.GroupCount, c.cfg.NodeID, c.runtime)
	c.forwarder = NewForwarder(c.cfg.NodeID, c.transport, c.cfg.ForwardTimeout)
	c.transport.SetHandler(c)

	// 6. Open groups
	ctx := context.Background()
	for _, g := range c.cfg.Groups {
		if err := c.openOrBootstrapGroup(ctx, g); err != nil {
			c.Stop()
			return fmt.Errorf("open group %d: %w", g.GroupID, err)
		}
	}

	return nil
}

func (c *Cluster) openOrBootstrapGroup(ctx context.Context, g GroupConfig) error {
	storage := c.raftDB.ForGroup(uint64(g.GroupID))
	sm, err := wkfsm.NewStateMachine(c.db, uint64(g.GroupID))
	if err != nil {
		return err
	}
	opts := multiraft.GroupOptions{
		ID:           g.GroupID,
		Storage:      storage,
		StateMachine: sm,
	}

	initialState, err := storage.InitialState(ctx)
	if err != nil {
		return err
	}
	if !raft.IsEmptyHardState(initialState.HardState) {
		return c.runtime.OpenGroup(ctx, opts)
	}
	return c.runtime.BootstrapGroup(ctx, multiraft.BootstrapGroupRequest{
		Group:  opts,
		Voters: g.Peers,
	})
}

func (c *Cluster) Stop() {
	c.stopped.Store(true)

	if c.forwarder != nil {
		c.forwarder.Stop()
	}
	if c.runtime != nil {
		_ = c.runtime.Close()
	}
	if c.transport != nil {
		c.transport.Stop()
	}
	if c.raftDB != nil {
		_ = c.raftDB.Close()
	}
	if c.db != nil {
		_ = c.db.Close()
	}
}

// handleForward implements forwardHandler for the Transport.
// Called when this node (as leader) receives a forwarded request.
func (c *Cluster) handleForward(ctx context.Context, groupID multiraft.GroupID, cmd []byte) ([]byte, uint8) {
	if c.stopped.Load() {
		return nil, errCodeTimeout
	}
	_, err := c.runtime.Status(groupID)
	if err != nil {
		return nil, errCodeNoGroup
	}
	future, err := c.runtime.Propose(ctx, groupID, cmd)
	if err != nil {
		return nil, errCodeNotLeader
	}
	result, err := future.Wait(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return nil, errCodeTimeout
		}
		return nil, errCodeNotLeader
	}
	return result.Data, errCodeOK
}
