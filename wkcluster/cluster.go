package wkcluster

import (
	"context"
	"fmt"
	"net"
	"sync/atomic"

	"github.com/WuKongIM/wraft/multiraft"
	"github.com/WuKongIM/wraft/raftstore"
	"github.com/WuKongIM/wraft/wkdb"
	"github.com/WuKongIM/wraft/wkfsm"
	"github.com/WuKongIM/wraft/wktransport"
	raft "go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

type Cluster struct {
	cfg        Config
	server     *wktransport.Server
	raftPool   *wktransport.Pool
	raftClient *wktransport.Client
	fwdClient  *wktransport.Client
	runtime    *multiraft.Runtime
	router     *Router
	discovery  *StaticDiscovery
	db         *wkdb.DB
	raftDB     *raftstore.DB
	stopped    atomic.Bool
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

	// 3. Server
	c.server = wktransport.NewServer()
	c.server.Handle(msgTypeRaft, c.handleRaftMessage)
	c.server.HandleRPC(c.handleForwardRPC)
	if err := c.server.Start(c.cfg.ListenAddr); err != nil {
		_ = c.raftDB.Close()
		_ = c.db.Close()
		return fmt.Errorf("start server: %w", err)
	}

	// 4. Pools + Clients
	c.raftPool = wktransport.NewPool(c.discovery, c.cfg.PoolSize, c.cfg.DialTimeout)
	c.raftClient = wktransport.NewClient(c.raftPool)
	c.fwdClient = wktransport.NewClient(c.raftPool)

	// 5. Runtime
	c.runtime, err = multiraft.New(multiraft.Options{
		NodeID:       c.cfg.NodeID,
		TickInterval: c.cfg.TickInterval,
		Workers:      c.cfg.RaftWorkers,
		Transport:    &raftTransport{client: c.raftClient},
		Raft: multiraft.RaftOptions{
			ElectionTick:  c.cfg.ElectionTick,
			HeartbeatTick: c.cfg.HeartbeatTick,
		},
	})
	if err != nil {
		c.fwdClient.Stop()
		c.raftClient.Stop()
		c.raftPool.Close()
		c.server.Stop()
		_ = c.raftDB.Close()
		_ = c.db.Close()
		return fmt.Errorf("create runtime: %w", err)
	}

	// 6. Router
	c.router = NewRouter(c.cfg.GroupCount, c.cfg.NodeID, c.runtime)

	// 7. Open groups
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

	if c.fwdClient != nil {
		c.fwdClient.Stop()
	}
	if c.raftClient != nil {
		c.raftClient.Stop()
	}
	if c.runtime != nil {
		_ = c.runtime.Close()
	}
	if c.server != nil {
		c.server.Stop()
	}
	if c.raftPool != nil {
		c.raftPool.Close()
	}
	if c.raftDB != nil {
		_ = c.raftDB.Close()
	}
	if c.db != nil {
		_ = c.db.Close()
	}
}

// handleRaftMessage is the server handler for msgTypeRaft.
func (c *Cluster) handleRaftMessage(_ net.Conn, body []byte) {
	if c.runtime == nil {
		return
	}
	groupID, data, err := decodeRaftBody(body)
	if err != nil {
		return
	}
	var msg raftpb.Message
	if err := msg.Unmarshal(data); err != nil {
		return
	}
	_ = c.runtime.Step(context.Background(), multiraft.Envelope{
		GroupID: multiraft.GroupID(groupID),
		Message: msg,
	})
}

// Server returns the underlying wktransport.Server, allowing business layer
// to register additional handlers on the shared listener.
func (c *Cluster) Server() *wktransport.Server {
	return c.server
}

// Discovery returns the cluster's Discovery instance for creating business pools.
func (c *Cluster) Discovery() Discovery {
	return c.discovery
}
