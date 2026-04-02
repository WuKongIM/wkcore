package wkcluster

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync/atomic"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/multiraft"
	"github.com/WuKongIM/WuKongIM/pkg/wktransport"
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
	// 1. Discovery
	c.discovery = NewStaticDiscovery(c.cfg.Nodes)

	// 2. Server
	c.server = wktransport.NewServer()
	c.server.Handle(msgTypeRaft, c.handleRaftMessage)
	c.server.HandleRPC(c.handleForwardRPC)
	if err := c.server.Start(c.cfg.ListenAddr); err != nil {
		return fmt.Errorf("start server: %w", err)
	}

	// 3. Pools + Clients
	c.raftPool = wktransport.NewPool(c.discovery, c.cfg.PoolSize, c.cfg.DialTimeout)
	c.raftClient = wktransport.NewClient(c.raftPool)
	c.fwdClient = wktransport.NewClient(c.raftPool)

	// 4. Runtime
	var err error
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
		return fmt.Errorf("create runtime: %w", err)
	}

	// 5. Router
	c.router = NewRouter(c.cfg.GroupCount, c.cfg.NodeID, c.runtime)

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
	storage, err := c.cfg.NewStorage(g.GroupID)
	if err != nil {
		return fmt.Errorf("create storage for group %d: %w", g.GroupID, err)
	}
	sm, err := c.cfg.NewStateMachine(g.GroupID)
	if err != nil {
		return fmt.Errorf("create state machine for group %d: %w", g.GroupID, err)
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

// Propose submits a command to the specified group, automatically handling leader forwarding.
func (c *Cluster) Propose(ctx context.Context, groupID multiraft.GroupID, cmd []byte) error {
	if c.stopped.Load() {
		return wktransport.ErrStopped
	}
	const maxRetries = 3
	for attempt := 0; attempt < maxRetries; attempt++ {
		if attempt > 0 {
			// Exponential backoff: 50ms, 100ms
			backoff := time.Duration(attempt) * 50 * time.Millisecond
			select {
			case <-time.After(backoff):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		leaderID, err := c.router.LeaderOf(groupID)
		if err != nil {
			return err
		}
		if c.router.IsLocal(leaderID) {
			future, err := c.runtime.Propose(ctx, groupID, cmd)
			if err != nil {
				return err
			}
			_, err = future.Wait(ctx)
			return err
		}
		err = c.forwardToLeader(ctx, leaderID, groupID, cmd)
		if errors.Is(err, ErrNotLeader) {
			continue
		}
		return err
	}
	return ErrLeaderNotStable
}

// SlotForKey maps a key to a raft group via CRC32 hashing.
func (c *Cluster) SlotForKey(key string) multiraft.GroupID {
	return c.router.SlotForKey(key)
}

// LeaderOf returns the current leader of the specified group.
func (c *Cluster) LeaderOf(groupID multiraft.GroupID) (multiraft.NodeID, error) {
	return c.router.LeaderOf(groupID)
}

// IsLocal reports whether the given node is the local node.
func (c *Cluster) IsLocal(nodeID multiraft.NodeID) bool {
	return c.router.IsLocal(nodeID)
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
