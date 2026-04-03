package wkcluster

import (
	"fmt"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/replication/multiraft"
)

const (
	defaultForwardTimeout = 5 * time.Second
	defaultPoolSize       = 4
	defaultTickInterval   = 100 * time.Millisecond
	defaultRaftWorkers    = 2
	defaultElectionTick   = 10
	defaultHeartbeatTick  = 1
	defaultDialTimeout    = 5 * time.Second
)

type Config struct {
	NodeID          multiraft.NodeID
	ListenAddr      string
	GroupCount      uint32
	NewStorage      func(groupID multiraft.GroupID) (multiraft.Storage, error)
	NewStateMachine func(groupID multiraft.GroupID) (multiraft.StateMachine, error)
	Nodes           []NodeConfig
	Groups          []GroupConfig
	ForwardTimeout  time.Duration
	PoolSize        int
	TickInterval    time.Duration
	RaftWorkers     int
	ElectionTick    int
	HeartbeatTick   int
	DialTimeout     time.Duration
}

type NodeConfig struct {
	NodeID multiraft.NodeID
	Addr   string
}

type GroupConfig struct {
	GroupID multiraft.GroupID
	Peers   []multiraft.NodeID
}

func (c *Config) validate() error {
	if c.NodeID == 0 {
		return fmt.Errorf("%w: NodeID must be > 0", ErrInvalidConfig)
	}
	if c.ListenAddr == "" {
		return fmt.Errorf("%w: ListenAddr must be set", ErrInvalidConfig)
	}
	if c.NewStorage == nil {
		return fmt.Errorf("%w: NewStorage must be set", ErrInvalidConfig)
	}
	if c.NewStateMachine == nil {
		return fmt.Errorf("%w: NewStateMachine must be set", ErrInvalidConfig)
	}
	if c.GroupCount == 0 {
		return fmt.Errorf("%w: GroupCount must be > 0", ErrInvalidConfig)
	}
	if uint32(len(c.Groups)) != c.GroupCount {
		return fmt.Errorf("%w: len(Groups)=%d != GroupCount=%d", ErrInvalidConfig, len(c.Groups), c.GroupCount)
	}

	nodeSet := make(map[multiraft.NodeID]bool, len(c.Nodes))
	for _, n := range c.Nodes {
		if nodeSet[n.NodeID] {
			return fmt.Errorf("%w: duplicate NodeID %d in Nodes", ErrInvalidConfig, n.NodeID)
		}
		nodeSet[n.NodeID] = true
	}

	groupSet := make(map[multiraft.GroupID]bool, len(c.Groups))
	selfFound := false
	for _, g := range c.Groups {
		if groupSet[g.GroupID] {
			return fmt.Errorf("%w: duplicate GroupID %d", ErrInvalidConfig, g.GroupID)
		}
		groupSet[g.GroupID] = true
		for _, peer := range g.Peers {
			if !nodeSet[peer] {
				return fmt.Errorf("%w: peer %d in group %d not found in Nodes", ErrInvalidConfig, peer, g.GroupID)
			}
			if peer == c.NodeID {
				selfFound = true
			}
		}
	}
	if !selfFound {
		return fmt.Errorf("%w: NodeID %d not found as peer in any group", ErrInvalidConfig, c.NodeID)
	}
	return nil
}

func (c *Config) applyDefaults() {
	if c.ForwardTimeout == 0 {
		c.ForwardTimeout = defaultForwardTimeout
	}
	if c.PoolSize == 0 {
		c.PoolSize = defaultPoolSize
	}
	if c.TickInterval == 0 {
		c.TickInterval = defaultTickInterval
	}
	if c.RaftWorkers == 0 {
		c.RaftWorkers = defaultRaftWorkers
	}
	if c.ElectionTick == 0 {
		c.ElectionTick = defaultElectionTick
	}
	if c.HeartbeatTick == 0 {
		c.HeartbeatTick = defaultHeartbeatTick
	}
	if c.DialTimeout == 0 {
		c.DialTimeout = defaultDialTimeout
	}
}
