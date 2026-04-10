package cluster

import (
	"fmt"
	"sort"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/group/multiraft"
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
	NodeID             multiraft.NodeID
	ListenAddr         string
	GroupCount         uint32
	ControllerMetaPath string
	ControllerRaftPath string
	ControllerReplicaN int
	GroupReplicaN      int
	NewStorage         func(groupID multiraft.GroupID) (multiraft.Storage, error)
	NewStateMachine    func(groupID multiraft.GroupID) (multiraft.StateMachine, error)
	Nodes              []NodeConfig
	Groups             []GroupConfig
	ForwardTimeout     time.Duration
	PoolSize           int
	TickInterval       time.Duration
	RaftWorkers        int
	ElectionTick       int
	HeartbeatTick      int
	DialTimeout        time.Duration
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
	if c.ControllerReplicaN <= 0 {
		return fmt.Errorf("%w: ControllerReplicaN must be > 0", ErrInvalidConfig)
	}
	if c.GroupReplicaN <= 0 {
		return fmt.Errorf("%w: GroupReplicaN must be > 0", ErrInvalidConfig)
	}
	if c.ControllerReplicaN > len(c.Nodes) {
		return fmt.Errorf("%w: ControllerReplicaN=%d exceeds Nodes=%d", ErrInvalidConfig, c.ControllerReplicaN, len(c.Nodes))
	}
	if c.GroupReplicaN > len(c.Nodes) {
		return fmt.Errorf("%w: GroupReplicaN=%d exceeds Nodes=%d", ErrInvalidConfig, c.GroupReplicaN, len(c.Nodes))
	}
	if (c.ControllerMetaPath == "") != (c.ControllerRaftPath == "") {
		return fmt.Errorf("%w: ControllerMetaPath and ControllerRaftPath must be set together", ErrInvalidConfig)
	}

	nodeSet := make(map[multiraft.NodeID]bool, len(c.Nodes))
	selfFound := false
	for _, n := range c.Nodes {
		if nodeSet[n.NodeID] {
			return fmt.Errorf("%w: duplicate NodeID %d in Nodes", ErrInvalidConfig, n.NodeID)
		}
		nodeSet[n.NodeID] = true
		if n.NodeID == c.NodeID {
			selfFound = true
		}
	}
	if !selfFound {
		return fmt.Errorf("%w: NodeID %d not found in Nodes", ErrInvalidConfig, c.NodeID)
	}

	groupSet := make(map[multiraft.GroupID]bool, len(c.Groups))
	groupSelfFound := false
	for _, g := range c.Groups {
		if groupSet[g.GroupID] {
			return fmt.Errorf("%w: duplicate GroupID %d", ErrInvalidConfig, g.GroupID)
		}
		if g.GroupID == 0 || uint32(g.GroupID) > c.GroupCount {
			return fmt.Errorf("%w: GroupID %d exceeds GroupCount=%d", ErrInvalidConfig, g.GroupID, c.GroupCount)
		}
		groupSet[g.GroupID] = true
		for _, peer := range g.Peers {
			if !nodeSet[peer] {
				return fmt.Errorf("%w: peer %d in group %d not found in Nodes", ErrInvalidConfig, peer, g.GroupID)
			}
			if peer == c.NodeID {
				groupSelfFound = true
			}
		}
	}
	if len(c.Groups) > 0 && !groupSelfFound {
		return fmt.Errorf("%w: NodeID %d not found as peer in any group", ErrInvalidConfig, c.NodeID)
	}
	return nil
}

func (c *Config) applyDefaults() {
	if c.GroupCount == 0 && len(c.Groups) > 0 {
		c.GroupCount = uint32(len(c.Groups))
	}
	if c.ControllerReplicaN == 0 {
		c.ControllerReplicaN = len(c.Nodes)
	}
	if c.GroupReplicaN == 0 {
		c.GroupReplicaN = len(c.Nodes)
	}
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

func (c Config) DerivedControllerNodes() []NodeConfig {
	nodes := append([]NodeConfig(nil), c.Nodes...)
	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].NodeID < nodes[j].NodeID
	})
	if c.ControllerReplicaN > 0 && c.ControllerReplicaN < len(nodes) {
		nodes = nodes[:c.ControllerReplicaN]
	}
	return nodes
}

func (c Config) HasLocalControllerPeer() bool {
	for _, node := range c.DerivedControllerNodes() {
		if node.NodeID == c.NodeID {
			return true
		}
	}
	return false
}

func (c Config) ControllerEnabled() bool {
	return c.ControllerMetaPath != "" && c.ControllerRaftPath != ""
}
