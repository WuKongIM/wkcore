package cluster

import (
	"fmt"
	"sort"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/slot/multiraft"
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
	SlotCount          uint32
	ControllerMetaPath string
	ControllerRaftPath string
	ControllerReplicaN int
	SlotReplicaN       int
	NewStorage         func(slotID multiraft.SlotID) (multiraft.Storage, error)
	NewStateMachine    func(slotID multiraft.SlotID) (multiraft.StateMachine, error)
	Nodes              []NodeConfig
	Slots              []SlotConfig
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

type SlotConfig struct {
	SlotID multiraft.SlotID
	Peers  []multiraft.NodeID
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
	if c.SlotCount == 0 {
		return fmt.Errorf("%w: SlotCount must be > 0", ErrInvalidConfig)
	}
	if c.ControllerReplicaN <= 0 {
		return fmt.Errorf("%w: ControllerReplicaN must be > 0", ErrInvalidConfig)
	}
	if c.SlotReplicaN <= 0 {
		return fmt.Errorf("%w: SlotReplicaN must be > 0", ErrInvalidConfig)
	}
	if c.ControllerReplicaN > len(c.Nodes) {
		return fmt.Errorf("%w: ControllerReplicaN=%d exceeds Nodes=%d", ErrInvalidConfig, c.ControllerReplicaN, len(c.Nodes))
	}
	if c.SlotReplicaN > len(c.Nodes) {
		return fmt.Errorf("%w: SlotReplicaN=%d exceeds Nodes=%d", ErrInvalidConfig, c.SlotReplicaN, len(c.Nodes))
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

	slotSet := make(map[multiraft.SlotID]bool, len(c.Slots))
	slotSelfFound := false
	for _, g := range c.Slots {
		if slotSet[g.SlotID] {
			return fmt.Errorf("%w: duplicate SlotID %d", ErrInvalidConfig, g.SlotID)
		}
		if g.SlotID == 0 || uint32(g.SlotID) > c.SlotCount {
			return fmt.Errorf("%w: SlotID %d exceeds SlotCount=%d", ErrInvalidConfig, g.SlotID, c.SlotCount)
		}
		slotSet[g.SlotID] = true
		for _, peer := range g.Peers {
			if !nodeSet[peer] {
				return fmt.Errorf("%w: peer %d in slot %d not found in Nodes", ErrInvalidConfig, peer, g.SlotID)
			}
			if peer == c.NodeID {
				slotSelfFound = true
			}
		}
	}
	if len(c.Slots) > 0 && !slotSelfFound {
		return fmt.Errorf("%w: NodeID %d not found as peer in any slot", ErrInvalidConfig, c.NodeID)
	}
	return nil
}

func (c *Config) applyDefaults() {
	if c.SlotCount == 0 && len(c.Slots) > 0 {
		c.SlotCount = uint32(len(c.Slots))
	}
	if c.ControllerReplicaN == 0 {
		c.ControllerReplicaN = len(c.Nodes)
	}
	if c.SlotReplicaN == 0 {
		c.SlotReplicaN = len(c.Nodes)
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
