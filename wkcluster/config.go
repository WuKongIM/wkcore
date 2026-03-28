package wkcluster

import (
	"fmt"
	"path/filepath"
	"time"

	"github.com/WuKongIM/wraft/multiraft"
)

const (
	defaultForwardTimeout = 5 * time.Second
	defaultPoolSize       = 4
)

type Config struct {
	NodeID         multiraft.NodeID
	ListenAddr     string
	GroupCount     uint32
	DataDir        string
	RaftDataDir    string
	Nodes          []NodeConfig
	Groups         []GroupConfig
	ForwardTimeout time.Duration
	PoolSize       int
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
	if c.GroupCount == 0 {
		return fmt.Errorf("%w: GroupCount must be > 0", ErrInvalidConfig)
	}
	if uint32(len(c.Groups)) != c.GroupCount {
		return fmt.Errorf("%w: len(Groups)=%d != GroupCount=%d", ErrInvalidConfig, len(c.Groups), c.GroupCount)
	}

	nodeSet := make(map[multiraft.NodeID]bool, len(c.Nodes))
	for _, n := range c.Nodes {
		nodeSet[n.NodeID] = true
	}

	selfFound := false
	for _, g := range c.Groups {
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
	if c.RaftDataDir == "" {
		c.RaftDataDir = filepath.Join(c.DataDir, "raft")
	}
}

func (c *Config) dataDir() string {
	return filepath.Join(c.DataDir, "data")
}
