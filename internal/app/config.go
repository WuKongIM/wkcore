package app

import (
	"fmt"
	"path/filepath"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/gateway"
	"github.com/WuKongIM/WuKongIM/internal/runtime/messageid"
)

type Config struct {
	Node    NodeConfig
	Storage StorageConfig
	Cluster ClusterConfig
	API     APIConfig
	Gateway GatewayConfig
}

type NodeConfig struct {
	ID      uint64
	Name    string
	DataDir string
}

type StorageConfig struct {
	DBPath         string
	RaftPath       string
	ChannelLogPath string
}

type ClusterConfig struct {
	ListenAddr          string
	GroupCount          uint32
	Nodes               []NodeConfigRef
	Groups              []GroupConfig
	ForwardTimeout      time.Duration
	PoolSize            int
	TickInterval        time.Duration
	RaftWorkers         int
	ElectionTick        int
	HeartbeatTick       int
	DialTimeout         time.Duration
	DataPlaneRPCTimeout time.Duration
}

type NodeConfigRef struct {
	ID   uint64
	Addr string
}

type GroupConfig struct {
	ID    uint32
	Peers []uint64
}

type GatewayConfig struct {
	TokenAuthOn    bool
	DefaultSession gateway.SessionOptions
	Listeners      []gateway.ListenerOptions
}

type APIConfig struct {
	ListenAddr string
}

func (c *Config) ApplyDefaultsAndValidate() error {
	if c == nil {
		return fmt.Errorf("%w: nil config", ErrInvalidConfig)
	}

	if c.Node.ID == 0 {
		return fmt.Errorf("%w: node id must be set", ErrInvalidConfig)
	}
	if c.Node.ID > messageid.MaxNodeID {
		return fmt.Errorf("%w: node id %d exceeds snowflake max %d", ErrInvalidConfig, c.Node.ID, messageid.MaxNodeID)
	}
	if c.Node.DataDir == "" {
		return fmt.Errorf("%w: node data dir must be set", ErrInvalidConfig)
	}
	if c.Cluster.ListenAddr == "" {
		return fmt.Errorf("%w: cluster listen addr must be set", ErrInvalidConfig)
	}
	if len(c.Cluster.Nodes) == 0 {
		return fmt.Errorf("%w: cluster nodes must be set", ErrInvalidConfig)
	}
	if len(c.Cluster.Groups) == 0 {
		return fmt.Errorf("%w: cluster groups must be set", ErrInvalidConfig)
	}
	if len(c.Gateway.Listeners) == 0 {
		return fmt.Errorf("%w: gateway listeners must be set", ErrInvalidConfig)
	}
	if c.Gateway.TokenAuthOn {
		return fmt.Errorf("%w: gateway token auth requires verifier hooks", ErrInvalidConfig)
	}

	if c.Storage.DBPath == "" {
		c.Storage.DBPath = filepath.Join(c.Node.DataDir, "data")
	}
	if c.Storage.RaftPath == "" {
		c.Storage.RaftPath = filepath.Join(c.Node.DataDir, "raft")
	}
	if c.Storage.ChannelLogPath == "" {
		c.Storage.ChannelLogPath = filepath.Join(c.Node.DataDir, "channellog")
	}

	dbPath, err := normalizeStoragePath(c.Storage.DBPath)
	if err != nil {
		return fmt.Errorf("%w: normalize db path: %v", ErrInvalidConfig, err)
	}
	raftPath, err := normalizeStoragePath(c.Storage.RaftPath)
	if err != nil {
		return fmt.Errorf("%w: normalize raft path: %v", ErrInvalidConfig, err)
	}
	channelLogPath, err := normalizeStoragePath(c.Storage.ChannelLogPath)
	if err != nil {
		return fmt.Errorf("%w: normalize channel log path: %v", ErrInvalidConfig, err)
	}
	if dbPath == raftPath {
		return fmt.Errorf("%w: storage db path and raft path must differ", ErrInvalidConfig)
	}
	if dbPath == channelLogPath || raftPath == channelLogPath {
		return fmt.Errorf("%w: channel log path must differ from db and raft paths", ErrInvalidConfig)
	}

	if c.Cluster.GroupCount == 0 {
		c.Cluster.GroupCount = uint32(len(c.Cluster.Groups))
	}
	if c.Cluster.GroupCount == 0 {
		return fmt.Errorf("%w: cluster group count must be set", ErrInvalidConfig)
	}
	if uint32(len(c.Cluster.Groups)) != c.Cluster.GroupCount {
		return fmt.Errorf("%w: cluster groups do not match group count", ErrInvalidConfig)
	}

	c.Gateway.DefaultSession = gateway.NormalizeSessionOptions(c.Gateway.DefaultSession)

	nodeSet := make(map[uint64]struct{}, len(c.Cluster.Nodes))
	selfNodeFound := false
	for _, node := range c.Cluster.Nodes {
		if node.ID == 0 {
			return fmt.Errorf("%w: cluster node id must be set", ErrInvalidConfig)
		}
		if node.Addr == "" {
			return fmt.Errorf("%w: cluster node addr must be set", ErrInvalidConfig)
		}
		if _, ok := nodeSet[node.ID]; ok {
			return fmt.Errorf("%w: duplicate cluster node id %d", ErrInvalidConfig, node.ID)
		}
		nodeSet[node.ID] = struct{}{}
		if node.ID == c.Node.ID {
			selfNodeFound = true
		}
	}

	groupSet := make(map[uint32]struct{}, len(c.Cluster.Groups))
	selfPeerFound := false
	for _, group := range c.Cluster.Groups {
		if group.ID == 0 {
			return fmt.Errorf("%w: cluster group id must be set", ErrInvalidConfig)
		}
		if group.ID > c.Cluster.GroupCount {
			return fmt.Errorf("%w: cluster group id %d exceeds group count %d", ErrInvalidConfig, group.ID, c.Cluster.GroupCount)
		}
		if _, ok := groupSet[group.ID]; ok {
			return fmt.Errorf("%w: duplicate cluster group id %d", ErrInvalidConfig, group.ID)
		}
		groupSet[group.ID] = struct{}{}
		if len(group.Peers) == 0 {
			return fmt.Errorf("%w: cluster group peers must be set", ErrInvalidConfig)
		}
		for _, peerID := range group.Peers {
			if _, ok := nodeSet[peerID]; !ok {
				return fmt.Errorf("%w: peer %d not found in cluster nodes", ErrInvalidConfig, peerID)
			}
			if peerID == c.Node.ID {
				selfPeerFound = true
			}
		}
	}
	for expectedID := uint32(1); expectedID <= c.Cluster.GroupCount; expectedID++ {
		if _, ok := groupSet[expectedID]; !ok {
			return fmt.Errorf("%w: cluster group id %d missing from configured groups", ErrInvalidConfig, expectedID)
		}
	}
	if !selfNodeFound {
		return fmt.Errorf("%w: node id %d not found in cluster nodes", ErrInvalidConfig, c.Node.ID)
	}
	if !selfPeerFound {
		return fmt.Errorf("%w: node id %d not present as a peer in any group", ErrInvalidConfig, c.Node.ID)
	}

	return nil
}

func normalizeStoragePath(path string) (string, error) {
	return filepath.Abs(filepath.Clean(path))
}
