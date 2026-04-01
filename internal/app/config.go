package app

import (
	"fmt"
	"path/filepath"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/gateway"
)

type Config struct {
	Node    NodeConfig
	Storage StorageConfig
	Cluster ClusterConfig
	Gateway GatewayConfig
}

type NodeConfig struct {
	ID      uint64
	Name    string
	DataDir string
}

type StorageConfig struct {
	DBPath   string
	RaftPath string
}

type ClusterConfig struct {
	ListenAddr     string
	GroupCount     uint32
	Nodes          []NodeConfigRef
	Groups         []GroupConfig
	ForwardTimeout time.Duration
	PoolSize       int
	TickInterval   time.Duration
	RaftWorkers    int
	ElectionTick   int
	HeartbeatTick  int
	DialTimeout    time.Duration
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

func (c *Config) ApplyDefaultsAndValidate() error {
	if c == nil {
		return fmt.Errorf("%w: nil config", ErrInvalidConfig)
	}

	if c.Node.ID == 0 {
		return fmt.Errorf("%w: node id must be set", ErrInvalidConfig)
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

	if c.Storage.DBPath == "" {
		c.Storage.DBPath = filepath.Join(c.Node.DataDir, "data")
	}
	if c.Storage.RaftPath == "" {
		c.Storage.RaftPath = filepath.Join(c.Node.DataDir, "raft")
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

	c.Gateway.DefaultSession = normalizeSessionOptions(c.Gateway.DefaultSession)

	nodeSet := make(map[uint64]struct{}, len(c.Cluster.Nodes))
	for _, node := range c.Cluster.Nodes {
		if node.ID == 0 {
			return fmt.Errorf("%w: cluster node id must be set", ErrInvalidConfig)
		}
		if node.Addr == "" {
			return fmt.Errorf("%w: cluster node addr must be set", ErrInvalidConfig)
		}
		nodeSet[node.ID] = struct{}{}
	}

	for _, group := range c.Cluster.Groups {
		if group.ID == 0 {
			return fmt.Errorf("%w: cluster group id must be set", ErrInvalidConfig)
		}
		if len(group.Peers) == 0 {
			return fmt.Errorf("%w: cluster group peers must be set", ErrInvalidConfig)
		}
		for _, peerID := range group.Peers {
			if _, ok := nodeSet[peerID]; !ok {
				return fmt.Errorf("%w: peer %d not found in cluster nodes", ErrInvalidConfig, peerID)
			}
		}
	}

	return nil
}

func normalizeSessionOptions(opt gateway.SessionOptions) gateway.SessionOptions {
	def := gateway.DefaultSessionOptions()
	if opt == (gateway.SessionOptions{}) {
		return def
	}
	if opt.ReadBufferSize == 0 {
		opt.ReadBufferSize = def.ReadBufferSize
	}
	if opt.WriteQueueSize == 0 {
		opt.WriteQueueSize = def.WriteQueueSize
	}
	if opt.MaxInboundBytes == 0 {
		opt.MaxInboundBytes = def.MaxInboundBytes
	}
	if opt.MaxOutboundBytes == 0 {
		opt.MaxOutboundBytes = def.MaxOutboundBytes
	}
	if opt.IdleTimeout == 0 {
		opt.IdleTimeout = def.IdleTimeout
	}
	if opt.WriteTimeout == 0 {
		opt.WriteTimeout = def.WriteTimeout
	}
	if !opt.CloseOnHandlerError {
		opt.CloseOnHandlerError = def.CloseOnHandlerError
	}
	return opt
}
