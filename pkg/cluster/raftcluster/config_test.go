package raftcluster

import (
	"errors"
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/replication/multiraft"
)

func TestConfigValidate_Valid(t *testing.T) {
	cfg := validTestConfig()
	if err := cfg.validate(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestConfigValidate_GroupCountZero(t *testing.T) {
	cfg := validTestConfig()
	cfg.GroupCount = 0
	if err := cfg.validate(); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("expected ErrInvalidConfig, got: %v", err)
	}
}

func TestConfigValidate_GroupCountMismatch(t *testing.T) {
	cfg := validTestConfig()
	cfg.GroupCount = 5
	if err := cfg.validate(); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("expected ErrInvalidConfig, got: %v", err)
	}
}

func TestConfigValidate_PeerNotInNodes(t *testing.T) {
	cfg := validTestConfig()
	cfg.Groups[0].Peers = append(cfg.Groups[0].Peers, 99)
	if err := cfg.validate(); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("expected ErrInvalidConfig, got: %v", err)
	}
}

func TestConfigValidate_SelfNotPeer(t *testing.T) {
	cfg := validTestConfig()
	cfg.NodeID = 99
	cfg.Nodes = append(cfg.Nodes, NodeConfig{NodeID: 99, Addr: ":9999"})
	if err := cfg.validate(); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("expected ErrInvalidConfig, got: %v", err)
	}
}

func TestConfigApplyDefaults(t *testing.T) {
	cfg := validTestConfig()
	cfg.applyDefaults()
	if cfg.ForwardTimeout != defaultForwardTimeout {
		t.Fatalf("expected default ForwardTimeout")
	}
	if cfg.PoolSize != defaultPoolSize {
		t.Fatalf("expected default PoolSize")
	}
	if cfg.TickInterval != defaultTickInterval {
		t.Fatalf("expected default TickInterval")
	}
	if cfg.RaftWorkers != defaultRaftWorkers {
		t.Fatalf("expected default RaftWorkers")
	}
	if cfg.ElectionTick != defaultElectionTick {
		t.Fatalf("expected default ElectionTick")
	}
	if cfg.HeartbeatTick != defaultHeartbeatTick {
		t.Fatalf("expected default HeartbeatTick")
	}
	if cfg.DialTimeout != defaultDialTimeout {
		t.Fatalf("expected default DialTimeout")
	}
}

func TestConfigValidate_NodeIDZero(t *testing.T) {
	cfg := validTestConfig()
	cfg.NodeID = 0
	if err := cfg.validate(); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("expected ErrInvalidConfig, got: %v", err)
	}
}

func TestConfigValidate_ListenAddrEmpty(t *testing.T) {
	cfg := validTestConfig()
	cfg.ListenAddr = ""
	if err := cfg.validate(); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("expected ErrInvalidConfig, got: %v", err)
	}
}

func TestConfigValidate_NewStorageNil(t *testing.T) {
	cfg := validTestConfig()
	cfg.NewStorage = nil
	if err := cfg.validate(); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("expected ErrInvalidConfig, got: %v", err)
	}
}

func TestConfigValidate_NewStateMachineNil(t *testing.T) {
	cfg := validTestConfig()
	cfg.NewStateMachine = nil
	if err := cfg.validate(); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("expected ErrInvalidConfig, got: %v", err)
	}
}

func TestConfigValidate_DuplicateNodeID(t *testing.T) {
	cfg := validTestConfig()
	cfg.Nodes = append(cfg.Nodes, NodeConfig{NodeID: 1, Addr: "127.0.0.1:9004"})
	if err := cfg.validate(); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("expected ErrInvalidConfig, got: %v", err)
	}
}

func TestConfigValidate_DuplicateGroupID(t *testing.T) {
	cfg := validTestConfig()
	cfg.GroupCount = 2
	cfg.Groups = append(cfg.Groups, GroupConfig{GroupID: 1, Peers: []multiraft.NodeID{1, 2, 3}})
	if err := cfg.validate(); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("expected ErrInvalidConfig, got: %v", err)
	}
}

func validTestConfig() Config {
	return Config{
		NodeID:     1,
		ListenAddr: ":9001",
		GroupCount: 1,
		NewStorage: func(groupID multiraft.GroupID) (multiraft.Storage, error) {
			return nil, nil
		},
		NewStateMachine: func(groupID multiraft.GroupID) (multiraft.StateMachine, error) {
			return nil, nil
		},
		Nodes: []NodeConfig{
			{NodeID: 1, Addr: "127.0.0.1:9001"},
			{NodeID: 2, Addr: "127.0.0.1:9002"},
			{NodeID: 3, Addr: "127.0.0.1:9003"},
		},
		Groups: []GroupConfig{
			{GroupID: 1, Peers: []multiraft.NodeID{1, 2, 3}},
		},
	}
}
