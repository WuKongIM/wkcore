package app

import (
	"fmt"

	accessapi "github.com/WuKongIM/WuKongIM/internal/access/api"
	accessgateway "github.com/WuKongIM/WuKongIM/internal/access/gateway"
	"github.com/WuKongIM/WuKongIM/internal/gateway"
	"github.com/WuKongIM/WuKongIM/internal/runtime/online"
	"github.com/WuKongIM/WuKongIM/internal/runtime/sequence"
	"github.com/WuKongIM/WuKongIM/internal/usecase/message"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/raftcluster"
	"github.com/WuKongIM/WuKongIM/pkg/replication/multiraft"
	"github.com/WuKongIM/WuKongIM/pkg/storage/metadb"
	"github.com/WuKongIM/WuKongIM/pkg/storage/metafsm"
	"github.com/WuKongIM/WuKongIM/pkg/storage/metastore"
	"github.com/WuKongIM/WuKongIM/pkg/storage/raftstorage"
)

func build(cfg Config) (_ *App, err error) {
	if err := cfg.ApplyDefaultsAndValidate(); err != nil {
		return nil, err
	}

	app := &App{cfg: cfg}
	defer func() {
		if err == nil {
			return
		}
		if app.raftDB != nil {
			_ = app.raftDB.Close()
			app.raftDB = nil
		}
		if app.db != nil {
			_ = app.db.Close()
			app.db = nil
		}
	}()

	app.db, err = metadb.Open(cfg.Storage.DBPath)
	if err != nil {
		return nil, fmt.Errorf("app: open metadb: %w", err)
	}

	app.raftDB, err = raftstorage.Open(cfg.Storage.RaftPath)
	if err != nil {
		return nil, fmt.Errorf("app: open raftstorage: %w", err)
	}

	app.cluster, err = raftcluster.NewCluster(cfg.Cluster.runtimeConfig(app.db, app.raftDB, cfg.Node.ID))
	if err != nil {
		return nil, fmt.Errorf("app: create cluster: %w", err)
	}

	app.store = metastore.New(app.cluster, app.db)
	onlineRegistry := online.NewRegistry()
	sequenceAllocator := &sequence.MemoryAllocator{}
	app.messageApp = message.New(message.Options{
		IdentityStore: app.store,
		ChannelStore:  app.store,
		ClusterPort:   app.cluster,
		Online:        onlineRegistry,
		Delivery:      online.LocalDelivery{},
		Sequence:      sequenceAllocator,
	})
	if cfg.API.ListenAddr != "" {
		app.api = accessapi.New(accessapi.Options{
			ListenAddr: cfg.API.ListenAddr,
			Messages:   app.messageApp,
		})
	}
	app.gatewayHandler = accessgateway.New(accessgateway.Options{
		Online:   onlineRegistry,
		Messages: app.messageApp,
	})

	app.gateway, err = gateway.New(gateway.Options{
		Handler:        app.gatewayHandler,
		Authenticator:  gateway.NewWKProtoAuthenticator(gateway.WKProtoAuthOptions{TokenAuthOn: cfg.Gateway.TokenAuthOn, NodeID: cfg.Node.ID}),
		DefaultSession: cfg.Gateway.DefaultSession,
		Listeners:      cfg.Gateway.Listeners,
	})
	if err != nil {
		return nil, fmt.Errorf("app: create gateway: %w", err)
	}

	return app, nil
}

func (c ClusterConfig) runtimeConfig(db *metadb.DB, raftDB *raftstorage.DB, nodeID uint64) raftcluster.Config {
	return raftcluster.Config{
		NodeID:          multiraft.NodeID(nodeID),
		ListenAddr:      c.ListenAddr,
		GroupCount:      c.GroupCount,
		NewStorage:      newStorageFactory(raftDB),
		NewStateMachine: metafsm.NewStateMachineFactory(db),
		Nodes:           c.runtimeNodes(),
		Groups:          c.runtimeGroups(),
		ForwardTimeout:  c.ForwardTimeout,
		PoolSize:        c.PoolSize,
		TickInterval:    c.TickInterval,
		RaftWorkers:     c.RaftWorkers,
		ElectionTick:    c.ElectionTick,
		HeartbeatTick:   c.HeartbeatTick,
		DialTimeout:     c.DialTimeout,
	}
}

func (c ClusterConfig) runtimeNodes() []raftcluster.NodeConfig {
	nodes := make([]raftcluster.NodeConfig, 0, len(c.Nodes))
	for _, node := range c.Nodes {
		nodes = append(nodes, raftcluster.NodeConfig{
			NodeID: multiraft.NodeID(node.ID),
			Addr:   node.Addr,
		})
	}
	return nodes
}

func (c ClusterConfig) runtimeGroups() []raftcluster.GroupConfig {
	groups := make([]raftcluster.GroupConfig, 0, len(c.Groups))
	for _, group := range c.Groups {
		peers := make([]multiraft.NodeID, 0, len(group.Peers))
		for _, peerID := range group.Peers {
			peers = append(peers, multiraft.NodeID(peerID))
		}
		groups = append(groups, raftcluster.GroupConfig{
			GroupID: multiraft.GroupID(group.ID),
			Peers:   peers,
		})
	}
	return groups
}

func newStorageFactory(raftDB *raftstorage.DB) func(groupID multiraft.GroupID) (multiraft.Storage, error) {
	return func(groupID multiraft.GroupID) (multiraft.Storage, error) {
		return raftDB.ForGroup(uint64(groupID)), nil
	}
}
