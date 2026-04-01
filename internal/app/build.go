package app

import (
	"fmt"

	"github.com/WuKongIM/WuKongIM/internal/gateway"
	"github.com/WuKongIM/WuKongIM/internal/service"
	"github.com/WuKongIM/WuKongIM/pkg/multiraft"
	"github.com/WuKongIM/WuKongIM/pkg/raftstore"
	"github.com/WuKongIM/WuKongIM/pkg/wkcluster"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/WuKongIM/WuKongIM/pkg/wkstore"
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

	app.db, err = wkdb.Open(cfg.Storage.DBPath)
	if err != nil {
		return nil, fmt.Errorf("app: open wkdb: %w", err)
	}

	app.raftDB, err = raftstore.Open(cfg.Storage.RaftPath)
	if err != nil {
		return nil, fmt.Errorf("app: open raftstore: %w", err)
	}

	app.cluster, err = wkcluster.NewCluster(cfg.Cluster.runtimeConfig(app.db, app.raftDB, cfg.Node.ID))
	if err != nil {
		return nil, fmt.Errorf("app: create cluster: %w", err)
	}

	app.store = wkstore.New(app.cluster, app.db)
	app.service = service.New(service.Options{
		IdentityStore: app.store,
		ChannelStore:  app.store,
		ClusterPort:   app.cluster,
	})

	app.gateway, err = gateway.New(gateway.Options{
		Handler:        app.service,
		Authenticator:  gateway.NewWKProtoAuthenticator(gateway.WKProtoAuthOptions{TokenAuthOn: cfg.Gateway.TokenAuthOn, NodeID: cfg.Node.ID}),
		DefaultSession: cfg.Gateway.DefaultSession,
		Listeners:      cfg.Gateway.Listeners,
	})
	if err != nil {
		return nil, fmt.Errorf("app: create gateway: %w", err)
	}

	return app, nil
}

func (c ClusterConfig) runtimeConfig(db *wkdb.DB, raftDB *raftstore.DB, nodeID uint64) wkcluster.Config {
	return wkcluster.Config{
		NodeID:          multiraft.NodeID(nodeID),
		ListenAddr:      c.ListenAddr,
		GroupCount:      c.GroupCount,
		NewStorage:      newStorageFactory(raftDB),
		NewStateMachine: wkstore.NewStateMachineFactory(db),
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

func (c ClusterConfig) runtimeNodes() []wkcluster.NodeConfig {
	nodes := make([]wkcluster.NodeConfig, 0, len(c.Nodes))
	for _, node := range c.Nodes {
		nodes = append(nodes, wkcluster.NodeConfig{
			NodeID: multiraft.NodeID(node.ID),
			Addr:   node.Addr,
		})
	}
	return nodes
}

func (c ClusterConfig) runtimeGroups() []wkcluster.GroupConfig {
	groups := make([]wkcluster.GroupConfig, 0, len(c.Groups))
	for _, group := range c.Groups {
		peers := make([]multiraft.NodeID, 0, len(group.Peers))
		for _, peerID := range group.Peers {
			peers = append(peers, multiraft.NodeID(peerID))
		}
		groups = append(groups, wkcluster.GroupConfig{
			GroupID: multiraft.GroupID(group.ID),
			Peers:   peers,
		})
	}
	return groups
}

func newStorageFactory(raftDB *raftstore.DB) func(groupID multiraft.GroupID) (multiraft.Storage, error) {
	return func(groupID multiraft.GroupID) (multiraft.Storage, error) {
		return raftDB.ForGroup(uint64(groupID)), nil
	}
}
