package app

import (
	"fmt"
	"time"

	accessapi "github.com/WuKongIM/WuKongIM/internal/access/api"
	accessgateway "github.com/WuKongIM/WuKongIM/internal/access/gateway"
	accessnode "github.com/WuKongIM/WuKongIM/internal/access/node"
	"github.com/WuKongIM/WuKongIM/internal/gateway"
	deliveryruntime "github.com/WuKongIM/WuKongIM/internal/runtime/delivery"
	"github.com/WuKongIM/WuKongIM/internal/runtime/messageid"
	"github.com/WuKongIM/WuKongIM/internal/runtime/online"
	deliveryusecase "github.com/WuKongIM/WuKongIM/internal/usecase/delivery"
	"github.com/WuKongIM/WuKongIM/internal/usecase/message"
	"github.com/WuKongIM/WuKongIM/internal/usecase/presence"
	userusecase "github.com/WuKongIM/WuKongIM/internal/usecase/user"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/raftcluster"
	"github.com/WuKongIM/WuKongIM/pkg/replication/isr"
	"github.com/WuKongIM/WuKongIM/pkg/replication/isrnode"
	"github.com/WuKongIM/WuKongIM/pkg/replication/multiraft"
	"github.com/WuKongIM/WuKongIM/pkg/storage/channellog"
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
		if app.channelLogDB != nil {
			_ = app.channelLogDB.Close()
			app.channelLogDB = nil
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

	app.channelLogDB, err = channellog.Open(cfg.Storage.ChannelLogPath)
	if err != nil {
		return nil, fmt.Errorf("app: open channel log db: %w", err)
	}

	messageIDs, err := messageid.NewSnowflakeGenerator(cfg.Node.ID)
	if err != nil {
		return nil, fmt.Errorf("app: create message id generator: %w", err)
	}
	app.gatewayBootID, err = newGatewayBootID()
	if err != nil {
		return nil, fmt.Errorf("app: create gateway boot id: %w", err)
	}

	app.isrTransport = newISRTransportBridge()
	app.replicaFactory = newChannelReplicaFactory(app.channelLogDB, isr.NodeID(cfg.Node.ID), nil)
	app.isrRuntime, err = isrnode.New(isrnode.Config{
		LocalNode:        isr.NodeID(cfg.Node.ID),
		ReplicaFactory:   app.replicaFactory,
		GenerationStore:  newMemoryGenerationStore(),
		Transport:        app.isrTransport,
		PeerSessions:     app.isrTransport,
		AutoRunScheduler: true,
		Limits: isrnode.Limits{
			MaxFetchInflightPeer: 1,
			MaxSnapshotInflight:  1,
		},
		Tombstones: isrnode.TombstonePolicy{
			TombstoneTTL: time.Minute,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("app: create isr runtime: %w", err)
	}

	app.channelLog, err = channellog.New(channellog.Config{
		Runtime:    newChannelLogRuntimeAdapter(app.isrRuntime),
		Log:        app.channelLogDB,
		States:     app.channelLogDB.StateStoreFactory(),
		MessageIDs: messageIDs,
	})
	if err != nil {
		return nil, fmt.Errorf("app: create channel log cluster: %w", err)
	}

	app.store = metastore.New(app.cluster, app.db)
	app.channelMetaSync = &channelMetaSync{
		source:          app.store,
		runtime:         app.isrRuntime,
		cluster:         app.channelLog,
		replicaFactory:  app.replicaFactory,
		localNode:       cfg.Node.ID,
		refreshInterval: time.Second,
	}
	onlineRegistry := online.NewRegistry()
	app.nodeClient = accessnode.NewClient(app.cluster)
	authorityClient := &presenceAuthorityClient{
		cluster:     app.cluster,
		remote:      app.nodeClient,
		localNodeID: cfg.Node.ID,
	}
	app.presenceApp = presence.New(presence.Options{
		LocalNodeID:      cfg.Node.ID,
		GatewayBootID:    app.gatewayBootID,
		Online:           onlineRegistry,
		Router:           presenceRouter{cluster: app.cluster},
		AuthorityClient:  authorityClient,
		ActionDispatcher: authorityClient,
	})
	authorityClient.local = app.presenceApp
	app.nodeAccess = accessnode.New(accessnode.Options{
		Cluster:       app.cluster,
		Presence:      app.presenceApp,
		Online:        onlineRegistry,
		GatewayBootID: app.gatewayBootID,
	})
	app.presenceWorker = newPresenceWorker(app.presenceApp, 0)
	app.deliveryRuntime = deliveryruntime.NewManager(deliveryruntime.Config{
		Resolver: localDeliveryResolver{
			authority: app.presenceApp,
		},
		Push: localDeliveryPush{
			online:        onlineRegistry,
			localNodeID:   cfg.Node.ID,
			gatewayBootID: app.gatewayBootID,
			now:           time.Now,
		},
	})
	app.deliveryApp = deliveryusecase.New(deliveryusecase.Options{
		Runtime: app.deliveryRuntime,
	})
	app.messageApp = message.New(message.Options{
		IdentityStore:       app.store,
		ChannelStore:        app.store,
		Cluster:             app.channelLog,
		MetaRefresher:       app.channelMetaSync,
		Online:              onlineRegistry,
		CommittedDispatcher: asyncCommittedDispatcher{delivery: app.deliveryApp},
		DeliveryAck:         app.deliveryApp,
		DeliveryOffline:     app.deliveryApp,
	})
	userApp := userusecase.New(userusecase.Options{
		Users:   app.store,
		Devices: app.store,
		Online:  onlineRegistry,
	})
	if cfg.API.ListenAddr != "" {
		app.api = accessapi.New(accessapi.Options{
			ListenAddr: cfg.API.ListenAddr,
			Messages:   app.messageApp,
			Users:      userApp,
		})
	}
	app.gatewayHandler = accessgateway.New(accessgateway.Options{
		Online:   onlineRegistry,
		Messages: app.messageApp,
		Presence: app.presenceApp,
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

type presenceRouter struct {
	cluster *raftcluster.Cluster
}

func (r presenceRouter) SlotForKey(key string) uint64 {
	if r.cluster == nil {
		return 0
	}
	return uint64(r.cluster.SlotForKey(key))
}
