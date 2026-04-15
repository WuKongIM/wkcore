package app

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"time"

	accessapi "github.com/WuKongIM/WuKongIM/internal/access/api"
	accessgateway "github.com/WuKongIM/WuKongIM/internal/access/gateway"
	accessnode "github.com/WuKongIM/WuKongIM/internal/access/node"
	"github.com/WuKongIM/WuKongIM/internal/gateway"
	applog "github.com/WuKongIM/WuKongIM/internal/log"
	deliveryruntime "github.com/WuKongIM/WuKongIM/internal/runtime/delivery"
	"github.com/WuKongIM/WuKongIM/internal/runtime/messageid"
	"github.com/WuKongIM/WuKongIM/internal/runtime/online"
	conversationusecase "github.com/WuKongIM/WuKongIM/internal/usecase/conversation"
	deliveryusecase "github.com/WuKongIM/WuKongIM/internal/usecase/delivery"
	"github.com/WuKongIM/WuKongIM/internal/usecase/message"
	"github.com/WuKongIM/WuKongIM/internal/usecase/presence"
	userusecase "github.com/WuKongIM/WuKongIM/internal/usecase/user"
	"github.com/WuKongIM/WuKongIM/pkg/channel"
	channelruntime "github.com/WuKongIM/WuKongIM/pkg/channel/runtime"
	channelstore "github.com/WuKongIM/WuKongIM/pkg/channel/store"
	channeltransport "github.com/WuKongIM/WuKongIM/pkg/channel/transport"
	raftcluster "github.com/WuKongIM/WuKongIM/pkg/cluster"
	raftstorage "github.com/WuKongIM/WuKongIM/pkg/raftlog"
	metafsm "github.com/WuKongIM/WuKongIM/pkg/slot/fsm"
	metadb "github.com/WuKongIM/WuKongIM/pkg/slot/meta"
	"github.com/WuKongIM/WuKongIM/pkg/slot/multiraft"
	metastore "github.com/WuKongIM/WuKongIM/pkg/slot/proxy"
	"github.com/WuKongIM/WuKongIM/pkg/transport"
	"github.com/WuKongIM/WuKongIM/pkg/wklog"
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
		if app.logger != nil {
			_ = app.logger.Sync()
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

	app.logger, err = applog.NewLogger(applog.Config{
		Level:      cfg.Log.Level,
		Dir:        cfg.Log.Dir,
		MaxSize:    cfg.Log.MaxSize,
		MaxAge:     cfg.Log.MaxAge,
		MaxBackups: cfg.Log.MaxBackups,
		Compress:   cfg.Log.Compress,
		Console:    cfg.Log.Console,
		Format:     cfg.Log.Format,
	})
	if err != nil {
		return nil, fmt.Errorf("app: create logger: %w", err)
	}

	app.db, err = metadb.Open(cfg.Storage.DBPath)
	if err != nil {
		return nil, fmt.Errorf("app: open metadb: %w", err)
	}

	app.raftDB, err = raftstorage.Open(cfg.Storage.RaftPath)
	if err != nil {
		return nil, fmt.Errorf("app: open raftstorage: %w", err)
	}

	app.cluster, err = raftcluster.NewCluster(cfg.Cluster.runtimeConfig(cfg.Storage, app.db, app.raftDB, cfg.Node.ID, app.logger.Named("cluster")))
	if err != nil {
		return nil, fmt.Errorf("app: create cluster: %w", err)
	}

	app.channelLogDB, err = channelstore.Open(cfg.Storage.ChannelLogPath)
	if err != nil {
		return nil, fmt.Errorf("app: open channel store: %w", err)
	}

	messageIDs, err := messageid.NewSnowflakeGenerator(cfg.Node.ID)
	if err != nil {
		return nil, fmt.Errorf("app: create message id generator: %w", err)
	}
	app.gatewayBootID, err = newGatewayBootID()
	if err != nil {
		return nil, fmt.Errorf("app: create gateway boot id: %w", err)
	}

	discovery := raftcluster.NewStaticDiscovery(cfg.Cluster.runtimeNodes())
	poolSize := effectiveDataPlanePoolSize(cfg.Cluster.PoolSize, cfg.Cluster.DataPlanePoolSize)
	dialTimeout := cfg.Cluster.DialTimeout
	if dialTimeout <= 0 {
		dialTimeout = defaultDataPlaneDialTimeout
	}
	app.dataPlanePool = transport.NewPool(discovery, poolSize, dialTimeout)
	app.dataPlaneClient = transport.NewClient(app.dataPlanePool)
	app.isrTransport, err = channeltransport.New(channeltransport.Options{
		LocalNode:          channel.NodeID(cfg.Node.ID),
		Client:             app.dataPlaneClient,
		RPCMux:             app.cluster.RPCMux(),
		RPCTimeout:         cfg.Cluster.DataPlaneRPCTimeout,
		MaxPendingFetchRPC: effectiveDataPlaneMaxPendingFetch(cfg.Cluster.PoolSize, cfg.Cluster.DataPlaneMaxPendingFetch),
	})
	if err != nil {
		return nil, fmt.Errorf("app: create channel transport: %w", err)
	}
	app.isrRuntime, err = channelruntime.New(channelruntime.Config{
		LocalNode:                        channel.NodeID(cfg.Node.ID),
		ReplicaFactory:                   newChannelReplicaFactory(app.channelLogDB, channel.NodeID(cfg.Node.ID), nil),
		GenerationStore:                  newMemoryGenerationStore(),
		Transport:                        app.isrTransport,
		PeerSessions:                     app.isrTransport,
		AutoRunScheduler:                 true,
		FollowerReplicationRetryInterval: time.Second,
		Limits: channelruntime.Limits{
			MaxFetchInflightPeer:      effectiveDataPlaneMaxFetchInflight(cfg.Cluster.PoolSize, cfg.Cluster.DataPlaneMaxFetchInflight),
			MaxSnapshotInflight:       1,
			MaxRecoveryBytesPerSecond: 0,
		},
		Tombstones: channelruntime.TombstonePolicy{
			TombstoneTTL:    time.Minute,
			CleanupInterval: time.Minute,
		},
		Now: time.Now,
	})
	if err != nil {
		return nil, fmt.Errorf("app: create channel runtime: %w", err)
	}
	app.channelLog, err = newAppChannelCluster(app.channelLogDB, app.isrRuntime, app.isrTransport, messageIDs)
	if err != nil {
		return nil, fmt.Errorf("app: create channel cluster: %w", err)
	}

	app.store = metastore.New(app.cluster, app.db)
	app.nodeClient = accessnode.NewClient(app.cluster)
	app.conversationProjector = conversationusecase.NewProjector(conversationusecase.ProjectorOptions{
		Store:              app.store,
		FlushInterval:      cfg.Conversation.FlushInterval,
		DirtyLimit:         cfg.Conversation.FlushDirtyLimit,
		ColdThreshold:      cfg.Conversation.ColdThreshold,
		SubscriberPageSize: cfg.Conversation.SubscriberPageSize,
		Logger:             app.logger.Named("conversation.projector"),
	})
	app.store.RegisterChannelUpdateOverlay(app.conversationProjector)
	app.conversationApp = conversationusecase.New(conversationusecase.Options{
		States:        app.store,
		ChannelUpdate: app.store,
		Facts: channelLogConversationFacts{
			cluster: app.channelLog,
			metas:   app.store,
			remote:  app.nodeClient,
		},
		Now:                   time.Now,
		ColdThreshold:         cfg.Conversation.ColdThreshold,
		ActiveScanLimit:       cfg.Conversation.ActiveScanLimit,
		ChannelProbeBatchSize: cfg.Conversation.ChannelProbeBatchSize,
		Logger:                app.logger.Named("conversation"),
	})
	app.channelMetaSync = &channelMetaSync{
		source:          app.store,
		cluster:         app.channelLog,
		localNode:       cfg.Node.ID,
		refreshInterval: time.Second,
	}
	onlineRegistry := online.NewRegistry()
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
		Logger:           app.logger.Named("presence"),
	})
	authorityClient.local = app.presenceApp
	app.presenceWorker = newPresenceWorker(app.presenceApp, 0)
	app.deliveryAcks = deliveryruntime.NewAckIndex()
	subscriberResolver := deliveryusecase.NewSubscriberResolver(deliveryusecase.SubscriberResolverOptions{
		Store: app.store,
	})
	app.deliveryRuntime = deliveryruntime.NewManager(deliveryruntime.Config{
		ShardCount: deliveryShardCountForParallelism(runtime.GOMAXPROCS(0)),
		Resolver: localDeliveryResolver{
			subscribers: subscriberResolver,
			authority:   authorityClient,
		},
		Push: distributedDeliveryPush{
			localNodeID: cfg.Node.ID,
			local: localDeliveryPush{
				online:        onlineRegistry,
				localNodeID:   cfg.Node.ID,
				gatewayBootID: app.gatewayBootID,
			},
			client: app.nodeClient,
		},
	})
	app.deliveryApp = deliveryusecase.New(deliveryusecase.Options{
		Runtime: app.deliveryRuntime,
		Logger:  app.logger.Named("delivery"),
	})
	committedDispatcher := asyncCommittedDispatcher{
		localNodeID:  cfg.Node.ID,
		preferLocal:  true,
		channelLog:   app.channelLog,
		delivery:     app.deliveryApp,
		conversation: app.conversationProjector,
		nodeClient:   app.nodeClient,
	}
	app.nodeAccess = accessnode.New(accessnode.Options{
		Cluster:          app.cluster,
		Presence:         app.presenceApp,
		Online:           onlineRegistry,
		GatewayBootID:    app.gatewayBootID,
		LocalNodeID:      cfg.Node.ID,
		ChannelLog:       app.channelLog,
		DeliverySubmit:   committedDispatcher,
		DeliveryAck:      app.deliveryApp,
		DeliveryOffline:  app.deliveryApp,
		DeliveryAckIndex: app.deliveryAcks,
		Logger:           app.logger.Named("access.node"),
	})
	app.messageApp = message.New(message.Options{
		IdentityStore:       app.store,
		ChannelStore:        app.store,
		Cluster:             app.channelLog,
		MetaRefresher:       app.channelMetaSync,
		Online:              onlineRegistry,
		CommittedDispatcher: committedDispatcher,
		DeliveryAck: ackRouting{
			localNodeID: cfg.Node.ID,
			local:       app.deliveryApp,
			remoteAcks:  app.deliveryAcks,
			notifier:    app.nodeClient,
		},
		DeliveryOffline: offlineRouting{
			localNodeID: cfg.Node.ID,
			local:       app.deliveryApp,
			remoteAcks:  app.deliveryAcks,
			notifier:    app.nodeClient,
		},
		Logger: app.logger.Named("message"),
	})
	userApp := userusecase.New(userusecase.Options{
		Users:   app.store,
		Devices: app.store,
		Online:  onlineRegistry,
		Logger:  app.logger.Named("user"),
	})
	if cfg.API.ListenAddr != "" {
		app.api = accessapi.New(accessapi.Options{
			ListenAddr:               cfg.API.ListenAddr,
			Messages:                 app.messageApp,
			Users:                    userApp,
			Conversations:            app.conversationApp,
			ConversationSyncEnabled:  cfg.Conversation.SyncEnabled,
			ConversationDefaultLimit: cfg.Conversation.SyncDefaultLimit,
			ConversationMaxLimit:     cfg.Conversation.SyncMaxLimit,
			Logger:                   app.logger.Named("access.api"),
		})
	}
	app.gatewayHandler = accessgateway.New(accessgateway.Options{
		Online:   onlineRegistry,
		Messages: app.messageApp,
		Presence: app.presenceApp,
		Logger:   app.logger.Named("access.gateway"),
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

func deliveryShardCountForParallelism(parallelism int) int {
	if parallelism <= 0 {
		parallelism = 1
	}
	return min(16, max(4, parallelism))
}

func dataPlaneMaxFetchInflightPeer(poolSize int) int {
	if poolSize > 2 {
		return poolSize
	}
	return 2
}

func effectiveDataPlanePoolSize(clusterPoolSize, configured int) int {
	if configured > 0 {
		return configured
	}
	if clusterPoolSize > 0 {
		return clusterPoolSize
	}
	return 1
}

func effectiveDataPlaneMaxFetchInflight(clusterPoolSize, configured int) int {
	if configured > 0 {
		return configured
	}
	return dataPlaneMaxFetchInflightPeer(clusterPoolSize)
}

func effectiveDataPlaneMaxPendingFetch(clusterPoolSize, configured int) int {
	if configured > 0 {
		return configured
	}
	return dataPlaneMaxFetchInflightPeer(clusterPoolSize)
}

func (c ClusterConfig) runtimeConfig(storage StorageConfig, db *metadb.DB, raftDB *raftstorage.DB, nodeID uint64, logger wklog.Logger) raftcluster.Config {
	return raftcluster.Config{
		NodeID:                       multiraft.NodeID(nodeID),
		ListenAddr:                   c.ListenAddr,
		SlotCount:                    c.SlotCount,
		HashSlotCount:                c.HashSlotCount,
		InitialSlotCount:             c.InitialSlotCount,
		ControllerMetaPath:           storage.ControllerMetaPath,
		ControllerRaftPath:           storage.ControllerRaftPath,
		ControllerReplicaN:           c.ControllerReplicaN,
		SlotReplicaN:                 c.SlotReplicaN,
		NewStorage:                   newStorageFactory(raftDB),
		NewStateMachine:              metafsm.NewStateMachineFactory(db),
		NewStateMachineWithHashSlots: metafsm.NewHashSlotStateMachineFactory(db),
		Nodes:                        c.runtimeNodes(),
		ForwardTimeout:               c.ForwardTimeout,
		PoolSize:                     c.PoolSize,
		TickInterval:                 c.TickInterval,
		RaftWorkers:                  c.RaftWorkers,
		ElectionTick:                 c.ElectionTick,
		HeartbeatTick:                c.HeartbeatTick,
		DialTimeout:                  c.DialTimeout,
		Timeouts:                     c.Timeouts,
		Logger:                       logger,
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

const conversationFetchMaxBytes = 1 << 20

type channelLogConversationFacts struct {
	cluster interface {
		Status(id channel.ChannelID) (channel.ChannelRuntimeStatus, error)
		Fetch(ctx context.Context, req channel.FetchRequest) (channel.FetchResult, error)
	}
	metas interface {
		GetChannelRuntimeMeta(ctx context.Context, channelID string, channelType int64) (metadb.ChannelRuntimeMeta, error)
	}
	remote interface {
		LoadLatestConversationMessage(ctx context.Context, nodeID uint64, key channel.ChannelID, maxBytes int) (channel.Message, bool, error)
		LoadRecentConversationMessages(ctx context.Context, nodeID uint64, key channel.ChannelID, limit, maxBytes int) ([]channel.Message, error)
	}
}

type batchConversationFactsRemote interface {
	LoadLatestConversationMessages(ctx context.Context, nodeID uint64, keys []channel.ChannelID, maxBytes int) (map[channel.ChannelID]channel.Message, error)
	LoadRecentConversationMessagesBatch(ctx context.Context, nodeID uint64, keys []channel.ChannelID, limit, maxBytes int) (map[channel.ChannelID][]channel.Message, error)
}

type batchConversationFactsMetas interface {
	BatchGetChannelRuntimeMetas(ctx context.Context, keys []metadb.ConversationKey) (map[metadb.ConversationKey]metadb.ChannelRuntimeMeta, error)
}

func (f channelLogConversationFacts) LoadLatestMessages(ctx context.Context, keys []conversationusecase.ConversationKey) (map[conversationusecase.ConversationKey]channel.Message, error) {
	out := make(map[conversationusecase.ConversationKey]channel.Message, len(keys))
	remoteKeys := make([]conversationusecase.ConversationKey, 0, len(keys))
	for _, key := range keys {
		channelID := channel.ChannelID{ID: key.ChannelID, Type: key.ChannelType}
		msg, ok, err := loadLatestConversationMessage(ctx, f.cluster, channelID, conversationFetchMaxBytes)
		switch {
		case err == nil || errors.Is(err, channel.ErrChannelNotFound):
			if ok {
				out[key] = msg
			}
		case errors.Is(err, channel.ErrStaleMeta):
			remoteKeys = append(remoteKeys, key)
		default:
			return nil, err
		}
	}
	if len(remoteKeys) == 0 {
		return out, nil
	}
	if remote, ok := f.remote.(batchConversationFactsRemote); ok {
		remoteMessages, err := f.loadRemoteLatestMessagesBatch(ctx, remote, remoteKeys)
		if err != nil {
			return nil, err
		}
		for key, msg := range remoteMessages {
			out[key] = msg
		}
		return out, nil
	}
	for _, key := range remoteKeys {
		msg, ok, err := f.loadRemoteLatestMessage(ctx, channel.ChannelID{ID: key.ChannelID, Type: key.ChannelType})
		if err != nil {
			return nil, err
		}
		if ok {
			out[key] = msg
		}
	}
	return out, nil
}

func (f channelLogConversationFacts) LoadRecentMessages(ctx context.Context, key conversationusecase.ConversationKey, limit int) ([]channel.Message, error) {
	if limit <= 0 {
		return nil, nil
	}
	messagesByKey, err := f.LoadRecentMessagesBatch(ctx, []conversationusecase.ConversationKey{key}, limit)
	if err != nil {
		return nil, err
	}
	return messagesByKey[key], nil
}

func (f channelLogConversationFacts) LoadRecentMessagesBatch(ctx context.Context, keys []conversationusecase.ConversationKey, limit int) (map[conversationusecase.ConversationKey][]channel.Message, error) {
	if limit <= 0 {
		return map[conversationusecase.ConversationKey][]channel.Message{}, nil
	}
	out := make(map[conversationusecase.ConversationKey][]channel.Message, len(keys))
	remoteKeys := make([]conversationusecase.ConversationKey, 0, len(keys))
	for _, key := range keys {
		channelID := channel.ChannelID{ID: key.ChannelID, Type: key.ChannelType}
		messages, err := loadRecentConversationMessages(ctx, f.cluster, channelID, limit, conversationFetchMaxBytes)
		switch {
		case err == nil || errors.Is(err, channel.ErrChannelNotFound):
			out[key] = messages
		case errors.Is(err, channel.ErrStaleMeta):
			remoteKeys = append(remoteKeys, key)
		default:
			return nil, err
		}
	}
	if len(remoteKeys) == 0 {
		return out, nil
	}
	if remote, ok := f.remote.(batchConversationFactsRemote); ok {
		remoteMessages, err := f.loadRemoteRecentMessagesBatch(ctx, remote, remoteKeys, limit)
		if err != nil {
			return nil, err
		}
		for key, messages := range remoteMessages {
			out[key] = messages
		}
		return out, nil
	}
	for _, key := range remoteKeys {
		messages, err := f.loadRemoteRecentMessages(ctx, channel.ChannelID{ID: key.ChannelID, Type: key.ChannelType}, limit)
		if err != nil {
			return nil, err
		}
		out[key] = messages
	}
	return out, nil
}

func (f channelLogConversationFacts) loadLatestMessage(ctx context.Context, key conversationusecase.ConversationKey) (channel.Message, bool, error) {
	channelID := channel.ChannelID{ID: key.ChannelID, Type: key.ChannelType}
	msg, ok, err := loadLatestConversationMessage(ctx, f.cluster, channelID, conversationFetchMaxBytes)
	if err == nil || errors.Is(err, channel.ErrChannelNotFound) {
		return msg, ok, nil
	}
	if !errors.Is(err, channel.ErrStaleMeta) {
		return channel.Message{}, false, err
	}
	return f.loadRemoteLatestMessage(ctx, channelID)
}

func (f channelLogConversationFacts) loadRemoteLatestMessage(ctx context.Context, key channel.ChannelID) (channel.Message, bool, error) {
	ownerNodeID, err := f.ownerNodeID(ctx, key)
	if err != nil {
		return channel.Message{}, false, err
	}
	if ownerNodeID == 0 || f.remote == nil {
		return channel.Message{}, false, channel.ErrStaleMeta
	}
	return f.remote.LoadLatestConversationMessage(ctx, ownerNodeID, key, conversationFetchMaxBytes)
}

func (f channelLogConversationFacts) loadRemoteRecentMessages(ctx context.Context, key channel.ChannelID, limit int) ([]channel.Message, error) {
	ownerNodeID, err := f.ownerNodeID(ctx, key)
	if err != nil {
		return nil, err
	}
	if ownerNodeID == 0 || f.remote == nil {
		return nil, channel.ErrStaleMeta
	}
	return f.remote.LoadRecentConversationMessages(ctx, ownerNodeID, key, limit, conversationFetchMaxBytes)
}

func (f channelLogConversationFacts) loadRemoteLatestMessagesBatch(ctx context.Context, remote batchConversationFactsRemote, keys []conversationusecase.ConversationKey) (map[conversationusecase.ConversationKey]channel.Message, error) {
	grouped, err := f.groupConversationKeysByOwner(ctx, keys)
	if err != nil {
		return nil, err
	}
	out := make(map[conversationusecase.ConversationKey]channel.Message, len(keys))
	for ownerNodeID, groupKeys := range grouped {
		channelKeys := make([]channel.ChannelID, 0, len(groupKeys))
		keyByChannel := make(map[channel.ChannelID]conversationusecase.ConversationKey, len(groupKeys))
		for _, key := range groupKeys {
			channelID := channel.ChannelID{ID: key.ChannelID, Type: key.ChannelType}
			channelKeys = append(channelKeys, channelID)
			keyByChannel[channelID] = key
		}
		messages, err := remote.LoadLatestConversationMessages(ctx, ownerNodeID, channelKeys, conversationFetchMaxBytes)
		if err != nil {
			return nil, err
		}
		for channelKey, msg := range messages {
			if key, ok := keyByChannel[channelKey]; ok {
				out[key] = msg
			}
		}
	}
	return out, nil
}

func (f channelLogConversationFacts) loadRemoteRecentMessagesBatch(ctx context.Context, remote batchConversationFactsRemote, keys []conversationusecase.ConversationKey, limit int) (map[conversationusecase.ConversationKey][]channel.Message, error) {
	grouped, err := f.groupConversationKeysByOwner(ctx, keys)
	if err != nil {
		return nil, err
	}
	out := make(map[conversationusecase.ConversationKey][]channel.Message, len(keys))
	for ownerNodeID, groupKeys := range grouped {
		channelKeys := make([]channel.ChannelID, 0, len(groupKeys))
		keyByChannel := make(map[channel.ChannelID]conversationusecase.ConversationKey, len(groupKeys))
		for _, key := range groupKeys {
			channelID := channel.ChannelID{ID: key.ChannelID, Type: key.ChannelType}
			channelKeys = append(channelKeys, channelID)
			keyByChannel[channelID] = key
		}
		messagesByChannel, err := remote.LoadRecentConversationMessagesBatch(ctx, ownerNodeID, channelKeys, limit, conversationFetchMaxBytes)
		if err != nil {
			return nil, err
		}
		for channelKey, messages := range messagesByChannel {
			if key, ok := keyByChannel[channelKey]; ok {
				out[key] = append([]channel.Message(nil), messages...)
			}
		}
	}
	return out, nil
}

func (f channelLogConversationFacts) groupConversationKeysByOwner(ctx context.Context, keys []conversationusecase.ConversationKey) (map[uint64][]conversationusecase.ConversationKey, error) {
	grouped := make(map[uint64][]conversationusecase.ConversationKey, len(keys))
	if metas, ok := f.metas.(batchConversationFactsMetas); ok {
		metaKeys := make([]metadb.ConversationKey, 0, len(keys))
		convKeysByMeta := make(map[metadb.ConversationKey]conversationusecase.ConversationKey, len(keys))
		for _, key := range keys {
			metaKey := metadb.ConversationKey{ChannelID: key.ChannelID, ChannelType: int64(key.ChannelType)}
			metaKeys = append(metaKeys, metaKey)
			convKeysByMeta[metaKey] = key
		}
		metasByKey, err := metas.BatchGetChannelRuntimeMetas(ctx, metaKeys)
		if err != nil {
			return nil, err
		}
		for _, metaKey := range metaKeys {
			meta, ok := metasByKey[metaKey]
			if !ok || meta.Leader == 0 {
				return nil, channel.ErrStaleMeta
			}
			grouped[meta.Leader] = append(grouped[meta.Leader], convKeysByMeta[metaKey])
		}
		return grouped, nil
	}
	for _, key := range keys {
		ownerNodeID, err := f.ownerNodeID(ctx, channel.ChannelID{ID: key.ChannelID, Type: key.ChannelType})
		if err != nil {
			return nil, err
		}
		if ownerNodeID == 0 {
			return nil, channel.ErrStaleMeta
		}
		grouped[ownerNodeID] = append(grouped[ownerNodeID], key)
	}
	return grouped, nil
}

func (f channelLogConversationFacts) ownerNodeID(ctx context.Context, key channel.ChannelID) (uint64, error) {
	if f.metas == nil {
		return 0, channel.ErrStaleMeta
	}
	meta, err := f.metas.GetChannelRuntimeMeta(ctx, key.ID, int64(key.Type))
	if err != nil {
		return 0, err
	}
	return meta.Leader, nil
}

func loadLatestConversationMessage(ctx context.Context, cluster interface {
	Status(id channel.ChannelID) (channel.ChannelRuntimeStatus, error)
	Fetch(ctx context.Context, req channel.FetchRequest) (channel.FetchResult, error)
}, key channel.ChannelID, maxBytes int) (channel.Message, bool, error) {
	if cluster == nil {
		return channel.Message{}, false, channel.ErrStaleMeta
	}
	status, err := cluster.Status(key)
	if err != nil {
		return channel.Message{}, false, err
	}
	if status.CommittedSeq == 0 {
		return channel.Message{}, false, nil
	}

	fetch, err := cluster.Fetch(ctx, channel.FetchRequest{
		ChannelID: key,
		FromSeq:   status.CommittedSeq,
		Limit:     1,
		MaxBytes:  maxBytes,
	})
	if err != nil {
		return channel.Message{}, false, err
	}
	if len(fetch.Messages) == 0 {
		return channel.Message{}, false, nil
	}
	return fetch.Messages[0], true, nil
}

func loadRecentConversationMessages(ctx context.Context, cluster interface {
	Status(id channel.ChannelID) (channel.ChannelRuntimeStatus, error)
	Fetch(ctx context.Context, req channel.FetchRequest) (channel.FetchResult, error)
}, key channel.ChannelID, limit, maxBytes int) ([]channel.Message, error) {
	if cluster == nil || limit <= 0 {
		return nil, nil
	}
	status, err := cluster.Status(key)
	if err != nil {
		return nil, err
	}
	if status.CommittedSeq == 0 {
		return nil, nil
	}

	fromSeq := uint64(1)
	if status.CommittedSeq >= uint64(limit) {
		fromSeq = status.CommittedSeq - uint64(limit) + 1
	}
	fetch, err := cluster.Fetch(ctx, channel.FetchRequest{
		ChannelID: key,
		FromSeq:   fromSeq,
		Limit:     limit,
		MaxBytes:  maxBytes,
	})
	if err != nil {
		return nil, err
	}
	return append([]channel.Message(nil), fetch.Messages...), nil
}

func newStorageFactory(raftDB *raftstorage.DB) func(slotID multiraft.SlotID) (multiraft.Storage, error) {
	return func(slotID multiraft.SlotID) (multiraft.Storage, error) {
		return raftDB.ForSlot(uint64(slotID)), nil
	}
}

type presenceRouter struct {
	cluster raftcluster.API
}

func (r presenceRouter) SlotForKey(key string) uint64 {
	if r.cluster == nil {
		return 0
	}
	return uint64(r.cluster.SlotForKey(key))
}
