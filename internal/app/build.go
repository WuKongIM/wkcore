package app

import (
	"context"
	"errors"
	"fmt"
	"time"

	accessapi "github.com/WuKongIM/WuKongIM/internal/access/api"
	accessgateway "github.com/WuKongIM/WuKongIM/internal/access/gateway"
	accessnode "github.com/WuKongIM/WuKongIM/internal/access/node"
	"github.com/WuKongIM/WuKongIM/internal/gateway"
	deliveryruntime "github.com/WuKongIM/WuKongIM/internal/runtime/delivery"
	"github.com/WuKongIM/WuKongIM/internal/runtime/messageid"
	"github.com/WuKongIM/WuKongIM/internal/runtime/online"
	conversationusecase "github.com/WuKongIM/WuKongIM/internal/usecase/conversation"
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
			MaxFetchInflightPeer: dataPlaneMaxFetchInflightPeer(cfg.Cluster.PoolSize),
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
	app.nodeClient = accessnode.NewClient(app.cluster)
	app.conversationProjector = conversationusecase.NewProjector(conversationusecase.ProjectorOptions{
		Store:              app.store,
		FlushInterval:      cfg.Conversation.FlushInterval,
		DirtyLimit:         cfg.Conversation.FlushDirtyLimit,
		ColdThreshold:      cfg.Conversation.ColdThreshold,
		SubscriberPageSize: cfg.Conversation.SubscriberPageSize,
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
	})
	app.channelMetaSync = &channelMetaSync{
		source:          app.store,
		runtime:         app.isrRuntime,
		cluster:         app.channelLog,
		replicaFactory:  app.replicaFactory,
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
	})
	authorityClient.local = app.presenceApp
	app.presenceWorker = newPresenceWorker(app.presenceApp, 0)
	app.deliveryAcks = deliveryruntime.NewAckIndex()
	subscriberResolver := deliveryusecase.NewSubscriberResolver(deliveryusecase.SubscriberResolverOptions{
		Store: app.store,
	})
	app.deliveryRuntime = deliveryruntime.NewManager(deliveryruntime.Config{
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
	})
	committedDispatcher := asyncCommittedDispatcher{
		localNodeID:  cfg.Node.ID,
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
	})
	userApp := userusecase.New(userusecase.Options{
		Users:   app.store,
		Devices: app.store,
		Online:  onlineRegistry,
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

func dataPlaneMaxFetchInflightPeer(poolSize int) int {
	if poolSize > 2 {
		return poolSize
	}
	return 2
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

const conversationFetchMaxBytes = 1 << 20

type channelLogConversationFacts struct {
	cluster channellog.Cluster
	metas   interface {
		GetChannelRuntimeMeta(ctx context.Context, channelID string, channelType int64) (metadb.ChannelRuntimeMeta, error)
	}
	remote interface {
		LoadLatestConversationMessage(ctx context.Context, nodeID uint64, key channellog.ChannelKey, maxBytes int) (channellog.Message, bool, error)
		LoadRecentConversationMessages(ctx context.Context, nodeID uint64, key channellog.ChannelKey, limit, maxBytes int) ([]channellog.Message, error)
	}
}

type batchConversationFactsRemote interface {
	LoadLatestConversationMessages(ctx context.Context, nodeID uint64, keys []channellog.ChannelKey, maxBytes int) (map[channellog.ChannelKey]channellog.Message, error)
	LoadRecentConversationMessagesBatch(ctx context.Context, nodeID uint64, keys []channellog.ChannelKey, limit, maxBytes int) (map[channellog.ChannelKey][]channellog.Message, error)
}

type batchConversationFactsMetas interface {
	BatchGetChannelRuntimeMetas(ctx context.Context, keys []metadb.ConversationKey) (map[metadb.ConversationKey]metadb.ChannelRuntimeMeta, error)
}

func (f channelLogConversationFacts) LoadLatestMessages(ctx context.Context, keys []conversationusecase.ConversationKey) (map[conversationusecase.ConversationKey]channellog.Message, error) {
	out := make(map[conversationusecase.ConversationKey]channellog.Message, len(keys))
	remoteKeys := make([]conversationusecase.ConversationKey, 0, len(keys))
	for _, key := range keys {
		channelKey := channellog.ChannelKey{ChannelID: key.ChannelID, ChannelType: key.ChannelType}
		msg, ok, err := loadLatestConversationMessage(ctx, f.cluster, channelKey, conversationFetchMaxBytes)
		switch {
		case err == nil || errors.Is(err, channellog.ErrChannelNotFound):
			if ok {
				out[key] = msg
			}
		case errors.Is(err, channellog.ErrStaleMeta):
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
		msg, ok, err := f.loadRemoteLatestMessage(ctx, channellog.ChannelKey{ChannelID: key.ChannelID, ChannelType: key.ChannelType})
		if err != nil {
			return nil, err
		}
		if ok {
			out[key] = msg
		}
	}
	return out, nil
}

func (f channelLogConversationFacts) LoadRecentMessages(ctx context.Context, key conversationusecase.ConversationKey, limit int) ([]channellog.Message, error) {
	if limit <= 0 {
		return nil, nil
	}
	messagesByKey, err := f.LoadRecentMessagesBatch(ctx, []conversationusecase.ConversationKey{key}, limit)
	if err != nil {
		return nil, err
	}
	return messagesByKey[key], nil
}

func (f channelLogConversationFacts) LoadRecentMessagesBatch(ctx context.Context, keys []conversationusecase.ConversationKey, limit int) (map[conversationusecase.ConversationKey][]channellog.Message, error) {
	if limit <= 0 {
		return map[conversationusecase.ConversationKey][]channellog.Message{}, nil
	}
	out := make(map[conversationusecase.ConversationKey][]channellog.Message, len(keys))
	remoteKeys := make([]conversationusecase.ConversationKey, 0, len(keys))
	for _, key := range keys {
		channelKey := channellog.ChannelKey{ChannelID: key.ChannelID, ChannelType: key.ChannelType}
		messages, err := loadRecentConversationMessages(ctx, f.cluster, channelKey, limit, conversationFetchMaxBytes)
		switch {
		case err == nil || errors.Is(err, channellog.ErrChannelNotFound):
			out[key] = messages
		case errors.Is(err, channellog.ErrStaleMeta):
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
		messages, err := f.loadRemoteRecentMessages(ctx, channellog.ChannelKey{ChannelID: key.ChannelID, ChannelType: key.ChannelType}, limit)
		if err != nil {
			return nil, err
		}
		out[key] = messages
	}
	return out, nil
}

func (f channelLogConversationFacts) loadLatestMessage(ctx context.Context, key conversationusecase.ConversationKey) (channellog.Message, bool, error) {
	channelKey := channellog.ChannelKey{ChannelID: key.ChannelID, ChannelType: key.ChannelType}
	msg, ok, err := loadLatestConversationMessage(ctx, f.cluster, channelKey, conversationFetchMaxBytes)
	if err == nil || errors.Is(err, channellog.ErrChannelNotFound) {
		return msg, ok, nil
	}
	if !errors.Is(err, channellog.ErrStaleMeta) {
		return channellog.Message{}, false, err
	}
	return f.loadRemoteLatestMessage(ctx, channelKey)
}

func (f channelLogConversationFacts) loadRemoteLatestMessage(ctx context.Context, key channellog.ChannelKey) (channellog.Message, bool, error) {
	ownerNodeID, err := f.ownerNodeID(ctx, key)
	if err != nil {
		return channellog.Message{}, false, err
	}
	if ownerNodeID == 0 || f.remote == nil {
		return channellog.Message{}, false, channellog.ErrStaleMeta
	}
	return f.remote.LoadLatestConversationMessage(ctx, ownerNodeID, key, conversationFetchMaxBytes)
}

func (f channelLogConversationFacts) loadRemoteRecentMessages(ctx context.Context, key channellog.ChannelKey, limit int) ([]channellog.Message, error) {
	ownerNodeID, err := f.ownerNodeID(ctx, key)
	if err != nil {
		return nil, err
	}
	if ownerNodeID == 0 || f.remote == nil {
		return nil, channellog.ErrStaleMeta
	}
	return f.remote.LoadRecentConversationMessages(ctx, ownerNodeID, key, limit, conversationFetchMaxBytes)
}

func (f channelLogConversationFacts) loadRemoteLatestMessagesBatch(ctx context.Context, remote batchConversationFactsRemote, keys []conversationusecase.ConversationKey) (map[conversationusecase.ConversationKey]channellog.Message, error) {
	grouped, err := f.groupConversationKeysByOwner(ctx, keys)
	if err != nil {
		return nil, err
	}
	out := make(map[conversationusecase.ConversationKey]channellog.Message, len(keys))
	for ownerNodeID, groupKeys := range grouped {
		channelKeys := make([]channellog.ChannelKey, 0, len(groupKeys))
		keyByChannel := make(map[channellog.ChannelKey]conversationusecase.ConversationKey, len(groupKeys))
		for _, key := range groupKeys {
			channelKey := channellog.ChannelKey{ChannelID: key.ChannelID, ChannelType: key.ChannelType}
			channelKeys = append(channelKeys, channelKey)
			keyByChannel[channelKey] = key
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

func (f channelLogConversationFacts) loadRemoteRecentMessagesBatch(ctx context.Context, remote batchConversationFactsRemote, keys []conversationusecase.ConversationKey, limit int) (map[conversationusecase.ConversationKey][]channellog.Message, error) {
	grouped, err := f.groupConversationKeysByOwner(ctx, keys)
	if err != nil {
		return nil, err
	}
	out := make(map[conversationusecase.ConversationKey][]channellog.Message, len(keys))
	for ownerNodeID, groupKeys := range grouped {
		channelKeys := make([]channellog.ChannelKey, 0, len(groupKeys))
		keyByChannel := make(map[channellog.ChannelKey]conversationusecase.ConversationKey, len(groupKeys))
		for _, key := range groupKeys {
			channelKey := channellog.ChannelKey{ChannelID: key.ChannelID, ChannelType: key.ChannelType}
			channelKeys = append(channelKeys, channelKey)
			keyByChannel[channelKey] = key
		}
		messagesByChannel, err := remote.LoadRecentConversationMessagesBatch(ctx, ownerNodeID, channelKeys, limit, conversationFetchMaxBytes)
		if err != nil {
			return nil, err
		}
		for channelKey, messages := range messagesByChannel {
			if key, ok := keyByChannel[channelKey]; ok {
				out[key] = append([]channellog.Message(nil), messages...)
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
				return nil, channellog.ErrStaleMeta
			}
			grouped[meta.Leader] = append(grouped[meta.Leader], convKeysByMeta[metaKey])
		}
		return grouped, nil
	}
	for _, key := range keys {
		ownerNodeID, err := f.ownerNodeID(ctx, channellog.ChannelKey{ChannelID: key.ChannelID, ChannelType: key.ChannelType})
		if err != nil {
			return nil, err
		}
		if ownerNodeID == 0 {
			return nil, channellog.ErrStaleMeta
		}
		grouped[ownerNodeID] = append(grouped[ownerNodeID], key)
	}
	return grouped, nil
}

func (f channelLogConversationFacts) ownerNodeID(ctx context.Context, key channellog.ChannelKey) (uint64, error) {
	if f.metas == nil {
		return 0, channellog.ErrStaleMeta
	}
	meta, err := f.metas.GetChannelRuntimeMeta(ctx, key.ChannelID, int64(key.ChannelType))
	if err != nil {
		return 0, err
	}
	return meta.Leader, nil
}

func loadLatestConversationMessage(ctx context.Context, cluster channellog.Cluster, key channellog.ChannelKey, maxBytes int) (channellog.Message, bool, error) {
	if cluster == nil {
		return channellog.Message{}, false, channellog.ErrStaleMeta
	}
	status, err := cluster.Status(key)
	if err != nil {
		return channellog.Message{}, false, err
	}
	if status.CommittedSeq == 0 {
		return channellog.Message{}, false, nil
	}

	fetch, err := cluster.Fetch(ctx, channellog.FetchRequest{
		Key:      key,
		FromSeq:  status.CommittedSeq,
		Limit:    1,
		MaxBytes: maxBytes,
	})
	if err != nil {
		return channellog.Message{}, false, err
	}
	if len(fetch.Messages) == 0 {
		return channellog.Message{}, false, nil
	}
	return fetch.Messages[0], true, nil
}

func loadRecentConversationMessages(ctx context.Context, cluster channellog.Cluster, key channellog.ChannelKey, limit, maxBytes int) ([]channellog.Message, error) {
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
	fetch, err := cluster.Fetch(ctx, channellog.FetchRequest{
		Key:      key,
		FromSeq:  fromSeq,
		Limit:    limit,
		MaxBytes: maxBytes,
	})
	if err != nil {
		return nil, err
	}
	return append([]channellog.Message(nil), fetch.Messages...), nil
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
