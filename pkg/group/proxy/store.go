package proxy

import (
	"context"
	"errors"

	raftcluster "github.com/WuKongIM/WuKongIM/pkg/cluster"
	metafsm "github.com/WuKongIM/WuKongIM/pkg/group/fsm"
	metadb "github.com/WuKongIM/WuKongIM/pkg/group/meta"
)

// Store provides business-level distributed storage APIs
// built on top of raftcluster's generic Propose mechanism.
type Store struct {
	cluster              *raftcluster.Cluster
	db                   *metadb.DB
	channelUpdateOverlay ChannelUpdateOverlay
}

type ChannelUpdateOverlay interface {
	BatchGetHotChannelUpdates(ctx context.Context, keys []metadb.ConversationKey) (map[metadb.ConversationKey]metadb.ChannelUpdateLog, error)
}

// New creates a Store.
func New(cluster *raftcluster.Cluster, db *metadb.DB) *Store {
	store := &Store{cluster: cluster, db: db}
	if cluster != nil && cluster.RPCMux() != nil {
		cluster.RPCMux().Handle(runtimeMetaRPCServiceID, store.handleRuntimeMetaRPC)
		cluster.RPCMux().Handle(identityRPCServiceID, store.handleIdentityRPC)
		cluster.RPCMux().Handle(subscriberRPCServiceID, store.handleSubscriberRPC)
		cluster.RPCMux().Handle(userConversationStateRPCServiceID, store.handleUserConversationStateRPC)
		cluster.RPCMux().Handle(channelUpdateLogRPCServiceID, store.handleChannelUpdateLogRPC)
	}
	return store
}

func (s *Store) RegisterChannelUpdateOverlay(overlay ChannelUpdateOverlay) {
	if s == nil {
		return
	}
	s.channelUpdateOverlay = overlay
}

func (s *Store) CreateChannel(ctx context.Context, channelID string, channelType int64) error {
	groupID := s.cluster.SlotForKey(channelID)
	cmd := metafsm.EncodeUpsertChannelCommand(metadb.Channel{
		ChannelID:   channelID,
		ChannelType: channelType,
	})
	return s.cluster.Propose(ctx, groupID, cmd)
}

func (s *Store) UpdateChannel(ctx context.Context, channelID string, channelType int64, ban int64) error {
	groupID := s.cluster.SlotForKey(channelID)
	cmd := metafsm.EncodeUpsertChannelCommand(metadb.Channel{
		ChannelID:   channelID,
		ChannelType: channelType,
		Ban:         ban,
	})
	return s.cluster.Propose(ctx, groupID, cmd)
}

func (s *Store) DeleteChannel(ctx context.Context, channelID string, channelType int64) error {
	groupID := s.cluster.SlotForKey(channelID)
	cmd := metafsm.EncodeDeleteChannelCommand(channelID, channelType)
	return s.cluster.Propose(ctx, groupID, cmd)
}

func (s *Store) GetChannel(ctx context.Context, channelID string, channelType int64) (metadb.Channel, error) {
	groupID := s.cluster.SlotForKey(channelID)
	return s.db.ForSlot(uint64(groupID)).GetChannel(ctx, channelID, channelType)
}

func (s *Store) AddChannelSubscribers(ctx context.Context, channelID string, channelType int64, uids []string) error {
	groupID := s.cluster.SlotForKey(channelID)
	cmd := metafsm.EncodeAddSubscribersCommand(channelID, channelType, uids)
	return s.cluster.Propose(ctx, groupID, cmd)
}

func (s *Store) RemoveChannelSubscribers(ctx context.Context, channelID string, channelType int64, uids []string) error {
	groupID := s.cluster.SlotForKey(channelID)
	cmd := metafsm.EncodeRemoveSubscribersCommand(channelID, channelType, uids)
	return s.cluster.Propose(ctx, groupID, cmd)
}

func (s *Store) ListChannelSubscribers(ctx context.Context, channelID string, channelType int64, afterUID string, limit int) ([]string, string, bool, error) {
	groupID := s.cluster.SlotForKey(channelID)
	return s.listChannelSubscribersAuthoritative(ctx, groupID, channelID, channelType, afterUID, limit)
}

func (s *Store) UpsertChannelRuntimeMeta(ctx context.Context, meta metadb.ChannelRuntimeMeta) error {
	groupID := s.cluster.SlotForKey(meta.ChannelID)
	cmd := metafsm.EncodeUpsertChannelRuntimeMetaCommand(meta)
	return s.cluster.Propose(ctx, groupID, cmd)
}

func (s *Store) GetChannelRuntimeMeta(ctx context.Context, channelID string, channelType int64) (metadb.ChannelRuntimeMeta, error) {
	groupID := s.cluster.SlotForKey(channelID)
	return s.getChannelRuntimeMetaAuthoritative(ctx, groupID, channelID, channelType)
}

func (s *Store) ListChannelRuntimeMeta(ctx context.Context) ([]metadb.ChannelRuntimeMeta, error) {
	if s.cluster == nil {
		return s.db.ListChannelRuntimeMeta(ctx)
	}

	metas := make([]metadb.ChannelRuntimeMeta, 0, 16)
	for _, groupID := range s.cluster.GroupIDs() {
		groupMetas, err := s.listChannelRuntimeMetaAuthoritative(ctx, groupID)
		if err != nil {
			return nil, err
		}
		metas = append(metas, groupMetas...)
	}
	return metas, nil
}

func (s *Store) UpsertUser(ctx context.Context, u metadb.User) error {
	groupID := s.cluster.SlotForKey(u.UID)
	cmd := metafsm.EncodeUpsertUserCommand(u)
	return s.cluster.Propose(ctx, groupID, cmd)
}

// CreateUser returns ErrAlreadyExists when the authoritative slot already has
// the uid. Under concurrent duplicate creates, the replicated apply path
// treats the later create as a benign no-op to avoid failing the raft group.
func (s *Store) CreateUser(ctx context.Context, u metadb.User) error {
	groupID := s.cluster.SlotForKey(u.UID)
	if _, err := s.getUserAuthoritative(ctx, groupID, u.UID); err == nil {
		return metadb.ErrAlreadyExists
	} else if err != nil && !errors.Is(err, metadb.ErrNotFound) {
		return err
	}
	cmd := metafsm.EncodeCreateUserCommand(u)
	return s.cluster.Propose(ctx, groupID, cmd)
}

func (s *Store) GetUser(ctx context.Context, uid string) (metadb.User, error) {
	groupID := s.cluster.SlotForKey(uid)
	return s.getUserAuthoritative(ctx, groupID, uid)
}

func (s *Store) UpsertDevice(ctx context.Context, d metadb.Device) error {
	groupID := s.cluster.SlotForKey(d.UID)
	cmd := metafsm.EncodeUpsertDeviceCommand(d)
	return s.cluster.Propose(ctx, groupID, cmd)
}

func (s *Store) GetDevice(ctx context.Context, uid string, deviceFlag int64) (metadb.Device, error) {
	groupID := s.cluster.SlotForKey(uid)
	return s.getDeviceAuthoritative(ctx, groupID, uid, deviceFlag)
}
