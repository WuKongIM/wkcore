package metastore

import (
	"context"

	"github.com/WuKongIM/WuKongIM/pkg/cluster/raftcluster"
	"github.com/WuKongIM/WuKongIM/pkg/storage/metadb"
	"github.com/WuKongIM/WuKongIM/pkg/storage/metafsm"
)

// Store provides business-level distributed storage APIs
// built on top of raftcluster's generic Propose mechanism.
type Store struct {
	cluster *raftcluster.Cluster
	db      *metadb.DB
}

// New creates a Store.
func New(cluster *raftcluster.Cluster, db *metadb.DB) *Store {
	store := &Store{cluster: cluster, db: db}
	if cluster != nil && cluster.RPCMux() != nil {
		cluster.RPCMux().Handle(runtimeMetaRPCServiceID, store.handleRuntimeMetaRPC)
	}
	return store
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

func (s *Store) GetUser(ctx context.Context, uid string) (metadb.User, error) {
	groupID := s.cluster.SlotForKey(uid)
	return s.db.ForSlot(uint64(groupID)).GetUser(ctx, uid)
}
