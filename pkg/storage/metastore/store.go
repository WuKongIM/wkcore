package metastore

import (
	"context"

	"github.com/WuKongIM/WuKongIM/pkg/controller/wkcluster"
	"github.com/WuKongIM/WuKongIM/pkg/storage/metadb"
	"github.com/WuKongIM/WuKongIM/pkg/storage/metafsm"
)

// Store provides business-level distributed storage APIs
// built on top of wkcluster's generic Propose mechanism.
type Store struct {
	cluster *wkcluster.Cluster
	db      *metadb.DB
}

// New creates a Store.
func New(cluster *wkcluster.Cluster, db *metadb.DB) *Store {
	return &Store{cluster: cluster, db: db}
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

func (s *Store) UpsertUser(ctx context.Context, u metadb.User) error {
	groupID := s.cluster.SlotForKey(u.UID)
	cmd := metafsm.EncodeUpsertUserCommand(u)
	return s.cluster.Propose(ctx, groupID, cmd)
}

func (s *Store) GetUser(ctx context.Context, uid string) (metadb.User, error) {
	groupID := s.cluster.SlotForKey(uid)
	return s.db.ForSlot(uint64(groupID)).GetUser(ctx, uid)
}
