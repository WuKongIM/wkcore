package wkstore

import (
	"context"

	"github.com/WuKongIM/WuKongIM/pkg/wkcluster"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
)

// Store provides business-level distributed storage APIs
// built on top of wkcluster's generic Propose mechanism.
type Store struct {
	cluster *wkcluster.Cluster
	db      *wkdb.DB
}

// New creates a Store.
func New(cluster *wkcluster.Cluster, db *wkdb.DB) *Store {
	return &Store{cluster: cluster, db: db}
}

func (s *Store) CreateChannel(ctx context.Context, channelID string, channelType int64) error {
	groupID := s.cluster.SlotForKey(channelID)
	cmd := EncodeUpsertChannelCommand(wkdb.Channel{
		ChannelID:   channelID,
		ChannelType: channelType,
	})
	return s.cluster.Propose(ctx, groupID, cmd)
}

func (s *Store) UpdateChannel(ctx context.Context, channelID string, channelType int64, ban int64) error {
	groupID := s.cluster.SlotForKey(channelID)
	cmd := EncodeUpsertChannelCommand(wkdb.Channel{
		ChannelID:   channelID,
		ChannelType: channelType,
		Ban:         ban,
	})
	return s.cluster.Propose(ctx, groupID, cmd)
}

func (s *Store) DeleteChannel(ctx context.Context, channelID string, channelType int64) error {
	groupID := s.cluster.SlotForKey(channelID)
	cmd := EncodeDeleteChannelCommand(channelID, channelType)
	return s.cluster.Propose(ctx, groupID, cmd)
}

func (s *Store) GetChannel(ctx context.Context, channelID string, channelType int64) (wkdb.Channel, error) {
	groupID := s.cluster.SlotForKey(channelID)
	return s.db.ForSlot(uint64(groupID)).GetChannel(ctx, channelID, channelType)
}

func (s *Store) UpsertUser(ctx context.Context, u wkdb.User) error {
	groupID := s.cluster.SlotForKey(u.UID)
	cmd := EncodeUpsertUserCommand(u)
	return s.cluster.Propose(ctx, groupID, cmd)
}

func (s *Store) GetUser(ctx context.Context, uid string) (wkdb.User, error) {
	groupID := s.cluster.SlotForKey(uid)
	return s.db.ForSlot(uint64(groupID)).GetUser(ctx, uid)
}
