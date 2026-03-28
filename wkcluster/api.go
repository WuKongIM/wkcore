package wkcluster

import (
	"context"
	"errors"
	"time"

	"github.com/WuKongIM/wraft/multiraft"
	"github.com/WuKongIM/wraft/wkdb"
	"github.com/WuKongIM/wraft/wkfsm"
	"github.com/WuKongIM/wraft/wktransport"
)

func (c *Cluster) CreateChannel(ctx context.Context, channelID string, channelType int64) error {
	groupID := c.router.SlotForChannel(channelID)
	cmd := wkfsm.EncodeUpsertChannelCommand(wkdb.Channel{
		ChannelID:   channelID,
		ChannelType: channelType,
	})
	return c.proposeOrForward(ctx, groupID, cmd)
}

func (c *Cluster) UpdateChannel(ctx context.Context, channelID string, channelType int64, ban int64) error {
	groupID := c.router.SlotForChannel(channelID)
	cmd := wkfsm.EncodeUpsertChannelCommand(wkdb.Channel{
		ChannelID:   channelID,
		ChannelType: channelType,
		Ban:         ban,
	})
	return c.proposeOrForward(ctx, groupID, cmd)
}

func (c *Cluster) DeleteChannel(ctx context.Context, channelID string, channelType int64) error {
	groupID := c.router.SlotForChannel(channelID)
	cmd := wkfsm.EncodeDeleteChannelCommand(channelID, channelType)
	return c.proposeOrForward(ctx, groupID, cmd)
}

func (c *Cluster) GetChannel(ctx context.Context, channelID string, channelType int64) (wkdb.Channel, error) {
	groupID := c.router.SlotForChannel(channelID)
	store := c.db.ForSlot(uint64(groupID))
	return store.GetChannel(ctx, channelID, channelType)
}

func (c *Cluster) proposeOrForward(ctx context.Context, groupID multiraft.GroupID, cmd []byte) error {
	if c.stopped.Load() {
		return wktransport.ErrStopped
	}
	const maxRetries = 3
	for attempt := 0; attempt < maxRetries; attempt++ {
		if attempt > 0 {
			// Exponential backoff: 50ms, 100ms
			backoff := time.Duration(attempt) * 50 * time.Millisecond
			select {
			case <-time.After(backoff):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		leaderID, err := c.router.LeaderOf(groupID)
		if err != nil {
			return err
		}
		if c.router.IsLocal(leaderID) {
			future, err := c.runtime.Propose(ctx, groupID, cmd)
			if err != nil {
				return err
			}
			_, err = future.Wait(ctx)
			return err
		}
		_, err = c.forwarder.Forward(ctx, leaderID, groupID, cmd)
		if errors.Is(err, ErrNotLeader) {
			continue
		}
		return err
	}
	return ErrLeaderNotStable
}
