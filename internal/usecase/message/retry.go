package message

import (
	"context"
	"errors"

	"github.com/WuKongIM/WuKongIM/pkg/storage/channellog"
)

func sendWithMetaRefreshRetry(ctx context.Context, cluster ChannelCluster, refresher MetaRefresher, req channellog.SendRequest) (channellog.SendResult, error) {
	result, err := cluster.Send(ctx, req)
	if !shouldRefreshAndRetry(err) || refresher == nil {
		return result, err
	}

	meta, err := refresher.RefreshChannelMeta(ctx, channellog.ChannelKey{
		ChannelID:   req.ChannelID,
		ChannelType: req.ChannelType,
	})
	if err != nil {
		return channellog.SendResult{}, err
	}
	if err := cluster.ApplyMeta(meta); err != nil {
		return channellog.SendResult{}, err
	}

	req.ExpectedChannelEpoch = meta.ChannelEpoch
	req.ExpectedLeaderEpoch = meta.LeaderEpoch
	return cluster.Send(ctx, req)
}

func shouldRefreshAndRetry(err error) bool {
	return errors.Is(err, channellog.ErrStaleMeta) || errors.Is(err, channellog.ErrNotLeader)
}
