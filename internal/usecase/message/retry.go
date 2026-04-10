package message

import (
	"context"
	"errors"

	channellog "github.com/WuKongIM/WuKongIM/pkg/channel/log"
)

func sendWithMetaRefreshRetry(ctx context.Context, cluster ChannelCluster, refresher MetaRefresher, req channellog.AppendRequest) (channellog.AppendResult, error) {
	result, err := cluster.Append(ctx, req)
	if !shouldRefreshAndRetry(err) || refresher == nil {
		return result, err
	}

	meta, err := refresher.RefreshChannelMeta(ctx, channellog.ChannelKey{
		ChannelID:   req.ChannelID,
		ChannelType: req.ChannelType,
	})
	if err != nil {
		return channellog.AppendResult{}, err
	}
	if err := cluster.ApplyMeta(meta); err != nil {
		return channellog.AppendResult{}, err
	}

	req.ExpectedChannelEpoch = meta.ChannelEpoch
	req.ExpectedLeaderEpoch = meta.LeaderEpoch
	return cluster.Append(ctx, req)
}

func shouldRefreshAndRetry(err error) bool {
	return errors.Is(err, channellog.ErrStaleMeta) || errors.Is(err, channellog.ErrNotLeader)
}
