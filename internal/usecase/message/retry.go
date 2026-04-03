package message

import (
	"context"
	"errors"

	"github.com/WuKongIM/WuKongIM/pkg/channelcluster"
)

func sendWithMetaRefreshRetry(ctx context.Context, cluster ChannelCluster, refresher MetaRefresher, req channelcluster.SendRequest) (channelcluster.SendResult, error) {
	result, err := cluster.Send(ctx, req)
	if !shouldRefreshAndRetry(err) || refresher == nil {
		return result, err
	}

	meta, err := refresher.RefreshChannelMeta(ctx, channelcluster.ChannelKey{
		ChannelID:   req.ChannelID,
		ChannelType: req.ChannelType,
	})
	if err != nil {
		return channelcluster.SendResult{}, err
	}
	if err := cluster.ApplyMeta(meta); err != nil {
		return channelcluster.SendResult{}, err
	}

	req.ExpectedChannelEpoch = meta.ChannelEpoch
	req.ExpectedLeaderEpoch = meta.LeaderEpoch
	return cluster.Send(ctx, req)
}

func shouldRefreshAndRetry(err error) bool {
	return errors.Is(err, channelcluster.ErrStaleMeta) || errors.Is(err, channelcluster.ErrNotLeader)
}
