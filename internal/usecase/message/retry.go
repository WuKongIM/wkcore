package message

import (
	"context"
	"errors"

	"github.com/WuKongIM/WuKongIM/pkg/channel"
)

func sendWithMetaRefreshRetry(ctx context.Context, cluster ChannelCluster, refresher MetaRefresher, req channel.AppendRequest) (channel.AppendResult, error) {
	result, err := cluster.Append(ctx, req)
	if !shouldRefreshAndRetry(err) || refresher == nil {
		return result, err
	}

	meta, err := refresher.RefreshChannelMeta(ctx, req.ChannelID)
	if err != nil {
		return channel.AppendResult{}, err
	}
	if err := cluster.ApplyMeta(meta); err != nil {
		return channel.AppendResult{}, err
	}

	req.ExpectedChannelEpoch = meta.Epoch
	req.ExpectedLeaderEpoch = meta.LeaderEpoch
	return cluster.Append(ctx, req)
}

func shouldRefreshAndRetry(err error) bool {
	return errors.Is(err, channel.ErrStaleMeta) || errors.Is(err, channel.ErrNotLeader)
}
