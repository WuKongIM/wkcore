package message

import (
	"context"
	"errors"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/channel"
	raftcluster "github.com/WuKongIM/WuKongIM/pkg/cluster"
	"github.com/WuKongIM/WuKongIM/pkg/wklog"
)

func sendWithMetaRefreshRetry(ctx context.Context, now func() time.Time, sendLogger, retryLogger wklog.Logger, cluster ChannelCluster, refresher MetaRefresher, req channel.AppendRequest) (channel.AppendResult, error) {
	result, err := cluster.Append(ctx, req)
	if err == nil {
		return result, nil
	}

	if !shouldRefreshAndRetry(err) || refresher == nil {
		fields := append([]wklog.Field{
			wklog.Event("message.send.persist.failed"),
		}, messageLogFields(req.ChannelID, req.Message.FromUID)...)
		fields = append(fields, wklog.Error(err))
		sendLogger.Error("persist committed message failed", fields...)
		return result, err
	}

	fields := append([]wklog.Field{
		wklog.Event("message.retry.refresh.triggered"),
	}, messageLogFields(req.ChannelID, req.Message.FromUID)...)
	fields = append(fields, wklog.Error(err))
	retryLogger.Warn("channel metadata stale, refreshing and retrying", fields...)

	meta, err := refresher.RefreshChannelMeta(ctx, req.ChannelID)
	if err != nil {
		fields := append([]wklog.Field{
			wklog.Event("message.retry.refresh.failed"),
		}, messageLogFields(req.ChannelID, req.Message.FromUID)...)
		fields = append(fields, wklog.Error(err))
		retryLogger.Error("refresh channel metadata failed", fields...)
		return channel.AppendResult{}, err
	}
	resolvedFields := append([]wklog.Field{
		wklog.Event("message.retry.refresh.resolved"),
		wklog.LeaderNodeID(uint64(meta.Leader)),
		wklog.Uint64("channelEpoch", meta.Epoch),
		wklog.Uint64("leaderEpoch", meta.LeaderEpoch),
		wklog.Int("replicaCount", len(meta.Replicas)),
		wklog.Int("isrCount", len(meta.ISR)),
		wklog.Int("minISR", meta.MinISR),
	}, messageLogFields(req.ChannelID, req.Message.FromUID)...)
	if now != nil && !meta.LeaseUntil.IsZero() {
		resolvedFields = append(resolvedFields, wklog.Duration("leaseRemaining", meta.LeaseUntil.Sub(now())))
	}
	retryLogger.Debug("resolved refreshed channel metadata", resolvedFields...)

	req.ExpectedChannelEpoch = meta.Epoch
	req.ExpectedLeaderEpoch = meta.LeaderEpoch
	result, err = cluster.Append(ctx, req)
	if err != nil {
		fields := append([]wklog.Field{
			wklog.Event("message.retry.persist.failed"),
		}, messageLogFields(req.ChannelID, req.Message.FromUID)...)
		fields = append(fields, wklog.Error(err))
		retryLogger.Error("persist committed message failed after metadata refresh", fields...)
	}
	return result, err
}

func shouldRefreshAndRetry(err error) bool {
	return errors.Is(err, channel.ErrStaleMeta) || errors.Is(err, channel.ErrNotLeader) || errors.Is(err, raftcluster.ErrRerouted)
}
