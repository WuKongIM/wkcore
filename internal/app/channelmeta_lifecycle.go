package app

import (
	"context"
	"fmt"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/channel"
	metadb "github.com/WuKongIM/WuKongIM/pkg/slot/meta"
)

func (b *channelMetaBootstrapper) ReconcileChannelRuntimeMeta(ctx context.Context, meta metadb.ChannelRuntimeMeta, renewBefore time.Duration) (metadb.ChannelRuntimeMeta, bool, error) {
	if b == nil || b.store == nil || b.cluster == nil {
		return meta, false, nil
	}
	if meta.Status != uint8(channel.StatusActive) {
		return meta, false, nil
	}

	candidate, changed, err := b.reconciledChannelRuntimeMeta(meta, renewBefore)
	if err != nil || !changed {
		return meta, false, err
	}
	if err := b.store.UpsertChannelRuntimeMeta(ctx, candidate); err != nil {
		return metadb.ChannelRuntimeMeta{}, false, fmt.Errorf("channelmeta lifecycle: upsert runtime metadata: %w", err)
	}

	authoritative, err := b.store.GetChannelRuntimeMeta(ctx, meta.ChannelID, meta.ChannelType)
	if err != nil {
		return metadb.ChannelRuntimeMeta{}, false, fmt.Errorf("channelmeta lifecycle: reread runtime metadata: %w", err)
	}
	return authoritative, true, nil
}

func (b *channelMetaBootstrapper) reconciledChannelRuntimeMeta(meta metadb.ChannelRuntimeMeta, renewBefore time.Duration) (metadb.ChannelRuntimeMeta, bool, error) {
	slotID := b.cluster.SlotForKey(meta.ChannelID)
	replicas := projectBootstrapReplicaIDs(b.cluster.PeersForSlot(slotID))
	if len(replicas) == 0 {
		return metadb.ChannelRuntimeMeta{}, false, fmt.Errorf("channelmeta lifecycle: slot peers empty for slot %d", slotID)
	}

	leader, err := b.cluster.LeaderOf(slotID)
	if err != nil {
		return metadb.ChannelRuntimeMeta{}, false, err
	}

	candidate := meta
	changed := false
	now := b.now().UTC()

	if !equalUint64Slices(meta.Replicas, replicas) {
		candidate.Replicas = append([]uint64(nil), replicas...)
		candidate.ISR = append([]uint64(nil), replicas...)
		candidate.ChannelEpoch = nextRuntimeEpoch(meta.ChannelEpoch)
		changed = true
	} else if !equalUint64Slices(meta.ISR, replicas) {
		candidate.ISR = append([]uint64(nil), replicas...)
		candidate.ChannelEpoch = nextRuntimeEpoch(meta.ChannelEpoch)
		changed = true
	}

	minISR := int64(clampBootstrapMinISR(b.defaultMinISR, len(replicas)))
	if candidate.MinISR != minISR {
		candidate.MinISR = minISR
		candidate.ChannelEpoch = nextRuntimeEpoch(candidate.ChannelEpoch)
		changed = true
	}

	if candidate.Leader != uint64(leader) {
		candidate.Leader = uint64(leader)
		candidate.LeaderEpoch = nextRuntimeEpoch(meta.LeaderEpoch)
		changed = true
	}

	leaseUntilMS := now.Add(channelMetaBootstrapLease).UnixMilli()
	if changed || runtimeMetaLeaseNeedsRenewal(meta.LeaseUntilMS, now, renewBefore) {
		candidate.LeaseUntilMS = leaseUntilMS
		changed = true
	}

	return candidate, changed, nil
}

func runtimeMetaLeaseNeedsRenewal(leaseUntilMS int64, now time.Time, renewBefore time.Duration) bool {
	if leaseUntilMS <= 0 {
		return true
	}
	leaseUntil := time.UnixMilli(leaseUntilMS).UTC()
	deadline := now.UTC().Add(renewBefore)
	return !leaseUntil.After(deadline)
}

func nextRuntimeEpoch(current uint64) uint64 {
	if current == 0 {
		return 1
	}
	return current + 1
}

func equalUint64Slices(left, right []uint64) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}
