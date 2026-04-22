package app

import (
	"context"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/channel"
	channelhandler "github.com/WuKongIM/WuKongIM/pkg/channel/handler"
	"github.com/WuKongIM/WuKongIM/pkg/slot/multiraft"
)

const slotLeaderRefreshAttempt = time.Second

// scheduleSlotLeaderRefresh refreshes active local channel metas in the slot
// after the authoritative slot leader changes.
func (s *channelMetaSync) scheduleSlotLeaderRefresh(slotID multiraft.SlotID) {
	if s == nil {
		return
	}
	keys := s.snapshotAppliedLocalKeysForSlot(slotID)
	if len(keys) == 0 {
		return
	}

	s.mu.Lock()
	if s.pendingSlots == nil {
		s.pendingSlots = make(map[multiraft.SlotID]struct{})
	}
	if _, ok := s.pendingSlots[slotID]; ok {
		s.mu.Unlock()
		return
	}
	s.pendingSlots[slotID] = struct{}{}
	s.mu.Unlock()

	go func() {
		defer s.clearPendingSlotRefresh(slotID)
		for _, key := range keys {
			ctx, cancel := context.WithTimeout(context.Background(), slotLeaderRefreshAttempt)
			_, _ = s.refreshAuthoritativeByKey(ctx, key)
			cancel()
		}
	}()
}

func (s *channelMetaSync) clearPendingSlotRefresh(slotID multiraft.SlotID) {
	if s == nil {
		return
	}
	s.mu.Lock()
	if s.pendingSlots != nil {
		delete(s.pendingSlots, slotID)
	}
	s.mu.Unlock()
}

func (s *channelMetaSync) snapshotAppliedLocalKeysForSlot(slotID multiraft.SlotID) []channel.ChannelKey {
	if s == nil || s.bootstrap == nil || s.bootstrap.cluster == nil {
		return nil
	}
	applied := s.snapshotAppliedLocal()
	if len(applied) == 0 {
		return nil
	}
	keys := make([]channel.ChannelKey, 0, len(applied))
	for key := range applied {
		id, err := channelhandler.ParseChannelKey(key)
		if err != nil {
			continue
		}
		if s.bootstrap.cluster.SlotForKey(id.ID) != slotID {
			continue
		}
		keys = append(keys, key)
	}
	return keys
}

func (s *channelMetaSync) refreshAuthoritativeByKey(ctx context.Context, key channel.ChannelKey) (channel.Meta, error) {
	if s == nil {
		return channel.Meta{}, channel.ErrInvalidConfig
	}
	s.observeHashSlotTableVersion()
	id, err := channelhandler.ParseChannelKey(key)
	if err != nil {
		return channel.Meta{}, err
	}
	return s.cache.runSingleflight(key, func() (channel.Meta, error) {
		meta, err := s.source.GetChannelRuntimeMeta(ctx, id.ID, int64(id.Type))
		if err != nil {
			s.cache.storeNegative(key, err, s.now())
			return channel.Meta{}, err
		}
		meta, err = s.reconcileChannelRuntimeMeta(ctx, meta)
		if err != nil {
			return channel.Meta{}, err
		}
		applied, err := s.applyAuthoritativeMeta(meta)
		if err != nil {
			return channel.Meta{}, err
		}
		s.cache.storePositive(key, applied, s.now())
		return applied, nil
	})
}
