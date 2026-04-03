package channellog

import (
	"context"
	"sync"

	"github.com/WuKongIM/WuKongIM/pkg/replication/isr"
)

type isrLogStoreBridge struct {
	store *Store
	mu    sync.Mutex
	leo   uint64
}

type isrCheckpointStoreBridge struct {
	store *Store
}

type isrEpochHistoryStoreBridge struct {
	store *Store
}

type isrSnapshotApplierBridge struct {
	store *Store
}

func (s *Store) isrLogStore() isr.LogStore {
	bridge := &isrLogStoreBridge{store: s}
	if leo, err := s.leo(); err == nil {
		bridge.leo = leo
	}
	return bridge
}

func (s *Store) isrCheckpointStore() isr.CheckpointStore {
	return &isrCheckpointStoreBridge{store: s}
}

func (s *Store) isrEpochHistoryStore() isr.EpochHistoryStore {
	return &isrEpochHistoryStoreBridge{store: s}
}

func (s *Store) isrSnapshotApplier() isr.SnapshotApplier {
	return &isrSnapshotApplierBridge{store: s}
}

func (b *isrLogStoreBridge) LEO() uint64 {
	// isr.LogStore exposes LEO without an error return. Keep using the last
	// successful value rather than crashing the process if the store becomes
	// unavailable later.
	if leo, err := b.store.leo(); err == nil {
		b.setLEO(leo)
		return leo
	}
	return b.cachedLEO()
}

func (b *isrLogStoreBridge) Append(records []isr.Record) (uint64, error) {
	payloads := make([][]byte, 0, len(records))
	for _, record := range records {
		payloads = append(payloads, record.Payload)
	}
	base, err := b.store.appendPayloads(payloads)
	if err != nil {
		return 0, err
	}
	b.setLEO(base + uint64(len(records)))
	return base, nil
}

func (b *isrLogStoreBridge) Read(from uint64, maxBytes int) ([]isr.Record, error) {
	if err := b.store.validate(); err != nil {
		return nil, err
	}
	if maxBytes <= 0 {
		return nil, nil
	}

	out := make([]isr.Record, 0, logScanInitialCapacity)
	_, err := b.store.db.scanGroupOffsets(b.store.groupKey, from, maxLogScanLimit(), maxBytes, func(_ uint64, payload []byte) error {
		out = append(out, isr.Record{
			Payload:   payload,
			SizeBytes: len(payload),
		})
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (b *isrLogStoreBridge) Truncate(to uint64) error {
	if err := b.store.truncateOffsets(to); err != nil {
		return err
	}
	if leo, err := b.store.leo(); err == nil {
		b.setLEO(leo)
	} else {
		b.setLEO(to)
	}
	return nil
}

func (b *isrLogStoreBridge) Sync() error {
	return b.store.sync()
}

func (b *isrCheckpointStoreBridge) Load() (isr.Checkpoint, error) {
	return b.store.loadCheckpoint()
}

func (b *isrCheckpointStoreBridge) Store(checkpoint isr.Checkpoint) error {
	return b.store.storeCheckpoint(checkpoint)
}

func (b *isrEpochHistoryStoreBridge) Load() ([]isr.EpochPoint, error) {
	return b.store.loadEpochHistory()
}

func (b *isrEpochHistoryStoreBridge) Append(point isr.EpochPoint) error {
	points, err := b.store.loadEpochHistoryOrEmpty()
	if err != nil {
		return err
	}
	if len(points) > 0 {
		last := points[len(points)-1]
		switch {
		case point.Epoch > last.Epoch:
		case point.Epoch == last.Epoch && point.StartOffset == last.StartOffset:
			return nil
		default:
			return isr.ErrCorruptState
		}
	}
	return b.store.appendEpochPoint(point)
}

func (b *isrSnapshotApplierBridge) InstallSnapshot(_ context.Context, snap isr.Snapshot) error {
	return b.store.storeSnapshotPayload(snap.Payload)
}

var _ isr.LogStore = (*isrLogStoreBridge)(nil)
var _ isr.CheckpointStore = (*isrCheckpointStoreBridge)(nil)
var _ isr.EpochHistoryStore = (*isrEpochHistoryStoreBridge)(nil)
var _ isr.SnapshotApplier = (*isrSnapshotApplierBridge)(nil)

func (b *isrLogStoreBridge) cachedLEO() uint64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.leo
}

func (b *isrLogStoreBridge) setLEO(leo uint64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.leo = leo
}
