package channellog

import (
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/replication/isr"
)

func NewReplica(store *Store, localNode isr.NodeID, now func() time.Time) (isr.Replica, error) {
	if now == nil {
		now = time.Now
	}
	state, err := store.db.StateStoreFactory().ForChannel(store.key)
	if err != nil {
		return nil, err
	}
	checkpoints, err := newCheckpointBridge(store.isrCheckpointStore(), store.db, store.key, state, store.groupKey)
	if err != nil {
		return nil, err
	}
	return isr.NewReplica(isr.ReplicaConfig{
		LocalNode:         localNode,
		LogStore:          store.isrLogStore(),
		CheckpointStore:   checkpoints,
		EpochHistoryStore: store.isrEpochHistoryStore(),
		SnapshotApplier:   newSnapshotBridge(store.isrSnapshotApplier(), state),
		Now:               now,
	})
}
