package channellog

import (
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/replication/isr"
)

func NewReplica(store *Store, localNode isr.NodeID, now func() time.Time) (isr.Replica, error) {
	if now == nil {
		now = time.Now
	}
	return isr.NewReplica(isr.ReplicaConfig{
		LocalNode:         localNode,
		LogStore:          store.isrLogStore(),
		CheckpointStore:   store.isrCheckpointStore(),
		EpochHistoryStore: store.isrEpochHistoryStore(),
		SnapshotApplier:   store.isrSnapshotApplier(),
		Now:               now,
	})
}
