package channellog

import (
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/replication/isr"
)

func TestStoreEpochHistoryAppendAndLoad(t *testing.T) {
	store := openTestStore(t, ChannelKey{ChannelID: "c1", ChannelType: 1})
	if err := store.appendEpochPoint(isr.EpochPoint{Epoch: 7, StartOffset: 10}); err != nil {
		t.Fatalf("appendEpochPoint() error = %v", err)
	}
	points, err := store.loadEpochHistory()
	if err != nil {
		t.Fatalf("loadEpochHistory() error = %v", err)
	}
	if len(points) != 1 || points[0].Epoch != 7 || points[0].StartOffset != 10 {
		t.Fatalf("points = %+v", points)
	}
}
