package log

import (
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/channel/isr"
)

func TestStoreCheckpointRoundTrip(t *testing.T) {
	store := openTestStore(t, ChannelKey{ChannelID: "c1", ChannelType: 1})
	want := isr.Checkpoint{Epoch: 3, LogStartOffset: 4, HW: 9}

	if err := store.storeCheckpoint(want); err != nil {
		t.Fatalf("storeCheckpoint() error = %v", err)
	}
	got, err := store.loadCheckpoint()
	if err != nil {
		t.Fatalf("loadCheckpoint() error = %v", err)
	}
	if got != want {
		t.Fatalf("checkpoint = %+v, want %+v", got, want)
	}
}
