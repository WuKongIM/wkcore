package store

import (
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/channel"
)

func TestChannelStoreCheckpointRoundTrip(t *testing.T) {
	st := newTestChannelStore(t)
	want := channel.Checkpoint{Epoch: 3, LogStartOffset: 4, HW: 9}

	if err := st.StoreCheckpoint(want); err != nil {
		t.Fatalf("StoreCheckpoint() error = %v", err)
	}
	got, err := st.LoadCheckpoint()
	if err != nil {
		t.Fatalf("LoadCheckpoint() error = %v", err)
	}
	if got != want {
		t.Fatalf("checkpoint = %+v, want %+v", got, want)
	}
}
