package channellog

import "testing"

func TestStoreReplaceSnapshotPayload(t *testing.T) {
	store := openTestStore(t, ChannelKey{ChannelID: "c1", ChannelType: 1})
	if err := store.storeSnapshotPayload([]byte("first")); err != nil {
		t.Fatalf("storeSnapshotPayload(first) error = %v", err)
	}
	if err := store.storeSnapshotPayload([]byte("second")); err != nil {
		t.Fatalf("storeSnapshotPayload(second) error = %v", err)
	}
	payload, err := store.loadSnapshotPayload()
	if err != nil {
		t.Fatalf("loadSnapshotPayload() error = %v", err)
	}
	if string(payload) != "second" {
		t.Fatalf("payload = %q, want %q", payload, "second")
	}
}
