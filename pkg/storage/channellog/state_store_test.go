package channellog

import "testing"

func TestStoreStateFactoryPersistsIdempotencyAndRestoresSnapshotAtOffset(t *testing.T) {
	db := openTestDB(t)
	key := ChannelKey{ChannelID: "u1", ChannelType: 1}

	store, err := db.StateStoreFactory().ForChannel(key)
	if err != nil {
		t.Fatalf("StateStoreFactory().ForChannel() error = %v", err)
	}

	firstKey := IdempotencyKey{
		ChannelID:   "u1",
		ChannelType: 1,
		FromUID:     "s1",
		ClientMsgNo: "m1",
	}
	firstEntry := IdempotencyEntry{
		MessageID:  9,
		MessageSeq: 3,
		Offset:     2,
	}
	if err := store.PutIdempotency(firstKey, firstEntry); err != nil {
		t.Fatalf("PutIdempotency(first) error = %v", err)
	}

	secondKey := IdempotencyKey{
		ChannelID:   "u1",
		ChannelType: 1,
		FromUID:     "s1",
		ClientMsgNo: "m2",
	}
	secondEntry := IdempotencyEntry{
		MessageID:  10,
		MessageSeq: 5,
		Offset:     4,
	}
	if err := store.PutIdempotency(secondKey, secondEntry); err != nil {
		t.Fatalf("PutIdempotency(second) error = %v", err)
	}

	fresh, err := db.StateStoreFactory().ForChannel(key)
	if err != nil {
		t.Fatalf("StateStoreFactory().ForChannel(fresh) error = %v", err)
	}
	got, ok, err := fresh.GetIdempotency(firstKey)
	if err != nil {
		t.Fatalf("GetIdempotency() error = %v", err)
	}
	if !ok {
		t.Fatal("expected idempotency entry")
	}
	if got != firstEntry {
		t.Fatalf("idempotency entry = %+v, want %+v", got, firstEntry)
	}

	snapshot, err := store.Snapshot(3)
	if err != nil {
		t.Fatalf("Snapshot() error = %v", err)
	}

	restoreDB := openTestDB(t)
	restored, err := restoreDB.StateStoreFactory().ForChannel(key)
	if err != nil {
		t.Fatalf("restore StateStoreFactory().ForChannel() error = %v", err)
	}
	if err := restored.Restore(snapshot); err != nil {
		t.Fatalf("Restore() error = %v", err)
	}

	got, ok, err = restored.GetIdempotency(firstKey)
	if err != nil {
		t.Fatalf("restored GetIdempotency(first) error = %v", err)
	}
	if !ok {
		t.Fatal("expected restored first entry")
	}
	if got != firstEntry {
		t.Fatalf("restored first entry = %+v, want %+v", got, firstEntry)
	}

	_, ok, err = restored.GetIdempotency(secondKey)
	if err != nil {
		t.Fatalf("restored GetIdempotency(second) error = %v", err)
	}
	if ok {
		t.Fatal("expected second entry to be excluded by snapshot offset")
	}
}

func TestStoreStateFactoryRoundTripsFullIdempotencyStateAtCurrentOffset(t *testing.T) {
	db := openTestDB(t)
	key := ChannelKey{ChannelID: "u1", ChannelType: 1}

	store, err := db.StateStoreFactory().ForChannel(key)
	if err != nil {
		t.Fatalf("StateStoreFactory().ForChannel() error = %v", err)
	}

	firstKey := IdempotencyKey{
		ChannelID:   "u1",
		ChannelType: 1,
		FromUID:     "s1",
		ClientMsgNo: "m1",
	}
	firstEntry := IdempotencyEntry{
		MessageID:  9,
		MessageSeq: 3,
		Offset:     2,
	}
	if err := store.PutIdempotency(firstKey, firstEntry); err != nil {
		t.Fatalf("PutIdempotency(first) error = %v", err)
	}

	secondKey := IdempotencyKey{
		ChannelID:   "u1",
		ChannelType: 1,
		FromUID:     "s1",
		ClientMsgNo: "m2",
	}
	secondEntry := IdempotencyEntry{
		MessageID:  10,
		MessageSeq: 5,
		Offset:     4,
	}
	if err := store.PutIdempotency(secondKey, secondEntry); err != nil {
		t.Fatalf("PutIdempotency(second) error = %v", err)
	}

	snapshot, err := store.Snapshot(5)
	if err != nil {
		t.Fatalf("Snapshot() error = %v", err)
	}

	restoreDB := openTestDB(t)
	restored, err := restoreDB.StateStoreFactory().ForChannel(key)
	if err != nil {
		t.Fatalf("restore StateStoreFactory().ForChannel() error = %v", err)
	}
	if err := restored.Restore(snapshot); err != nil {
		t.Fatalf("Restore() error = %v", err)
	}

	got, ok, err := restored.GetIdempotency(firstKey)
	if err != nil {
		t.Fatalf("restored GetIdempotency(first) error = %v", err)
	}
	if !ok {
		t.Fatal("expected restored first entry")
	}
	if got != firstEntry {
		t.Fatalf("restored first entry = %+v, want %+v", got, firstEntry)
	}

	got, ok, err = restored.GetIdempotency(secondKey)
	if err != nil {
		t.Fatalf("restored GetIdempotency(second) error = %v", err)
	}
	if !ok {
		t.Fatal("expected restored second entry")
	}
	if got != secondEntry {
		t.Fatalf("restored second entry = %+v, want %+v", got, secondEntry)
	}
}
