package channellog

import "testing"

func TestStoreAppendAndReadByOffset(t *testing.T) {
	store := openTestStore(t, ChannelKey{ChannelID: "c1", ChannelType: 1})

	base, err := store.appendPayloads([][]byte{[]byte("one"), []byte("two")})
	if err != nil {
		t.Fatalf("appendPayloads() error = %v", err)
	}
	if base != 0 {
		t.Fatalf("base = %d, want 0", base)
	}

	records, err := store.readOffsets(0, 2, 1024)
	if err != nil {
		t.Fatalf("readOffsets() error = %v", err)
	}
	if len(records) != 2 {
		t.Fatalf("len(records) = %d, want 2", len(records))
	}
	if records[1].Offset != 1 {
		t.Fatalf("records[1].Offset = %d, want 1", records[1].Offset)
	}
}

func TestDBReadScopesByGroupKeyAndBudget(t *testing.T) {
	db := openTestDB(t)
	first := db.ForChannel(ChannelKey{ChannelID: "c1", ChannelType: 1})
	second := db.ForChannel(ChannelKey{ChannelID: "c2", ChannelType: 1})
	mustAppendPayloads(t, first, []string{"one", "two"})
	mustAppendPayloads(t, second, []string{"zzz"})

	records, err := db.Read(channelGroupKey(first.key), 0, 10, len("one")+16)
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("len(records) = %d, want 1", len(records))
	}
}
