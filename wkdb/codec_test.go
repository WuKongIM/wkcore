package wkdb

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"hash/crc32"
	"path/filepath"
	"testing"
)

func openTestDB(t *testing.T) *DB {
	t.Helper()

	db, err := Open(filepath.Join(t.TempDir(), "db"))
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Fatalf("close db: %v", err)
		}
	})
	return db
}

func TestForSlotReturnsShardStore(t *testing.T) {
	db := openTestDB(t)

	shard := db.ForSlot(7)
	if shard == nil || shard.db != db || shard.slot != 7 {
		t.Fatalf("ForSlot(7) = %#v", shard)
	}
}

func TestStateAndIndexPrefixesIncludeSlotAndSortStably(t *testing.T) {
	aState := encodeStatePrefix(1, TableIDUser)
	bState := encodeStatePrefix(2, TableIDUser)
	if bytes.Compare(aState, bState) >= 0 {
		t.Fatalf("state prefixes did not sort by slot: %x >= %x", aState, bState)
	}

	aIndex := encodeIndexPrefix(1, TableIDChannel, channelIndexIDChannelID)
	bIndex := encodeIndexPrefix(2, TableIDChannel, channelIndexIDChannelID)
	if bytes.Compare(aIndex, bIndex) >= 0 {
		t.Fatalf("index prefixes did not sort by slot: %x >= %x", aIndex, bIndex)
	}

	meta := encodeMetaPrefix(1)
	if len(meta) == 0 {
		t.Fatal("encodeMetaPrefix(1) returned empty prefix")
	}
}

func TestUserPrimaryKeyEncodingMatchesDoc(t *testing.T) {
	got := encodeUserPrimaryKey("u1001", 0)
	want := mustHex(t, "01 00 00 00 01 00 01 00 05 75 31 30 30 31 00")
	if !bytes.Equal(got, want) {
		t.Fatalf("unexpected key:\n got: %x\nwant: %x", got, want)
	}
}

func TestChannelPrimaryKeyEncodingMatchesDoc(t *testing.T) {
	got := encodeChannelPrimaryKey("group-001", 1, 0)
	want := mustHex(t, "01 00 00 00 02 00 01 00 09 67 72 6f 75 70 2d 30 30 31 80 00 00 00 00 00 00 01 00")
	if !bytes.Equal(got, want) {
		t.Fatalf("unexpected key:\n got: %x\nwant: %x", got, want)
	}
}

func TestChannelIndexKeyEncodingMatchesDoc(t *testing.T) {
	got := encodeChannelIDIndexKey("group-001", 1)
	want := mustHex(t, "02 00 00 00 02 00 02 00 09 67 72 6f 75 70 2d 30 30 31 80 00 00 00 00 00 00 01")
	if !bytes.Equal(got, want) {
		t.Fatalf("unexpected key:\n got: %x\nwant: %x", got, want)
	}
}

func TestUserValueEncodingMatchesDoc(t *testing.T) {
	key := encodeUserPrimaryKey("u1001", 0)
	got := encodeUserFamilyValue("tk_abc", 1, 2, key)
	want := mustHex(t, "42 b6 f5 91 0a 26 06 74 6b 5f 61 62 63 13 02 13 04")
	if !bytes.Equal(got, want) {
		t.Fatalf("unexpected value:\n got: %x\nwant: %x", got, want)
	}
}

func TestChannelValueEncodingMatchesDoc(t *testing.T) {
	key := encodeChannelPrimaryKey("group-001", 1, 0)
	got := encodeChannelFamilyValue(0, key)
	want := mustHex(t, "6f 27 0b 83 0a 33 00")
	if !bytes.Equal(got, want) {
		t.Fatalf("unexpected value:\n got: %x\nwant: %x", got, want)
	}
}

func TestDecodeWrappedValueDetectsChecksumMismatch(t *testing.T) {
	key := encodeUserPrimaryKey("u1001", 0)
	value := encodeUserFamilyValue("tk_abc", 1, 2, key)
	value[len(value)-1] ^= 0xff

	_, _, err := decodeWrappedValue(key, value)
	if !errors.Is(err, ErrChecksumMismatch) {
		t.Fatalf("expected ErrChecksumMismatch, got %v", err)
	}
}

func TestDecodeUserFamilyValueRejectsUnexpectedTag(t *testing.T) {
	key := encodeUserPrimaryKey("u1001", 0)
	value := encodeUserFamilyValue("tk_abc", 1, 2, key)
	value[4] = 0x09
	binary.BigEndian.PutUint32(value[:4], crc32.ChecksumIEEE(append(append([]byte{}, key...), value[4:]...)))

	_, _, _, err := decodeUserFamilyValue(key, value)
	if !errors.Is(err, ErrCorruptValue) {
		t.Fatalf("expected ErrCorruptValue, got %v", err)
	}
}

func TestDecodeUserFamilyValueRejectsMissingColumns(t *testing.T) {
	key := encodeUserPrimaryKey("u1001", 0)
	payload := appendBytesValue(nil, userColumnIDToken, 0, "tk_only")
	value := wrapFamilyValue(key, payload)

	_, _, _, err := decodeUserFamilyValue(key, value)
	if !errors.Is(err, ErrCorruptValue) {
		t.Fatalf("expected ErrCorruptValue, got %v", err)
	}
}

func mustHex(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(bytes.NewBufferString(s).String())
	if err == nil {
		return b
	}
	compact := bytes.ReplaceAll([]byte(s), []byte(" "), nil)
	b, err = hex.DecodeString(string(compact))
	if err != nil {
		t.Fatalf("decode hex: %v", err)
	}
	return b
}
