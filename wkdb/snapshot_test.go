package wkdb

import (
	"bytes"
	"context"
	"errors"
	"testing"
)

func TestDeleteSlotDataRemovesOnlyTargetSlot(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	left := db.ForSlot(1)
	right := db.ForSlot(2)

	if err := left.CreateUser(ctx, User{UID: "u1", Token: "left"}); err != nil {
		t.Fatalf("left create user: %v", err)
	}
	if err := right.CreateUser(ctx, User{UID: "u1", Token: "right"}); err != nil {
		t.Fatalf("right create user: %v", err)
	}
	if err := left.CreateChannel(ctx, Channel{ChannelID: "c1", ChannelType: 1, Ban: 1}); err != nil {
		t.Fatalf("left create channel: %v", err)
	}
	if err := right.CreateChannel(ctx, Channel{ChannelID: "c1", ChannelType: 1, Ban: 2}); err != nil {
		t.Fatalf("right create channel: %v", err)
	}

	if err := db.DeleteSlotData(ctx, 1); err != nil {
		t.Fatalf("DeleteSlotData(1): %v", err)
	}

	if _, err := left.GetUser(ctx, "u1"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("left GetUser() err = %v, want ErrNotFound", err)
	}
	if _, err := left.GetChannel(ctx, "c1", 1); !errors.Is(err, ErrNotFound) {
		t.Fatalf("left GetChannel() err = %v, want ErrNotFound", err)
	}

	user, err := right.GetUser(ctx, "u1")
	if err != nil {
		t.Fatalf("right GetUser(): %v", err)
	}
	if user.Token != "right" {
		t.Fatalf("right user token = %q", user.Token)
	}

	channel, err := right.GetChannel(ctx, "c1", 1)
	if err != nil {
		t.Fatalf("right GetChannel(): %v", err)
	}
	if channel.Ban != 2 {
		t.Fatalf("right channel ban = %d", channel.Ban)
	}
}

func TestSlotAllDataSpansAreOrderedAndDisjoint(t *testing.T) {
	spans := slotAllDataSpans(7)
	if len(spans) != 3 {
		t.Fatalf("slotAllDataSpans(7) len = %d", len(spans))
	}

	for i, span := range spans {
		if len(span.Start) == 0 || len(span.End) == 0 {
			t.Fatalf("span %d is empty: %#v", i, span)
		}
		if bytes.Compare(span.Start, span.End) >= 0 {
			t.Fatalf("span %d start >= end: %#v", i, span)
		}
		if i > 0 && bytes.Compare(spans[i-1].End, span.Start) > 0 {
			t.Fatalf("spans overlap: %#v then %#v", spans[i-1], span)
		}
	}

	assertKeyInSpan(t, encodeUserPrimaryKey(7, "u1", userPrimaryFamilyID), spans[0])
	assertKeyInSpan(t, encodeChannelPrimaryKey(7, "c1", 1, channelPrimaryFamilyID), spans[0])
	assertKeyInSpan(t, encodeChannelIDIndexKey(7, "c1", 1), spans[1])
	assertKeyInSpan(t, encodeMetaPrefix(7), spans[2])
}

func assertKeyInSpan(t *testing.T, key []byte, span Span) {
	t.Helper()

	if bytes.Compare(key, span.Start) < 0 || bytes.Compare(key, span.End) >= 0 {
		t.Fatalf("key %x not in span [%x, %x)", key, span.Start, span.End)
	}
}
