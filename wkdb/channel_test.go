package wkdb

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestChannelCRUD(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	db, err := Open(filepath.Join(dir, "db"))
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Fatalf("close db: %v", err)
		}
	})

	ch1 := Channel{ChannelID: "group-001", ChannelType: 1, Ban: 0}
	ch2 := Channel{ChannelID: "group-001", ChannelType: 2, Ban: 1}
	ch3 := Channel{ChannelID: "group-002", ChannelType: 1, Ban: 0}

	if err := db.CreateChannel(ctx, ch1); err != nil {
		t.Fatalf("create ch1: %v", err)
	}
	if err := db.CreateChannel(ctx, ch2); err != nil {
		t.Fatalf("create ch2: %v", err)
	}
	if err := db.CreateChannel(ctx, ch3); err != nil {
		t.Fatalf("create ch3: %v", err)
	}

	if err := db.CreateChannel(ctx, ch1); !errors.Is(err, ErrAlreadyExists) {
		t.Fatalf("expected ErrAlreadyExists, got %v", err)
	}

	got, err := db.GetChannel(ctx, ch1.ChannelID, ch1.ChannelType)
	if err != nil {
		t.Fatalf("get channel: %v", err)
	}
	if !reflect.DeepEqual(got, ch1) {
		t.Fatalf("unexpected channel:\n got: %#v\nwant: %#v", got, ch1)
	}

	updated := Channel{ChannelID: "group-001", ChannelType: 1, Ban: 9}
	if err := db.UpdateChannel(ctx, updated); err != nil {
		t.Fatalf("update channel: %v", err)
	}

	got, err = db.GetChannel(ctx, updated.ChannelID, updated.ChannelType)
	if err != nil {
		t.Fatalf("get updated channel: %v", err)
	}
	if !reflect.DeepEqual(got, updated) {
		t.Fatalf("unexpected updated channel:\n got: %#v\nwant: %#v", got, updated)
	}

	list, err := db.ListChannelsByChannelID(ctx, "group-001")
	if err != nil {
		t.Fatalf("list by channel id: %v", err)
	}
	wantList := []Channel{updated, ch2}
	if !reflect.DeepEqual(list, wantList) {
		t.Fatalf("unexpected channel list:\n got: %#v\nwant: %#v", list, wantList)
	}

	if err := db.DeleteChannel(ctx, updated.ChannelID, updated.ChannelType); err != nil {
		t.Fatalf("delete updated channel: %v", err)
	}

	_, err = db.GetChannel(ctx, updated.ChannelID, updated.ChannelType)
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound after delete, got %v", err)
	}

	list, err = db.ListChannelsByChannelID(ctx, "group-001")
	if err != nil {
		t.Fatalf("list after one delete: %v", err)
	}
	wantList = []Channel{ch2}
	if !reflect.DeepEqual(list, wantList) {
		t.Fatalf("unexpected channel list after one delete:\n got: %#v\nwant: %#v", list, wantList)
	}

	if err := db.DeleteChannel(ctx, ch2.ChannelID, ch2.ChannelType); err != nil {
		t.Fatalf("delete ch2: %v", err)
	}

	list, err = db.ListChannelsByChannelID(ctx, "group-001")
	if err != nil {
		t.Fatalf("list after deleting indexed rows: %v", err)
	}
	if len(list) != 0 {
		t.Fatalf("expected empty list, got %#v", list)
	}
}

func TestListChannelsByChannelIDHonorsCanceledContext(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(filepath.Join(dir, "db"))
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Fatalf("close db: %v", err)
		}
	})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err = db.ListChannelsByChannelID(ctx, "group-001")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

func TestCreateChannelRejectsOverlongChannelID(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	db, err := Open(filepath.Join(dir, "db"))
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Fatalf("close db: %v", err)
		}
	})

	ch := Channel{ChannelID: strings.Repeat("c", maxKeyStringLen+1), ChannelType: 1}
	err = db.CreateChannel(ctx, ch)
	if !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("expected ErrInvalidArgument, got %v", err)
	}
}
