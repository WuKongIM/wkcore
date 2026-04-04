package channellog

import (
	"errors"
	"testing"
)

func TestApplyMetaRejectsConflictingReplay(t *testing.T) {
	c := newTestCluster()
	meta := testMeta("c1", 1, 3, 9)
	if err := c.ApplyMeta(meta); err != nil {
		t.Fatalf("ApplyMeta() error = %v", err)
	}

	err := c.ApplyMeta(conflictingReplay(meta))
	if !errors.Is(err, ErrConflictingMeta) {
		t.Fatalf("expected ErrConflictingMeta, got %v", err)
	}
}

func TestStatusReturnsErrStaleMetaWhenCacheMisses(t *testing.T) {
	c := newTestCluster()

	_, err := c.Status(ChannelKey{ChannelID: "missing", ChannelType: 1})
	if !errors.Is(err, ErrStaleMeta) {
		t.Fatalf("expected ErrStaleMeta, got %v", err)
	}
}

func TestApplyMetaTreatsIdenticalVersionWithoutGroupIDAsIdempotent(t *testing.T) {
	c := newTestCluster()
	meta := testMeta("c1", 1, 3, 9)
	if err := c.ApplyMeta(meta); err != nil {
		t.Fatalf("ApplyMeta() error = %v", err)
	}

	if err := c.ApplyMeta(meta); err != nil {
		t.Fatalf("expected idempotent replay, got %v", err)
	}
}

func TestRemoveMetaEvictsCachedChannel(t *testing.T) {
	c := newTestCluster()
	meta := testMeta("c1", 1, 3, 9)
	if err := c.ApplyMeta(meta); err != nil {
		t.Fatalf("ApplyMeta() error = %v", err)
	}

	if err := c.RemoveMeta(metaKey(meta)); err != nil {
		t.Fatalf("RemoveMeta() error = %v", err)
	}

	_, err := c.Status(metaKey(meta))
	if !errors.Is(err, ErrStaleMeta) {
		t.Fatalf("expected ErrStaleMeta, got %v", err)
	}
}
