package multiraft_test

import (
	"errors"
	"testing"
	"time"

	"github.com/WuKongIM/wraft/multiraft"
)

func TestNewValidatesRequiredOptions(t *testing.T) {
	_, err := multiraft.New(multiraft.Options{})
	if !errors.Is(err, multiraft.ErrInvalidOptions) {
		t.Fatalf("expected ErrInvalidOptions, got %v", err)
	}
}

func TestPublicTypesExposeApprovedFields(t *testing.T) {
	var opts multiraft.Options
	opts.NodeID = 1
	opts.TickInterval = time.Second
	opts.Workers = 1

	if opts.NodeID != 1 {
		t.Fatalf("unexpected NodeID: %d", opts.NodeID)
	}
}
