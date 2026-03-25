package multiraft_test

import (
	"context"
	"errors"
	"reflect"
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

func TestOpenGroupRegistersGroup(t *testing.T) {
	rt := newTestRuntime(t)
	err := rt.OpenGroup(context.Background(), multiraft.GroupOptions{
		ID:           10,
		Storage:      newFakeStorage(),
		StateMachine: newFakeStateMachine(),
	})
	if err != nil {
		t.Fatalf("OpenGroup() error = %v", err)
	}
	if got := rt.Groups(); !reflect.DeepEqual(got, []multiraft.GroupID{10}) {
		t.Fatalf("Groups() = %v", got)
	}
}

func TestOpenGroupRejectsDuplicateID(t *testing.T) {
	rt := newTestRuntime(t)
	if err := rt.OpenGroup(context.Background(), newGroupOptions(10)); err != nil {
		t.Fatalf("first OpenGroup() error = %v", err)
	}
	err := rt.OpenGroup(context.Background(), newGroupOptions(10))
	if !errors.Is(err, multiraft.ErrGroupExists) {
		t.Fatalf("expected ErrGroupExists, got %v", err)
	}
}
