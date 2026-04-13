package handler

import (
	"context"
	"errors"
	"testing"

	core "github.com/WuKongIM/WuKongIM/pkg/channel"
	"github.com/WuKongIM/WuKongIM/pkg/channel/runtime"
	"github.com/WuKongIM/WuKongIM/pkg/channel/store"
)

func TestAppendUsesRuntimeKeyAndReturnsMessageSeq(t *testing.T) {
	id := core.ChannelID{ID: "room-1", Type: 2}
	key := KeyFromChannelID(id)
	engine := openTestEngine(t)
	store := engine.ForChannel(key, id)
	handle := &fakeChannelHandle{
		id: key,
		status: core.ReplicaState{
			ChannelKey: key,
			Role:       core.ReplicaRoleLeader,
		},
	}
	handle.appendFn = func(_ context.Context, records []core.Record) (core.CommitResult, error) {
		base, err := store.Append(records)
		if err != nil {
			return core.CommitResult{}, err
		}
		for i, record := range records {
			view, err := decodeMessageView(record.Payload)
			if err != nil {
				return core.CommitResult{}, err
			}
			if view.Message.ClientMsgNo == "" {
				continue
			}
			if err := store.PutIdempotency(core.IdempotencyKey{
				ChannelID:   id,
				FromUID:     view.Message.FromUID,
				ClientMsgNo: view.Message.ClientMsgNo,
			}, core.IdempotencyEntry{
				MessageID:  view.Message.MessageID,
				MessageSeq: base + uint64(i) + 1,
				Offset:     base + uint64(i),
			}); err != nil {
				return core.CommitResult{}, err
			}
		}
		handle.status.HW = base + uint64(len(records))
		return core.CommitResult{BaseOffset: base, NextCommitHW: handle.status.HW, RecordCount: len(records)}, nil
	}
	rt := &fakeRuntime{channels: map[core.ChannelKey]*fakeChannelHandle{key: handle}}

	svc, err := New(Config{
		Runtime:    rt,
		Store:      engine,
		MessageIDs: &fakeMessageIDGenerator{},
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if err := svc.ApplyMeta(core.Meta{
		Key:         key,
		ID:          id,
		Epoch:       7,
		LeaderEpoch: 9,
		Leader:      1,
		Replicas:    []core.NodeID{1},
		ISR:         []core.NodeID{1},
		MinISR:      1,
		Status:      core.StatusActive,
		Features:    core.Features{MessageSeqFormat: core.MessageSeqFormatU64},
	}); err != nil {
		t.Fatalf("ApplyMeta() error = %v", err)
	}

	res, err := svc.Append(context.Background(), core.AppendRequest{
		ChannelID:             id,
		SupportsMessageSeqU64: true,
		Message: core.Message{
			FromUID:     "u1",
			ClientMsgNo: "m1",
			Payload:     []byte("payload"),
		},
	})
	if err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	if res.MessageSeq != 1 {
		t.Fatalf("MessageSeq = %d, want 1", res.MessageSeq)
	}
	if res.MessageID != 1 {
		t.Fatalf("MessageID = %d, want 1", res.MessageID)
	}
	if handle.idCalls != 0 {
		t.Fatalf("ID() calls = %d, want 0", handle.idCalls)
	}
	if handle.appendCalls != 1 {
		t.Fatalf("Append() calls = %d, want 1", handle.appendCalls)
	}
	if rt.lastChannelKey != key {
		t.Fatalf("runtime.Channel() key = %q, want %q", rt.lastChannelKey, key)
	}
}

func TestAppendReturnsExistingEntryOnIdempotentRetry(t *testing.T) {
	id := core.ChannelID{ID: "room-1", Type: 2}
	svc, rt, _ := newAppendService(t, id)

	first, err := svc.Append(context.Background(), core.AppendRequest{
		ChannelID:             id,
		SupportsMessageSeqU64: true,
		Message: core.Message{
			FromUID:     "u1",
			ClientMsgNo: "m1",
			Payload:     []byte("payload"),
		},
	})
	if err != nil {
		t.Fatalf("first Append() error = %v", err)
	}
	second, err := svc.Append(context.Background(), core.AppendRequest{
		ChannelID:             id,
		SupportsMessageSeqU64: true,
		Message: core.Message{
			FromUID:     "u1",
			ClientMsgNo: "m1",
			Payload:     []byte("payload"),
		},
	})
	if err != nil {
		t.Fatalf("second Append() error = %v", err)
	}
	if first.MessageID != second.MessageID || first.MessageSeq != second.MessageSeq {
		t.Fatalf("idempotent results differ: first=%+v second=%+v", first, second)
	}
	if rt.channels[KeyFromChannelID(id)].appendCalls != 1 {
		t.Fatalf("Append() calls = %d, want 1", rt.channels[KeyFromChannelID(id)].appendCalls)
	}
}

func TestAppendReturnsErrProtocolUpgradeRequiredForLegacyClientOnU64Channel(t *testing.T) {
	id := core.ChannelID{ID: "room-1", Type: 2}
	svc, _, _ := newAppendService(t, id)

	if err := svc.ApplyMeta(core.Meta{
		ID:          id,
		Epoch:       8,
		LeaderEpoch: 10,
		Leader:      1,
		Replicas:    []core.NodeID{1},
		ISR:         []core.NodeID{1},
		MinISR:      1,
		Status:      core.StatusActive,
		Features:    core.Features{MessageSeqFormat: core.MessageSeqFormatU64},
	}); err != nil {
		t.Fatalf("ApplyMeta() error = %v", err)
	}

	_, err := svc.Append(context.Background(), core.AppendRequest{
		ChannelID:             id,
		SupportsMessageSeqU64: false,
		Message: core.Message{
			FromUID:     "u1",
			ClientMsgNo: "m1",
			Payload:     []byte("payload"),
		},
	})
	if !errors.Is(err, core.ErrProtocolUpgradeRequired) {
		t.Fatalf("expected ErrProtocolUpgradeRequired, got %v", err)
	}
}

func TestAppendReturnsExistingEntryAtLegacySeqCeiling(t *testing.T) {
	id := core.ChannelID{ID: "room-1", Type: 2}
	svc, rt, _ := newAppendService(t, id)

	if err := svc.ApplyMeta(core.Meta{
		ID:          id,
		Epoch:       8,
		LeaderEpoch: 10,
		Leader:      1,
		Replicas:    []core.NodeID{1},
		ISR:         []core.NodeID{1},
		MinISR:      1,
		Status:      core.StatusActive,
		Features:    core.Features{MessageSeqFormat: core.MessageSeqFormatLegacyU32},
	}); err != nil {
		t.Fatalf("ApplyMeta() error = %v", err)
	}

	first, err := svc.Append(context.Background(), core.AppendRequest{
		ChannelID:             id,
		SupportsMessageSeqU64: true,
		Message: core.Message{
			FromUID:     "u1",
			ClientMsgNo: "m-legacy",
			Payload:     []byte("payload"),
		},
	})
	if err != nil {
		t.Fatalf("first Append() error = %v", err)
	}

	rt.channels[KeyFromChannelID(id)].status.HW = maxLegacyMessageSeq

	second, err := svc.Append(context.Background(), core.AppendRequest{
		ChannelID:             id,
		SupportsMessageSeqU64: true,
		Message: core.Message{
			FromUID:     "u1",
			ClientMsgNo: "m-legacy",
			Payload:     []byte("payload"),
		},
	})
	if err != nil {
		t.Fatalf("retry Append() error = %v", err)
	}
	if second.MessageID != first.MessageID || second.MessageSeq != first.MessageSeq {
		t.Fatalf("retry result = %+v, want %+v", second, first)
	}
	if rt.channels[KeyFromChannelID(id)].appendCalls != 1 {
		t.Fatalf("Append() calls = %d, want 1", rt.channels[KeyFromChannelID(id)].appendCalls)
	}
}

func newAppendService(t *testing.T, id core.ChannelID) (Service, *fakeRuntime, *store.Engine) {
	t.Helper()

	key := KeyFromChannelID(id)
	engine := openTestEngine(t)
	st := engine.ForChannel(key, id)
	handle := &fakeChannelHandle{
		id: key,
		status: core.ReplicaState{
			ChannelKey: key,
			Role:       core.ReplicaRoleLeader,
		},
	}
	handle.appendFn = func(_ context.Context, records []core.Record) (core.CommitResult, error) {
		base, err := st.Append(records)
		if err != nil {
			return core.CommitResult{}, err
		}
		for i, record := range records {
			view, err := decodeMessageView(record.Payload)
			if err != nil {
				return core.CommitResult{}, err
			}
			if view.Message.ClientMsgNo == "" {
				continue
			}
			if err := st.PutIdempotency(core.IdempotencyKey{
				ChannelID:   id,
				FromUID:     view.Message.FromUID,
				ClientMsgNo: view.Message.ClientMsgNo,
			}, core.IdempotencyEntry{
				MessageID:  view.Message.MessageID,
				MessageSeq: base + uint64(i) + 1,
				Offset:     base + uint64(i),
			}); err != nil {
				return core.CommitResult{}, err
			}
		}
		handle.status.HW = base + uint64(len(records))
		return core.CommitResult{BaseOffset: base, NextCommitHW: handle.status.HW, RecordCount: len(records)}, nil
	}
	rt := &fakeRuntime{channels: map[core.ChannelKey]*fakeChannelHandle{key: handle}}

	svc, err := New(Config{
		Runtime:    rt,
		Store:      engine,
		MessageIDs: &fakeMessageIDGenerator{},
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if err := svc.ApplyMeta(core.Meta{
		Key:         key,
		ID:          id,
		Epoch:       7,
		LeaderEpoch: 9,
		Leader:      1,
		Replicas:    []core.NodeID{1},
		ISR:         []core.NodeID{1},
		MinISR:      1,
		Status:      core.StatusActive,
		Features:    core.Features{MessageSeqFormat: core.MessageSeqFormatU64},
	}); err != nil {
		t.Fatalf("ApplyMeta() error = %v", err)
	}
	return svc, rt, engine
}

func openTestEngine(tb testing.TB) *store.Engine {
	tb.Helper()
	engine, err := store.Open(tb.TempDir())
	if err != nil {
		tb.Fatalf("Open() error = %v", err)
	}
	tb.Cleanup(func() {
		if err := engine.Close(); err != nil {
			tb.Fatalf("Close() error = %v", err)
		}
	})
	return engine
}

type fakeRuntime struct {
	channels       map[core.ChannelKey]*fakeChannelHandle
	lastChannelKey core.ChannelKey
}

func (r *fakeRuntime) EnsureChannel(meta core.Meta) error      { return nil }
func (r *fakeRuntime) RemoveChannel(key core.ChannelKey) error { return nil }
func (r *fakeRuntime) ApplyMeta(meta core.Meta) error          { return nil }
func (r *fakeRuntime) Close() error                            { return nil }
func (r *fakeRuntime) ServeFetch(context.Context, runtime.FetchRequestEnvelope) (runtime.FetchResponseEnvelope, error) {
	return runtime.FetchResponseEnvelope{}, nil
}
func (r *fakeRuntime) Channel(key core.ChannelKey) (runtime.ChannelHandle, bool) {
	r.lastChannelKey = key
	h, ok := r.channels[key]
	return h, ok
}

type fakeChannelHandle struct {
	id          core.ChannelKey
	status      core.ReplicaState
	appendCalls int
	idCalls     int
	appendFn    func(context.Context, []core.Record) (core.CommitResult, error)
}

func (h *fakeChannelHandle) ID() core.ChannelKey {
	h.idCalls++
	return h.id
}

func (h *fakeChannelHandle) Meta() core.Meta           { return core.Meta{Key: h.id} }
func (h *fakeChannelHandle) Status() core.ReplicaState { return h.status }
func (h *fakeChannelHandle) Append(ctx context.Context, records []core.Record) (core.CommitResult, error) {
	h.appendCalls++
	return h.appendFn(ctx, records)
}

type fakeMessageIDGenerator struct{ next uint64 }

func (g *fakeMessageIDGenerator) Next() uint64 {
	g.next++
	return g.next
}
