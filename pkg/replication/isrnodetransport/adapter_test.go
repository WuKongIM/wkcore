package isrnodetransport

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/replication/isr"
	"github.com/WuKongIM/WuKongIM/pkg/replication/isrnode"
	"github.com/WuKongIM/WuKongIM/pkg/transport/nodetransport"
)

func TestSessionManagerReusesSessionPerPeer(t *testing.T) {
	mux := nodetransport.NewRPCMux()
	client := nodetransport.NewClient(nodetransport.NewPool(staticDiscovery{
		addrs: map[uint64]string{},
	}, 1, time.Second))
	defer client.Stop()

	adapter, err := New(Options{
		LocalNode: 1,
		Client:    client,
		RPCMux:    mux,
		FetchService: fetchServiceFunc(func(ctx context.Context, req isrnode.FetchRequestEnvelope) (isrnode.FetchResponseEnvelope, error) {
			return isrnode.FetchResponseEnvelope{}, nil
		}),
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	first := adapter.SessionManager().Session(2)
	second := adapter.SessionManager().Session(2)
	if first != second {
		t.Fatal("expected session reuse per peer")
	}
}

func TestPeerSessionReportsHardBackpressureWhileRPCInFlight(t *testing.T) {
	server := nodetransport.NewServer()
	mux := nodetransport.NewRPCMux()
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	mux.Handle(RPCServiceFetch, func(ctx context.Context, body []byte) ([]byte, error) {
		started <- struct{}{}
		<-release
		return encodeFetchResponse(isrnode.FetchResponseEnvelope{
			GroupKey:   "g1",
			Epoch:      3,
			Generation: 7,
			LeaderHW:   9,
		})
	})
	server.HandleRPCMux(mux)
	if err := server.Start("127.0.0.1:0"); err != nil {
		t.Fatalf("server.Start() error = %v", err)
	}
	defer server.Stop()

	client := nodetransport.NewClient(nodetransport.NewPool(staticDiscovery{
		addrs: map[uint64]string{2: server.Listener().Addr().String()},
	}, 1, time.Second))
	defer client.Stop()

	adapter, err := New(Options{
		LocalNode: 1,
		Client:    client,
		RPCMux:    nodetransport.NewRPCMux(),
		FetchService: fetchServiceFunc(func(ctx context.Context, req isrnode.FetchRequestEnvelope) (isrnode.FetchResponseEnvelope, error) {
			return isrnode.FetchResponseEnvelope{}, nil
		}),
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	session := adapter.SessionManager().Session(2)
	errCh := make(chan error, 1)
	go func() {
		errCh <- session.Send(isrnode.Envelope{
			Peer:       2,
			GroupKey:   "g1",
			Epoch:      3,
			Generation: 7,
			RequestID:  1,
			Kind:       isrnode.MessageKindFetchRequest,
			FetchRequest: &isrnode.FetchRequestEnvelope{
				GroupKey:    "g1",
				Epoch:       3,
				Generation:  7,
				ReplicaID:   1,
				FetchOffset: 11,
				OffsetEpoch: 3,
				MaxBytes:    4096,
			},
		})
	}()

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for rpc handler to block")
	}

	state := session.Backpressure()
	if state.Level != isrnode.BackpressureHard {
		t.Fatalf("Backpressure().Level = %v, want hard", state.Level)
	}
	if state.PendingRequests != 1 {
		t.Fatalf("PendingRequests = %d, want 1", state.PendingRequests)
	}
	if state.PendingBytes <= 0 {
		t.Fatalf("PendingBytes = %d, want > 0", state.PendingBytes)
	}

	releaseOnce.Do(func() { close(release) })
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("session.Send() error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for session.Send()")
	}

	if state := session.Backpressure(); state.Level != isrnode.BackpressureNone {
		t.Fatalf("Backpressure().Level after response = %v, want none", state.Level)
	}
}

func TestPeerSessionUsesConfiguredRPCTimeout(t *testing.T) {
	server := nodetransport.NewServer()
	mux := nodetransport.NewRPCMux()
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	mux.Handle(RPCServiceFetch, func(ctx context.Context, body []byte) ([]byte, error) {
		started <- struct{}{}
		<-release
		return encodeFetchResponse(isrnode.FetchResponseEnvelope{
			GroupKey:   "g-timeout",
			Epoch:      3,
			Generation: 7,
			LeaderHW:   9,
		})
	})
	server.HandleRPCMux(mux)
	if err := server.Start("127.0.0.1:0"); err != nil {
		t.Fatalf("server.Start() error = %v", err)
	}
	defer func() {
		close(release)
		server.Stop()
	}()

	client := nodetransport.NewClient(nodetransport.NewPool(staticDiscovery{
		addrs: map[uint64]string{2: server.Listener().Addr().String()},
	}, 1, time.Second))
	defer client.Stop()

	adapter := newAdapterWithTestTimeout(t, client, 25*time.Millisecond)
	session := adapter.SessionManager().Session(2)

	errCh := make(chan error, 1)
	go func() {
		errCh <- session.Send(fetchRequestEnvelopeForTest("g-timeout"))
	}()

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for rpc handler to block")
	}

	select {
	case err := <-errCh:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("session.Send() error = %v, want context deadline exceeded", err)
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatal("expected configured rpc timeout to abort fetch request")
	}
}

func TestPeerSessionSendReturnsErrStoppedWhenClientStops(t *testing.T) {
	server := nodetransport.NewServer()
	mux := nodetransport.NewRPCMux()
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	mux.Handle(RPCServiceFetch, func(ctx context.Context, body []byte) ([]byte, error) {
		started <- struct{}{}
		<-release
		return encodeFetchResponse(isrnode.FetchResponseEnvelope{
			GroupKey:   "g-stop",
			Epoch:      3,
			Generation: 7,
			LeaderHW:   9,
		})
	})
	server.HandleRPCMux(mux)
	if err := server.Start("127.0.0.1:0"); err != nil {
		t.Fatalf("server.Start() error = %v", err)
	}
	defer func() {
		close(release)
		server.Stop()
	}()

	client := nodetransport.NewClient(nodetransport.NewPool(staticDiscovery{
		addrs: map[uint64]string{2: server.Listener().Addr().String()},
	}, 1, time.Second))

	adapter := newAdapterWithTestTimeout(t, client, time.Second)
	session := adapter.SessionManager().Session(2)

	errCh := make(chan error, 1)
	go func() {
		errCh <- session.Send(fetchRequestEnvelopeForTest("g-stop"))
	}()

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for rpc handler to block")
	}

	client.Stop()

	select {
	case err := <-errCh:
		if !errors.Is(err, nodetransport.ErrStopped) {
			t.Fatalf("session.Send() error = %v, want ErrStopped", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("expected client.Stop to abort pending fetch rpc")
	}

	if state := session.Backpressure(); state.Level != isrnode.BackpressureNone {
		t.Fatalf("Backpressure().Level after stop = %v, want none", state.Level)
	}
}

type staticDiscovery struct {
	addrs map[uint64]string
}

func (d staticDiscovery) Resolve(nodeID uint64) (string, error) {
	addr, ok := d.addrs[nodeID]
	if !ok {
		return "", nodetransport.ErrNodeNotFound
	}
	return addr, nil
}

type fetchServiceFunc func(context.Context, isrnode.FetchRequestEnvelope) (isrnode.FetchResponseEnvelope, error)

func (f fetchServiceFunc) ServeFetch(ctx context.Context, req isrnode.FetchRequestEnvelope) (isrnode.FetchResponseEnvelope, error) {
	return f(ctx, req)
}

func newAdapterWithTestTimeout(t *testing.T, client *nodetransport.Client, timeout time.Duration) *Adapter {
	t.Helper()

	opts := Options{
		LocalNode:  1,
		Client:     client,
		RPCMux:     nodetransport.NewRPCMux(),
		RPCTimeout: timeout,
		FetchService: fetchServiceFunc(func(ctx context.Context, req isrnode.FetchRequestEnvelope) (isrnode.FetchResponseEnvelope, error) {
			return isrnode.FetchResponseEnvelope{}, nil
		}),
	}

	adapter, err := New(opts)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	return adapter
}

func fetchRequestEnvelopeForTest(groupKey string) isrnode.Envelope {
	key := isr.GroupKey(groupKey)
	return isrnode.Envelope{
		Peer:       2,
		GroupKey:   key,
		Epoch:      3,
		Generation: 7,
		RequestID:  1,
		Kind:       isrnode.MessageKindFetchRequest,
		FetchRequest: &isrnode.FetchRequestEnvelope{
			GroupKey:    key,
			Epoch:       3,
			Generation:  7,
			ReplicaID:   1,
			FetchOffset: 11,
			OffsetEpoch: 3,
			MaxBytes:    4096,
		},
	}
}
