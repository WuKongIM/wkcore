package isrnodetransport

import (
	"context"
	"sync"
	"testing"
	"time"

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
