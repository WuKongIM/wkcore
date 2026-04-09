package isrnodetransport

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/fnv"
	"net"
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

func TestPeerSessionBackpressureAllowsConfiguredConcurrentInflightRPCs(t *testing.T) {
	server := nodetransport.NewServer()
	mux := nodetransport.NewRPCMux()
	started := make(chan struct{}, 2)
	release := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	mux.Handle(RPCServiceFetch, func(ctx context.Context, body []byte) ([]byte, error) {
		started <- struct{}{}
		<-release
		return encodeFetchResponse(isrnode.FetchResponseEnvelope{
			GroupKey:   "g-concurrent",
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
		MaxPendingFetchRPC: 2,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	session := adapter.SessionManager().Session(2)
	errCh1 := make(chan error, 1)
	go func() {
		errCh1 <- session.Send(fetchRequestEnvelopeForTest("g-concurrent-1"))
	}()

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for first rpc handler to block")
	}

	state := session.Backpressure()
	if state.Level != isrnode.BackpressureNone {
		t.Fatalf("Backpressure().Level after first in-flight request = %v, want none", state.Level)
	}
	if state.PendingRequests != 1 {
		t.Fatalf("PendingRequests after first in-flight request = %d, want 1", state.PendingRequests)
	}

	errCh2 := make(chan error, 1)
	go func() {
		errCh2 <- session.Send(fetchRequestEnvelopeForTest("g-concurrent-2"))
	}()

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for second rpc handler to block")
	}

	state = session.Backpressure()
	if state.Level != isrnode.BackpressureHard {
		t.Fatalf("Backpressure().Level after reaching configured in-flight limit = %v, want hard", state.Level)
	}
	if state.PendingRequests != 2 {
		t.Fatalf("PendingRequests after second in-flight request = %d, want 2", state.PendingRequests)
	}

	releaseOnce.Do(func() { close(release) })
	for _, errCh := range []<-chan error{errCh1, errCh2} {
		select {
		case err := <-errCh:
			if err != nil {
				t.Fatalf("session.Send() error = %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("timeout waiting for session.Send()")
		}
	}

	if state := session.Backpressure(); state.Level != isrnode.BackpressureNone {
		t.Fatalf("Backpressure().Level after responses = %v, want none", state.Level)
	}
}

func TestPeerSessionDistributesConcurrentFetchRPCsAcrossPoolShards(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen() error = %v", err)
	}
	defer ln.Close()

	started := make(chan struct{}, 2)
	release := make(chan struct{})
	var releaseOnce sync.Once
	var serverWG sync.WaitGroup
	defer func() {
		releaseOnce.Do(func() { close(release) })
		_ = ln.Close()
		serverWG.Wait()
	}()

	serverWG.Add(1)
	go func() {
		defer serverWG.Done()
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			serverWG.Add(1)
			go func(conn net.Conn) {
				defer serverWG.Done()
				defer conn.Close()

				for {
					msgType, body, err := nodetransport.ReadMessage(conn)
					if err != nil {
						return
					}
					if msgType != nodetransport.MsgTypeRPCRequest || len(body) < 8 {
						return
					}

					requestID := binary.BigEndian.Uint64(body[:8])
					started <- struct{}{}
					<-release

					respPayload, err := encodeFetchResponse(isrnode.FetchResponseEnvelope{
						GroupKey:   "g-ok",
						Epoch:      3,
						Generation: 7,
						LeaderHW:   9,
					})
					if err != nil {
						return
					}
					if err := nodetransport.WriteMessage(conn, nodetransport.MsgTypeRPCResponse, encodeRPCResponseForTest(requestID, respPayload)); err != nil {
						return
					}
				}
			}(conn)
		}
	}()
	client := nodetransport.NewClient(nodetransport.NewPool(staticDiscovery{
		addrs: map[uint64]string{2: ln.Addr().String()},
	}, 2, time.Second))
	defer client.Stop()

	adapter, err := New(Options{
		LocalNode: 1,
		Client:    client,
		RPCMux:    nodetransport.NewRPCMux(),
		FetchService: fetchServiceFunc(func(ctx context.Context, req isrnode.FetchRequestEnvelope) (isrnode.FetchResponseEnvelope, error) {
			return isrnode.FetchResponseEnvelope{}, nil
		}),
		MaxPendingFetchRPC: 2,
		RPCTimeout:         2 * time.Second,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	groupKey0 := groupKeyForShard(t, 0, 2)
	groupKey1 := groupKeyForShard(t, 1, 2)
	session := adapter.SessionManager().Session(2)

	errCh1 := make(chan error, 1)
	go func() {
		errCh1 <- session.Send(fetchRequestEnvelopeForTest(string(groupKey0)))
	}()

	errCh2 := make(chan error, 1)
	go func() {
		errCh2 <- session.Send(fetchRequestEnvelopeForTest(string(groupKey1)))
	}()

	for i := 0; i < 2; i++ {
		select {
		case <-started:
		case <-time.After(300 * time.Millisecond):
			t.Fatalf("started fetch handlers = %d, want 2", i)
		}
	}

	releaseOnce.Do(func() { close(release) })
	for _, errCh := range []<-chan error{errCh1, errCh2} {
		select {
		case err := <-errCh:
			if err != nil {
				t.Fatalf("session.Send() error = %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("timeout waiting for session.Send()")
		}
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

func TestPeerSessionSendDispatchesProgressAckRPC(t *testing.T) {
	server := nodetransport.NewServer()
	mux := nodetransport.NewRPCMux()
	got := make(chan isrnode.ProgressAckEnvelope, 1)
	mux.Handle(RPCServiceProgressAck, func(ctx context.Context, body []byte) ([]byte, error) {
		ack, err := decodeProgressAck(body)
		if err != nil {
			return nil, err
		}
		got <- ack
		return nil, nil
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

	adapter := newAdapterWithTestTimeout(t, client, time.Second)
	session := adapter.SessionManager().Session(2)
	env := isrnode.Envelope{
		Peer:       2,
		GroupKey:   "g-progress",
		Epoch:      3,
		Generation: 7,
		RequestID:  1,
		Kind:       isrnode.MessageKindProgressAck,
		ProgressAck: &isrnode.ProgressAckEnvelope{
			GroupKey:    "g-progress",
			Epoch:       3,
			Generation:  7,
			ReplicaID:   1,
			MatchOffset: 11,
		},
	}

	if err := session.Send(env); err != nil {
		t.Fatalf("session.Send() error = %v", err)
	}

	select {
	case ack := <-got:
		if ack.MatchOffset != 11 {
			t.Fatalf("MatchOffset = %d, want 11", ack.MatchOffset)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for progress ack rpc")
	}
}

func TestAdapterHandleProgressAckInvokesRegisteredHandler(t *testing.T) {
	client := nodetransport.NewClient(nodetransport.NewPool(staticDiscovery{
		addrs: map[uint64]string{},
	}, 1, time.Second))
	defer client.Stop()

	mux := nodetransport.NewRPCMux()
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

	delivered := make(chan isrnode.Envelope, 1)
	adapter.RegisterHandler(func(env isrnode.Envelope) {
		delivered <- env
	})

	body, err := encodeProgressAck(isrnode.ProgressAckEnvelope{
		GroupKey:    "g-progress",
		Epoch:       3,
		Generation:  7,
		ReplicaID:   2,
		MatchOffset: 19,
	})
	if err != nil {
		t.Fatalf("encodeProgressAck() error = %v", err)
	}

	servicePayload := append([]byte{RPCServiceProgressAck}, body...)
	if _, err := mux.HandleRPC(context.Background(), servicePayload); err != nil {
		t.Fatalf("HandleRPC() error = %v", err)
	}

	select {
	case env := <-delivered:
		if env.Kind != isrnode.MessageKindProgressAck {
			t.Fatalf("Kind = %v, want progress ack", env.Kind)
		}
		if env.ProgressAck == nil || env.ProgressAck.MatchOffset != 19 {
			t.Fatalf("ProgressAck = %+v", env.ProgressAck)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for delivered progress ack")
	}
}

func TestPeerSessionTryBatchQueuesMultipleFetchRequests(t *testing.T) {
	client := nodetransport.NewClient(nodetransport.NewPool(staticDiscovery{
		addrs: map[uint64]string{},
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
	if !session.TryBatch(fetchRequestEnvelopeForTest("g-batch-1")) {
		t.Fatal("expected first fetch request to be queued for batching")
	}
	if !session.TryBatch(fetchRequestEnvelopeForTest("g-batch-2")) {
		t.Fatal("expected second fetch request to be queued for batching")
	}

	peer := session.(*peerSession)
	peer.mu.Lock()
	queued := len(peer.batchFetchQueue)
	peer.mu.Unlock()
	if queued != 2 {
		t.Fatalf("queued batch fetch requests = %d, want 2", queued)
	}
}

func TestPeerSessionFlushSendsSingleFetchBatchRPC(t *testing.T) {
	server := nodetransport.NewServer()
	mux := nodetransport.NewRPCMux()

	var (
		mu          sync.Mutex
		batchCalls  int
		singleCalls int
		batchItems  int
	)

	mux.Handle(RPCServiceFetch, func(ctx context.Context, body []byte) ([]byte, error) {
		mu.Lock()
		singleCalls++
		mu.Unlock()
		return encodeFetchResponse(isrnode.FetchResponseEnvelope{
			GroupKey:   "g-single",
			Epoch:      3,
			Generation: 7,
			LeaderHW:   9,
		})
	})
	mux.Handle(RPCServiceFetchBatch, func(ctx context.Context, body []byte) ([]byte, error) {
		req, err := decodeFetchBatchRequest(body)
		if err != nil {
			return nil, err
		}

		mu.Lock()
		batchCalls++
		batchItems = len(req.Items)
		mu.Unlock()

		resp := isrnode.FetchBatchResponseEnvelope{
			Items: make([]isrnode.FetchBatchResponseItem, 0, len(req.Items)),
		}
		for _, item := range req.Items {
			resp.Items = append(resp.Items, isrnode.FetchBatchResponseItem{
				RequestID: item.RequestID,
				Response: &isrnode.FetchResponseEnvelope{
					GroupKey:   item.Request.GroupKey,
					Epoch:      item.Request.Epoch,
					Generation: item.Request.Generation,
					LeaderHW:   item.Request.FetchOffset,
				},
			})
		}
		return encodeFetchBatchResponse(resp)
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

	adapter := newAdapterWithTestTimeout(t, client, time.Second)
	session := adapter.SessionManager().Session(2)
	if !session.TryBatch(fetchRequestEnvelopeForTest("g-batch-1")) {
		t.Fatal("expected first fetch request to be queued for batching")
	}
	if !session.TryBatch(fetchRequestEnvelopeForTest("g-batch-2")) {
		t.Fatal("expected second fetch request to be queued for batching")
	}

	if err := session.Flush(); err != nil {
		t.Fatalf("session.Flush() error = %v", err)
	}

	mu.Lock()
	gotBatchCalls := batchCalls
	gotSingleCalls := singleCalls
	gotBatchItems := batchItems
	mu.Unlock()

	if gotBatchCalls != 1 {
		t.Fatalf("batch rpc calls = %d, want 1", gotBatchCalls)
	}
	if gotSingleCalls != 0 {
		t.Fatalf("single fetch rpc calls = %d, want 0", gotSingleCalls)
	}
	if gotBatchItems != 2 {
		t.Fatalf("batch item count = %d, want 2", gotBatchItems)
	}
}

func TestBatchedFetchResponseFansOutResultsPerGroup(t *testing.T) {
	server := nodetransport.NewServer()
	mux := nodetransport.NewRPCMux()
	mux.Handle(RPCServiceFetchBatch, func(ctx context.Context, body []byte) ([]byte, error) {
		req, err := decodeFetchBatchRequest(body)
		if err != nil {
			return nil, err
		}
		resp := isrnode.FetchBatchResponseEnvelope{
			Items: make([]isrnode.FetchBatchResponseItem, 0, len(req.Items)),
		}
		for _, item := range req.Items {
			resp.Items = append(resp.Items, isrnode.FetchBatchResponseItem{
				RequestID: item.RequestID,
				Response: &isrnode.FetchResponseEnvelope{
					GroupKey:   item.Request.GroupKey,
					Epoch:      item.Request.Epoch,
					Generation: item.Request.Generation,
					LeaderHW:   item.Request.FetchOffset + 100,
				},
			})
		}
		return encodeFetchBatchResponse(resp)
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

	adapter := newAdapterWithTestTimeout(t, client, time.Second)
	delivered := make(chan isrnode.Envelope, 2)
	adapter.RegisterHandler(func(env isrnode.Envelope) {
		delivered <- env
	})

	session := adapter.SessionManager().Session(2)
	req1 := fetchRequestEnvelopeForTest("g-fanout-1")
	req1.RequestID = 101
	req2 := fetchRequestEnvelopeForTest("g-fanout-2")
	req2.RequestID = 102
	if !session.TryBatch(req1) {
		t.Fatal("expected first fetch request to be queued for batching")
	}
	if !session.TryBatch(req2) {
		t.Fatal("expected second fetch request to be queued for batching")
	}
	if err := session.Flush(); err != nil {
		t.Fatalf("session.Flush() error = %v", err)
	}

	got := make(map[isr.GroupKey]isrnode.Envelope, 2)
	for i := 0; i < 2; i++ {
		select {
		case env := <-delivered:
			got[env.GroupKey] = env
		case <-time.After(2 * time.Second):
			t.Fatalf("delivered envelopes = %d, want 2", len(got))
		}
	}

	env1, ok := got[isr.GroupKey("g-fanout-1")]
	if !ok {
		t.Fatal("missing fanout response for g-fanout-1")
	}
	if env1.Kind != isrnode.MessageKindFetchResponse {
		t.Fatalf("g-fanout-1 kind = %v, want fetch response", env1.Kind)
	}
	if env1.RequestID != 101 {
		t.Fatalf("g-fanout-1 request id = %d, want 101", env1.RequestID)
	}
	if env1.FetchResponse == nil || env1.FetchResponse.LeaderHW != 111 {
		t.Fatalf("g-fanout-1 response = %+v, want LeaderHW=111", env1.FetchResponse)
	}

	env2, ok := got[isr.GroupKey("g-fanout-2")]
	if !ok {
		t.Fatal("missing fanout response for g-fanout-2")
	}
	if env2.Kind != isrnode.MessageKindFetchResponse {
		t.Fatalf("g-fanout-2 kind = %v, want fetch response", env2.Kind)
	}
	if env2.RequestID != 102 {
		t.Fatalf("g-fanout-2 request id = %d, want 102", env2.RequestID)
	}
	if env2.FetchResponse == nil || env2.FetchResponse.LeaderHW != 111 {
		t.Fatalf("g-fanout-2 response = %+v, want LeaderHW=111", env2.FetchResponse)
	}
}

func TestPeerSessionFlushBatchFailureDeliversFetchFailureEnvelope(t *testing.T) {
	client := nodetransport.NewClient(nodetransport.NewPool(staticDiscovery{
		addrs: map[uint64]string{},
	}, 1, time.Second))
	defer client.Stop()

	adapter := newAdapterWithTestTimeout(t, client, 20*time.Millisecond)
	delivered := make(chan isrnode.Envelope, 1)
	adapter.RegisterHandler(func(env isrnode.Envelope) {
		delivered <- env
	})

	session := adapter.SessionManager().Session(2)
	req := fetchRequestEnvelopeForTest("g-batch-fail")
	req.RequestID = 99
	if !session.TryBatch(req) {
		t.Fatal("expected fetch request to be queued for batching")
	}
	if err := session.Flush(); err == nil {
		t.Fatal("expected flush to fail when peer is unavailable")
	}

	select {
	case env := <-delivered:
		if env.Kind != isrnode.MessageKindFetchFailure {
			t.Fatalf("kind = %v, want fetch failure", env.Kind)
		}
		if env.RequestID != 99 || env.GroupKey != isr.GroupKey("g-batch-fail") {
			t.Fatalf("failure envelope = %+v", env)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for fetch failure envelope")
	}
}

func TestPeerSessionBackpressureIncludesQueuedBatchRequests(t *testing.T) {
	client := nodetransport.NewClient(nodetransport.NewPool(staticDiscovery{
		addrs: map[uint64]string{},
	}, 1, time.Second))
	defer client.Stop()

	adapter := newAdapterWithTestTimeout(t, client, time.Second)
	session := adapter.SessionManager().Session(2).(*peerSession)

	for i := 0; i < session.maxQueuedFetchRequests(); i++ {
		req := fetchRequestEnvelopeForTest(fmt.Sprintf("g-queued-%d", i))
		req.RequestID = uint64(i + 1)
		if !session.TryBatch(req) {
			t.Fatalf("TryBatch(%d) returned false", i)
		}
	}

	state := session.Backpressure()
	if state.PendingRequests < session.maxQueuedFetchRequests() {
		t.Fatalf("PendingRequests = %d, want at least %d", state.PendingRequests, session.maxQueuedFetchRequests())
	}
	if state.PendingBytes <= 0 {
		t.Fatalf("PendingBytes = %d, want > 0", state.PendingBytes)
	}
	if state.Level != isrnode.BackpressureHard {
		t.Fatalf("Backpressure().Level = %v, want hard", state.Level)
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

func groupKeyForShard(t *testing.T, shard int, poolSize int) isr.GroupKey {
	t.Helper()

	for i := 0; i < 1024; i++ {
		key := isr.GroupKey(fmt.Sprintf("g-shard-%d", i))
		if fetchShardForTest(key, poolSize) == shard {
			return key
		}
	}
	t.Fatalf("no group key found for shard %d", shard)
	return ""
}

func fetchShardForTest(groupKey isr.GroupKey, poolSize int) int {
	hasher := fnv.New64a()
	_, _ = hasher.Write([]byte(groupKey))
	return int(hasher.Sum64() % uint64(poolSize))
}

func encodeRPCResponseForTest(requestID uint64, data []byte) []byte {
	buf := make([]byte, 9+len(data))
	binary.BigEndian.PutUint64(buf[:8], requestID)
	copy(buf[9:], data)
	return buf
}
