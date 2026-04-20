package transport

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/fnv"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/channel"
	"github.com/WuKongIM/WuKongIM/pkg/channel/runtime"
	"github.com/WuKongIM/WuKongIM/pkg/transport"
)

func TestSessionManagerReusesSessionPerPeer(t *testing.T) {
	mux := transport.NewRPCMux()
	client := transport.NewClient(transport.NewPool(staticDiscovery{
		addrs: map[uint64]string{},
	}, 1, time.Second))
	defer client.Stop()

	adapter, err := New(Options{
		LocalNode: 1,
		Client:    client,
		RPCMux:    mux,
		FetchService: fetchServiceFunc(func(ctx context.Context, req runtime.FetchRequestEnvelope) (runtime.FetchResponseEnvelope, error) {
			return runtime.FetchResponseEnvelope{}, nil
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

func TestNewDoesNotCollideWithClusterManagedSlotRPCService(t *testing.T) {
	const managedSlotRPCServiceID uint8 = 20

	mux := transport.NewRPCMux()
	mux.Handle(managedSlotRPCServiceID, func(ctx context.Context, body []byte) ([]byte, error) {
		return nil, nil
	})

	client := transport.NewClient(transport.NewPool(staticDiscovery{
		addrs: map[uint64]string{},
	}, 1, time.Second))
	defer client.Stop()

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("New() panicked after cluster reserved service registration: %v", r)
		}
	}()

	adapter, err := New(Options{
		LocalNode: 1,
		Client:    client,
		RPCMux:    mux,
		FetchService: fetchServiceFunc(func(ctx context.Context, req runtime.FetchRequestEnvelope) (runtime.FetchResponseEnvelope, error) {
			return runtime.FetchResponseEnvelope{}, nil
		}),
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if adapter == nil {
		t.Fatal("New() returned nil adapter")
	}
}

func TestPeerSessionReportsHardBackpressureWhileRPCInFlight(t *testing.T) {
	server := transport.NewServer()
	mux := transport.NewRPCMux()
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	mux.Handle(RPCServiceFetch, func(ctx context.Context, body []byte) ([]byte, error) {
		started <- struct{}{}
		<-release
		return encodeFetchResponse(runtime.FetchResponseEnvelope{
			ChannelKey: "g1",
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

	client := transport.NewClient(transport.NewPool(staticDiscovery{
		addrs: map[uint64]string{2: server.Listener().Addr().String()},
	}, 1, time.Second))
	defer client.Stop()

	adapter, err := New(Options{
		LocalNode: 1,
		Client:    client,
		RPCMux:    transport.NewRPCMux(),
		FetchService: fetchServiceFunc(func(ctx context.Context, req runtime.FetchRequestEnvelope) (runtime.FetchResponseEnvelope, error) {
			return runtime.FetchResponseEnvelope{}, nil
		}),
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	session := adapter.SessionManager().Session(2)
	errCh := make(chan error, 1)
	go func() {
		errCh <- session.Send(runtime.Envelope{
			Peer:       2,
			ChannelKey: "g1",
			Epoch:      3,
			Generation: 7,
			RequestID:  1,
			Kind:       runtime.MessageKindFetchRequest,
			FetchRequest: &runtime.FetchRequestEnvelope{
				ChannelKey:  "g1",
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
	if state.Level != runtime.BackpressureHard {
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

	if state := session.Backpressure(); state.Level != runtime.BackpressureNone {
		t.Fatalf("Backpressure().Level after response = %v, want none", state.Level)
	}
}

func TestPeerSessionBackpressureAllowsConfiguredConcurrentInflightRPCs(t *testing.T) {
	server := transport.NewServer()
	mux := transport.NewRPCMux()
	started := make(chan struct{}, 2)
	release := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	mux.Handle(RPCServiceFetch, func(ctx context.Context, body []byte) ([]byte, error) {
		started <- struct{}{}
		<-release
		return encodeFetchResponse(runtime.FetchResponseEnvelope{
			ChannelKey: "g-concurrent",
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

	client := transport.NewClient(transport.NewPool(staticDiscovery{
		addrs: map[uint64]string{2: server.Listener().Addr().String()},
	}, 1, time.Second))
	defer client.Stop()

	adapter, err := New(Options{
		LocalNode: 1,
		Client:    client,
		RPCMux:    transport.NewRPCMux(),
		FetchService: fetchServiceFunc(func(ctx context.Context, req runtime.FetchRequestEnvelope) (runtime.FetchResponseEnvelope, error) {
			return runtime.FetchResponseEnvelope{}, nil
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
	if state.Level != runtime.BackpressureNone {
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
	if state.Level != runtime.BackpressureHard {
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

	if state := session.Backpressure(); state.Level != runtime.BackpressureNone {
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
					msgType, body, err := transport.ReadMessage(conn)
					if err != nil {
						return
					}
					if msgType != transport.MsgTypeRPCRequest || len(body) < 8 {
						return
					}

					requestID := binary.BigEndian.Uint64(body[:8])
					started <- struct{}{}
					<-release

					respPayload, err := encodeFetchResponse(runtime.FetchResponseEnvelope{
						ChannelKey: "g-ok",
						Epoch:      3,
						Generation: 7,
						LeaderHW:   9,
					})
					if err != nil {
						return
					}
					if err := transport.WriteMessage(conn, transport.MsgTypeRPCResponse, encodeRPCResponseForTest(requestID, respPayload)); err != nil {
						return
					}
				}
			}(conn)
		}
	}()
	client := transport.NewClient(transport.NewPool(staticDiscovery{
		addrs: map[uint64]string{2: ln.Addr().String()},
	}, 2, time.Second))
	defer client.Stop()

	adapter, err := New(Options{
		LocalNode: 1,
		Client:    client,
		RPCMux:    transport.NewRPCMux(),
		FetchService: fetchServiceFunc(func(ctx context.Context, req runtime.FetchRequestEnvelope) (runtime.FetchResponseEnvelope, error) {
			return runtime.FetchResponseEnvelope{}, nil
		}),
		MaxPendingFetchRPC: 2,
		RPCTimeout:         2 * time.Second,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	channelKey0 := channelKeyForShard(t, 0, 2)
	channelKey1 := channelKeyForShard(t, 1, 2)
	session := adapter.SessionManager().Session(2)

	errCh1 := make(chan error, 1)
	go func() {
		errCh1 <- session.Send(fetchRequestEnvelopeForTest(string(channelKey0)))
	}()

	errCh2 := make(chan error, 1)
	go func() {
		errCh2 <- session.Send(fetchRequestEnvelopeForTest(string(channelKey1)))
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
	server := transport.NewServer()
	mux := transport.NewRPCMux()
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	mux.Handle(RPCServiceFetch, func(ctx context.Context, body []byte) ([]byte, error) {
		started <- struct{}{}
		<-release
		return encodeFetchResponse(runtime.FetchResponseEnvelope{
			ChannelKey: "g-timeout",
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

	client := transport.NewClient(transport.NewPool(staticDiscovery{
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
	server := transport.NewServer()
	mux := transport.NewRPCMux()
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	mux.Handle(RPCServiceFetch, func(ctx context.Context, body []byte) ([]byte, error) {
		started <- struct{}{}
		<-release
		return encodeFetchResponse(runtime.FetchResponseEnvelope{
			ChannelKey: "g-stop",
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

	client := transport.NewClient(transport.NewPool(staticDiscovery{
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
		if !errors.Is(err, transport.ErrStopped) {
			t.Fatalf("session.Send() error = %v, want ErrStopped", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("expected client.Stop to abort pending fetch rpc")
	}

	if state := session.Backpressure(); state.Level != runtime.BackpressureNone {
		t.Fatalf("Backpressure().Level after stop = %v, want none", state.Level)
	}
}

func TestPeerSessionSendDispatchesProgressAckRPC(t *testing.T) {
	server := transport.NewServer()
	mux := transport.NewRPCMux()
	got := make(chan runtime.ProgressAckEnvelope, 1)
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

	client := transport.NewClient(transport.NewPool(staticDiscovery{
		addrs: map[uint64]string{2: server.Listener().Addr().String()},
	}, 1, time.Second))
	defer client.Stop()

	adapter := newAdapterWithTestTimeout(t, client, time.Second)
	session := adapter.SessionManager().Session(2)
	env := runtime.Envelope{
		Peer:       2,
		ChannelKey: "g-progress",
		Epoch:      3,
		Generation: 7,
		RequestID:  1,
		Kind:       runtime.MessageKindProgressAck,
		ProgressAck: &runtime.ProgressAckEnvelope{
			ChannelKey:  "g-progress",
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

func TestPeerSessionSendProgressAckAppliesLeaderHWFromRPCResponse(t *testing.T) {
	server := transport.NewServer()
	mux := transport.NewRPCMux()
	mux.Handle(RPCServiceProgressAck, func(ctx context.Context, body []byte) ([]byte, error) {
		ack, err := decodeProgressAck(body)
		if err != nil {
			return nil, err
		}
		return encodeProgressAckResponse(progressAckResponseEnvelope{LeaderHW: ack.MatchOffset})
	})
	server.HandleRPCMux(mux)
	if err := server.Start("127.0.0.1:0"); err != nil {
		t.Fatalf("server.Start() error = %v", err)
	}
	defer server.Stop()

	client := transport.NewClient(transport.NewPool(staticDiscovery{
		addrs: map[uint64]string{2: server.Listener().Addr().String()},
	}, 1, time.Second))
	defer client.Stop()

	adapter := newAdapterWithTestTimeout(t, client, time.Second)
	delivered := make(chan runtime.Envelope, 1)
	adapter.RegisterHandler(func(env runtime.Envelope) {
		delivered <- env
	})

	session := adapter.SessionManager().Session(2)
	env := runtime.Envelope{
		Peer:       2,
		ChannelKey: "g-progress",
		Epoch:      3,
		Generation: 7,
		RequestID:  1,
		Kind:       runtime.MessageKindProgressAck,
		ProgressAck: &runtime.ProgressAckEnvelope{
			ChannelKey:  "g-progress",
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
	case got := <-delivered:
		if got.Kind != runtime.MessageKindFetchResponse {
			t.Fatalf("Kind = %v, want fetch response", got.Kind)
		}
		if got.RequestID != 0 {
			t.Fatalf("RequestID = %d, want synthetic zero request id", got.RequestID)
		}
		if got.FetchResponse == nil || got.FetchResponse.LeaderHW != 11 {
			t.Fatalf("FetchResponse = %+v", got.FetchResponse)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for progress ack commit update")
	}
}

func TestAdapterHandleProgressAckInvokesRegisteredHandler(t *testing.T) {
	client := transport.NewClient(transport.NewPool(staticDiscovery{
		addrs: map[uint64]string{},
	}, 1, time.Second))
	defer client.Stop()

	mux := transport.NewRPCMux()
	adapter, err := New(Options{
		LocalNode: 1,
		Client:    client,
		RPCMux:    mux,
		FetchService: fetchServiceFunc(func(ctx context.Context, req runtime.FetchRequestEnvelope) (runtime.FetchResponseEnvelope, error) {
			return runtime.FetchResponseEnvelope{}, nil
		}),
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	delivered := make(chan runtime.Envelope, 1)
	adapter.RegisterHandler(func(env runtime.Envelope) {
		delivered <- env
	})

	body, err := encodeProgressAck(runtime.ProgressAckEnvelope{
		ChannelKey:  "g-progress",
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
		if env.Kind != runtime.MessageKindProgressAck {
			t.Fatalf("Kind = %v, want progress ack", env.Kind)
		}
		if env.ProgressAck == nil || env.ProgressAck.MatchOffset != 19 {
			t.Fatalf("ProgressAck = %+v", env.ProgressAck)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for delivered progress ack")
	}
}

func TestTransportLongPollModeRegistersLongPollRPC(t *testing.T) {
	client := transport.NewClient(transport.NewPool(staticDiscovery{
		addrs: map[uint64]string{},
	}, 1, time.Second))
	defer client.Stop()

	mux := transport.NewRPCMux()
	got := make(chan LongPollFetchRequest, 1)
	adapter, err := New(Options{
		LocalNode:       1,
		Client:          client,
		RPCMux:          mux,
		ReplicationMode: "long_poll",
		LongPollService: longPollServiceFunc(func(ctx context.Context, req LongPollFetchRequest) (LongPollFetchResponse, error) {
			got <- req
			return LongPollFetchResponse{
				Status:       LanePollStatusOK,
				SessionID:    req.SessionID,
				SessionEpoch: req.SessionEpoch,
				TimedOut:     true,
			}, nil
		}),
		FetchService: fetchServiceFunc(func(ctx context.Context, req runtime.FetchRequestEnvelope) (runtime.FetchResponseEnvelope, error) {
			return runtime.FetchResponseEnvelope{}, nil
		}),
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer adapter.Close()

	body, err := encodeLongPollFetchRequest(LongPollFetchRequest{
		PeerID:          2,
		LaneID:          4,
		LaneCount:       8,
		SessionID:       101,
		SessionEpoch:    6,
		Op:              LanePollOpOpen,
		ProtocolVersion: 1,
		Capabilities:    LongPollCapabilityQuorumAck | LongPollCapabilityLocalAck,
		MaxWaitMs:       1,
		MaxBytes:        64 * 1024,
		MaxChannels:     64,
		FullMembership: []LongPollMembership{
			{ChannelKey: "g1", ChannelEpoch: 11},
		},
	})
	if err != nil {
		t.Fatalf("encodeLongPollFetchRequest() error = %v", err)
	}

	respBody, err := mux.HandleRPC(context.Background(), append([]byte{RPCServiceLongPollFetch}, body...))
	if err != nil {
		t.Fatalf("HandleRPC() error = %v", err)
	}
	resp, err := decodeLongPollFetchResponse(respBody)
	if err != nil {
		t.Fatalf("decodeLongPollFetchResponse() error = %v", err)
	}
	if !resp.TimedOut || resp.Status != LanePollStatusOK {
		t.Fatalf("response = %+v, want timed out ok response", resp)
	}

	select {
	case req := <-got:
		if req.LaneID != 4 || req.SessionID != 101 {
			t.Fatalf("request = %+v, want lane=4 session=101", req)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for long poll request delivery")
	}
}

func TestTransportLongPollModeDoesNotRequireProgressAckRPC(t *testing.T) {
	client := transport.NewClient(transport.NewPool(staticDiscovery{
		addrs: map[uint64]string{},
	}, 1, time.Second))
	defer client.Stop()

	mux := transport.NewRPCMux()
	adapter, err := New(Options{
		LocalNode:       1,
		Client:          client,
		RPCMux:          mux,
		ReplicationMode: "long_poll",
		FetchService: fetchServiceFunc(func(ctx context.Context, req runtime.FetchRequestEnvelope) (runtime.FetchResponseEnvelope, error) {
			return runtime.FetchResponseEnvelope{}, nil
		}),
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer adapter.Close()

	body, err := encodeProgressAck(runtime.ProgressAckEnvelope{
		ChannelKey:  "g-progress",
		Epoch:       3,
		Generation:  7,
		ReplicaID:   2,
		MatchOffset: 19,
	})
	if err != nil {
		t.Fatalf("encodeProgressAck() error = %v", err)
	}

	_, err = mux.HandleRPC(context.Background(), append([]byte{RPCServiceProgressAck}, body...))
	if err == nil {
		t.Fatal("expected unknown progress ack service in long_poll mode")
	}
	if !strings.Contains(err.Error(), "unknown rpc service") {
		t.Fatalf("HandleRPC() error = %v, want unknown service", err)
	}
}

func TestPeerSessionTryBatchQueuesMultipleFetchRequests(t *testing.T) {
	client := transport.NewClient(transport.NewPool(staticDiscovery{
		addrs: map[uint64]string{},
	}, 1, time.Second))
	defer client.Stop()

	adapter, err := New(Options{
		LocalNode: 1,
		Client:    client,
		RPCMux:    transport.NewRPCMux(),
		FetchService: fetchServiceFunc(func(ctx context.Context, req runtime.FetchRequestEnvelope) (runtime.FetchResponseEnvelope, error) {
			return runtime.FetchResponseEnvelope{}, nil
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

func TestPeerSessionTryBatchFlushesAtCountThreshold(t *testing.T) {
	prevSchedule := scheduleFetchBatchFlush
	prevEager := runEagerFetchBatchFlush
	scheduleFetchBatchFlush = func(delay time.Duration, fn func()) {}
	runEagerFetchBatchFlush = func(fn func()) { fn() }
	t.Cleanup(func() {
		scheduleFetchBatchFlush = prevSchedule
		runEagerFetchBatchFlush = prevEager
	})

	server := transport.NewServer()
	mux := transport.NewRPCMux()

	batchStarted := make(chan int, 1)
	mux.Handle(RPCServiceFetchBatch, func(ctx context.Context, body []byte) ([]byte, error) {
		req, err := decodeFetchBatchRequest(body)
		if err != nil {
			return nil, err
		}
		select {
		case batchStarted <- len(req.Items):
		default:
		}

		resp := runtime.FetchBatchResponseEnvelope{
			Items: make([]runtime.FetchBatchResponseItem, 0, len(req.Items)),
		}
		for _, item := range req.Items {
			resp.Items = append(resp.Items, runtime.FetchBatchResponseItem{
				RequestID: item.RequestID,
				Response: &runtime.FetchResponseEnvelope{
					ChannelKey: item.Request.ChannelKey,
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

	client := transport.NewClient(transport.NewPool(staticDiscovery{
		addrs: map[uint64]string{2: server.Listener().Addr().String()},
	}, 1, time.Second))
	defer client.Stop()

	adapter := newAdapterWithTestTimeout(t, client, time.Second)
	session := adapter.SessionManager().Session(2).(*peerSession)

	for i := 0; i < 8; i++ {
		req := fetchRequestEnvelopeWithRequestIDForTest(fmt.Sprintf("g-threshold-%d", i), uint64(i+1))
		if !session.TryBatch(req) {
			t.Fatalf("TryBatch(%d) returned false", i)
		}
	}

	session.mu.Lock()
	queued := len(session.batchFetchQueue)
	session.mu.Unlock()
	if queued != 0 {
		t.Fatalf("queued batch fetch requests after count threshold = %d, want 0", queued)
	}

	select {
	case items := <-batchStarted:
		if items != 8 {
			t.Fatalf("batch item count = %d, want 8", items)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for eager batch flush at count threshold")
	}
}

func TestPeerSessionTryBatchFlushesAtByteThreshold(t *testing.T) {
	prevSchedule := scheduleFetchBatchFlush
	prevEager := runEagerFetchBatchFlush
	scheduleFetchBatchFlush = func(delay time.Duration, fn func()) {}
	runEagerFetchBatchFlush = func(fn func()) { fn() }
	t.Cleanup(func() {
		scheduleFetchBatchFlush = prevSchedule
		runEagerFetchBatchFlush = prevEager
	})

	server := transport.NewServer()
	mux := transport.NewRPCMux()

	batchStarted := make(chan int, 1)
	mux.Handle(RPCServiceFetchBatch, func(ctx context.Context, body []byte) ([]byte, error) {
		req, err := decodeFetchBatchRequest(body)
		if err != nil {
			return nil, err
		}
		select {
		case batchStarted <- len(req.Items):
		default:
		}

		resp := runtime.FetchBatchResponseEnvelope{
			Items: make([]runtime.FetchBatchResponseItem, 0, len(req.Items)),
		}
		for _, item := range req.Items {
			resp.Items = append(resp.Items, runtime.FetchBatchResponseItem{
				RequestID: item.RequestID,
				Response: &runtime.FetchResponseEnvelope{
					ChannelKey: item.Request.ChannelKey,
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

	client := transport.NewClient(transport.NewPool(staticDiscovery{
		addrs: map[uint64]string{2: server.Listener().Addr().String()},
	}, 1, time.Second))
	defer client.Stop()

	adapter := newAdapterWithTestTimeout(t, client, time.Second)
	session := adapter.SessionManager().Session(2).(*peerSession)

	largeKey := "g-byte-threshold-" + strings.Repeat("x", 33*1024)
	if !session.TryBatch(fetchRequestEnvelopeWithRequestIDForTest(largeKey, 1)) {
		t.Fatal("expected large fetch request to be queued for batching")
	}

	session.mu.Lock()
	queued := len(session.batchFetchQueue)
	session.mu.Unlock()
	if queued != 0 {
		t.Fatalf("queued batch fetch requests after byte threshold = %d, want 0", queued)
	}

	select {
	case items := <-batchStarted:
		if items != 1 {
			t.Fatalf("batch item count = %d, want 1", items)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for eager batch flush at byte threshold")
	}
}

func TestPeerSessionStaleFlushCallbacksDoNotDrainLaterBatchEarly(t *testing.T) {
	prevSchedule := scheduleFetchBatchFlush
	prevEager := runEagerFetchBatchFlush
	var scheduled []func()
	scheduleFetchBatchFlush = func(delay time.Duration, fn func()) {
		scheduled = append(scheduled, fn)
	}
	runEagerFetchBatchFlush = func(fn func()) { fn() }
	t.Cleanup(func() {
		scheduleFetchBatchFlush = prevSchedule
		runEagerFetchBatchFlush = prevEager
	})

	server := transport.NewServer()
	mux := transport.NewRPCMux()

	batchCalls := make(chan []uint64, 2)
	mux.Handle(RPCServiceFetchBatch, func(ctx context.Context, body []byte) ([]byte, error) {
		req, err := decodeFetchBatchRequest(body)
		if err != nil {
			return nil, err
		}
		offsets := make([]uint64, 0, len(req.Items))
		resp := runtime.FetchBatchResponseEnvelope{
			Items: make([]runtime.FetchBatchResponseItem, 0, len(req.Items)),
		}
		for _, item := range req.Items {
			offsets = append(offsets, item.Request.FetchOffset)
			resp.Items = append(resp.Items, runtime.FetchBatchResponseItem{
				RequestID: item.RequestID,
				Response: &runtime.FetchResponseEnvelope{
					ChannelKey: item.Request.ChannelKey,
					Epoch:      item.Request.Epoch,
					Generation: item.Request.Generation,
					LeaderHW:   item.Request.FetchOffset,
				},
			})
		}
		batchCalls <- offsets
		return encodeFetchBatchResponse(resp)
	})
	server.HandleRPCMux(mux)
	if err := server.Start("127.0.0.1:0"); err != nil {
		t.Fatalf("server.Start() error = %v", err)
	}
	defer server.Stop()

	client := transport.NewClient(transport.NewPool(staticDiscovery{
		addrs: map[uint64]string{2: server.Listener().Addr().String()},
	}, 1, time.Second))
	defer client.Stop()

	adapter := newAdapterWithTestTimeout(t, client, time.Second)
	session := adapter.SessionManager().Session(2).(*peerSession)

	for i := 0; i < 8; i++ {
		if !session.TryBatch(fetchRequestEnvelopeWithOffsetForTest(fmt.Sprintf("g-stale-%d", i), uint64(i+1), uint64(i+1))) {
			t.Fatalf("TryBatch(%d) returned false", i)
		}
	}
	if len(scheduled) != 1 {
		t.Fatalf("scheduled callbacks after eager flush = %d, want 1", len(scheduled))
	}

	select {
	case got := <-batchCalls:
		if fmt.Sprint(got) != fmt.Sprint([]uint64{1, 2, 3, 4, 5, 6, 7, 8}) {
			t.Fatalf("first eager batch offsets = %v, want [1 2 3 4 5 6 7 8]", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for eager flush batch")
	}

	req9 := fetchRequestEnvelopeWithOffsetForTest("g-stale-next-1", 9, 9)
	req10 := fetchRequestEnvelopeWithOffsetForTest("g-stale-next-2", 10, 10)
	if !session.TryBatch(req9) {
		t.Fatal("expected next batch first request to queue")
	}
	if !session.TryBatch(req10) {
		t.Fatal("expected next batch second request to queue")
	}
	if len(scheduled) != 2 {
		t.Fatalf("scheduled callbacks after second batch = %d, want 2", len(scheduled))
	}

	// Fire the stale timer from the first batch; it must not drain the later queue.
	scheduled[0]()

	session.mu.Lock()
	queued := len(session.batchFetchQueue)
	session.mu.Unlock()
	if queued != 2 {
		t.Fatalf("queued requests after stale callback = %d, want 2", queued)
	}

	select {
	case got := <-batchCalls:
		t.Fatalf("stale callback unexpectedly flushed later batch: %v", got)
	default:
	}

	scheduled[1]()

	select {
	case got := <-batchCalls:
		if fmt.Sprint(got) != fmt.Sprint([]uint64{9, 10}) {
			t.Fatalf("second batch offsets = %v, want [9 10]", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for fresh timer batch")
	}
}

func TestPeerSessionFlushSendsSingleFetchBatchRPC(t *testing.T) {
	server := transport.NewServer()
	mux := transport.NewRPCMux()

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
		return encodeFetchResponse(runtime.FetchResponseEnvelope{
			ChannelKey: "g-single",
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

		resp := runtime.FetchBatchResponseEnvelope{
			Items: make([]runtime.FetchBatchResponseItem, 0, len(req.Items)),
		}
		for _, item := range req.Items {
			resp.Items = append(resp.Items, runtime.FetchBatchResponseItem{
				RequestID: item.RequestID,
				Response: &runtime.FetchResponseEnvelope{
					ChannelKey: item.Request.ChannelKey,
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

	client := transport.NewClient(transport.NewPool(staticDiscovery{
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

func TestPeerSessionAutoFlushWindowSendsSingleFetchBatchRPC(t *testing.T) {
	server := transport.NewServer()
	mux := transport.NewRPCMux()

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
		return encodeFetchResponse(runtime.FetchResponseEnvelope{
			ChannelKey: "g-single",
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

		resp := runtime.FetchBatchResponseEnvelope{
			Items: make([]runtime.FetchBatchResponseItem, 0, len(req.Items)),
		}
		for _, item := range req.Items {
			resp.Items = append(resp.Items, runtime.FetchBatchResponseItem{
				RequestID: item.RequestID,
				Response: &runtime.FetchResponseEnvelope{
					ChannelKey: item.Request.ChannelKey,
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

	client := transport.NewClient(transport.NewPool(staticDiscovery{
		addrs: map[uint64]string{2: server.Listener().Addr().String()},
	}, 1, time.Second))
	defer client.Stop()

	adapter := newAdapterWithTestTimeout(t, client, time.Second)
	session := adapter.SessionManager().Session(2)
	if !session.TryBatch(fetchRequestEnvelopeForTest("g-auto-batch-1")) {
		t.Fatal("expected first fetch request to be queued for batching")
	}
	if !session.TryBatch(fetchRequestEnvelopeForTest("g-auto-batch-2")) {
		t.Fatal("expected second fetch request to be queued for batching")
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		gotBatchCalls := batchCalls
		gotSingleCalls := singleCalls
		gotBatchItems := batchItems
		mu.Unlock()

		if gotBatchCalls == 1 {
			if gotSingleCalls != 0 {
				t.Fatalf("single fetch rpc calls = %d, want 0", gotSingleCalls)
			}
			if gotBatchItems != 2 {
				t.Fatalf("batch item count = %d, want 2", gotBatchItems)
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}

	mu.Lock()
	defer mu.Unlock()
	t.Fatalf("auto flush did not send expected batch rpc: batchCalls=%d singleCalls=%d batchItems=%d", batchCalls, singleCalls, batchItems)
}

func TestBatchedFetchResponseFansOutResultsPerGroup(t *testing.T) {
	server := transport.NewServer()
	mux := transport.NewRPCMux()
	mux.Handle(RPCServiceFetchBatch, func(ctx context.Context, body []byte) ([]byte, error) {
		req, err := decodeFetchBatchRequest(body)
		if err != nil {
			return nil, err
		}
		resp := runtime.FetchBatchResponseEnvelope{
			Items: make([]runtime.FetchBatchResponseItem, 0, len(req.Items)),
		}
		for _, item := range req.Items {
			resp.Items = append(resp.Items, runtime.FetchBatchResponseItem{
				RequestID: item.RequestID,
				Response: &runtime.FetchResponseEnvelope{
					ChannelKey: item.Request.ChannelKey,
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

	client := transport.NewClient(transport.NewPool(staticDiscovery{
		addrs: map[uint64]string{2: server.Listener().Addr().String()},
	}, 1, time.Second))
	defer client.Stop()

	adapter := newAdapterWithTestTimeout(t, client, time.Second)
	delivered := make(chan runtime.Envelope, 2)
	adapter.RegisterHandler(func(env runtime.Envelope) {
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

	got := make(map[channel.ChannelKey]runtime.Envelope, 2)
	for i := 0; i < 2; i++ {
		select {
		case env := <-delivered:
			got[env.ChannelKey] = env
		case <-time.After(2 * time.Second):
			t.Fatalf("delivered envelopes = %d, want 2", len(got))
		}
	}

	env1, ok := got[channel.ChannelKey("g-fanout-1")]
	if !ok {
		t.Fatal("missing fanout response for g-fanout-1")
	}
	if env1.Kind != runtime.MessageKindFetchResponse {
		t.Fatalf("g-fanout-1 kind = %v, want fetch response", env1.Kind)
	}
	if env1.RequestID != 101 {
		t.Fatalf("g-fanout-1 request id = %d, want 101", env1.RequestID)
	}
	if env1.FetchResponse == nil || env1.FetchResponse.LeaderHW != 111 {
		t.Fatalf("g-fanout-1 response = %+v, want LeaderHW=111", env1.FetchResponse)
	}

	env2, ok := got[channel.ChannelKey("g-fanout-2")]
	if !ok {
		t.Fatal("missing fanout response for g-fanout-2")
	}
	if env2.Kind != runtime.MessageKindFetchResponse {
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
	client := transport.NewClient(transport.NewPool(staticDiscovery{
		addrs: map[uint64]string{},
	}, 1, time.Second))
	defer client.Stop()

	adapter := newAdapterWithTestTimeout(t, client, 20*time.Millisecond)
	delivered := make(chan runtime.Envelope, 1)
	adapter.RegisterHandler(func(env runtime.Envelope) {
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
		if env.Kind != runtime.MessageKindFetchFailure {
			t.Fatalf("kind = %v, want fetch failure", env.Kind)
		}
		if env.RequestID != 99 || env.ChannelKey != channel.ChannelKey("g-batch-fail") {
			t.Fatalf("failure envelope = %+v", env)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for fetch failure envelope")
	}
}

func TestPeerSessionFlushBatchFailureRetriesSubBatchesBeforeSingles(t *testing.T) {
	prevSchedule := scheduleFetchBatchFlush
	prevEager := runEagerFetchBatchFlush
	scheduleFetchBatchFlush = func(delay time.Duration, fn func()) {}
	runEagerFetchBatchFlush = func(fn func()) {}
	t.Cleanup(func() {
		scheduleFetchBatchFlush = prevSchedule
		runEagerFetchBatchFlush = prevEager
	})

	server := transport.NewServer()
	mux := transport.NewRPCMux()

	var (
		mu          sync.Mutex
		batchSizes  []int
		singleCalls int
	)

	mux.Handle(RPCServiceFetchBatch, func(ctx context.Context, body []byte) ([]byte, error) {
		req, err := decodeFetchBatchRequest(body)
		if err != nil {
			return nil, err
		}

		mu.Lock()
		batchSizes = append(batchSizes, len(req.Items))
		mu.Unlock()

		if len(req.Items) > 4 {
			return nil, errors.New("batch too large")
		}

		resp := runtime.FetchBatchResponseEnvelope{
			Items: make([]runtime.FetchBatchResponseItem, 0, len(req.Items)),
		}
		for _, item := range req.Items {
			resp.Items = append(resp.Items, runtime.FetchBatchResponseItem{
				RequestID: item.RequestID,
				Response: &runtime.FetchResponseEnvelope{
					ChannelKey: item.Request.ChannelKey,
					Epoch:      item.Request.Epoch,
					Generation: item.Request.Generation,
					LeaderHW:   item.Request.FetchOffset,
				},
			})
		}
		return encodeFetchBatchResponse(resp)
	})
	mux.Handle(RPCServiceFetch, func(ctx context.Context, body []byte) ([]byte, error) {
		mu.Lock()
		singleCalls++
		mu.Unlock()
		return encodeFetchResponse(runtime.FetchResponseEnvelope{
			ChannelKey: "g-fallback-single",
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

	client := transport.NewClient(transport.NewPool(staticDiscovery{
		addrs: map[uint64]string{2: server.Listener().Addr().String()},
	}, 1, time.Second))
	defer client.Stop()

	adapter := newAdapterWithTestTimeout(t, client, time.Second)
	session := adapter.SessionManager().Session(2)
	for i := 0; i < 8; i++ {
		req := fetchRequestEnvelopeWithRequestIDForTest(fmt.Sprintf("g-fallback-%d", i), uint64(i+1))
		if !session.TryBatch(req) {
			t.Fatalf("TryBatch(%d) returned false", i)
		}
	}

	if err := session.Flush(); err != nil {
		t.Fatalf("session.Flush() error = %v", err)
	}

	mu.Lock()
	gotBatchSizes := append([]int(nil), batchSizes...)
	gotSingleCalls := singleCalls
	mu.Unlock()

	if gotSingleCalls != 0 {
		t.Fatalf("single fetch rpc calls = %d, want 0", gotSingleCalls)
	}
	if fmt.Sprint(gotBatchSizes) != fmt.Sprint([]int{8, 4, 4}) {
		t.Fatalf("batch sizes = %v, want [8 4 4]", gotBatchSizes)
	}
}

func TestPeerSessionMissingBatchItemsRetrySubBatchesBeforeSingles(t *testing.T) {
	prevSchedule := scheduleFetchBatchFlush
	prevEager := runEagerFetchBatchFlush
	scheduleFetchBatchFlush = func(delay time.Duration, fn func()) {}
	runEagerFetchBatchFlush = func(fn func()) {}
	t.Cleanup(func() {
		scheduleFetchBatchFlush = prevSchedule
		runEagerFetchBatchFlush = prevEager
	})

	server := transport.NewServer()
	mux := transport.NewRPCMux()

	var (
		mu          sync.Mutex
		batchSizes  []int
		singleCalls int
	)

	mux.Handle(RPCServiceFetchBatch, func(ctx context.Context, body []byte) ([]byte, error) {
		req, err := decodeFetchBatchRequest(body)
		if err != nil {
			return nil, err
		}

		mu.Lock()
		batchSizes = append(batchSizes, len(req.Items))
		mu.Unlock()

		resp := runtime.FetchBatchResponseEnvelope{
			Items: make([]runtime.FetchBatchResponseItem, 0, len(req.Items)),
		}
		if len(req.Items) == 8 {
			for _, item := range req.Items[:4] {
				resp.Items = append(resp.Items, runtime.FetchBatchResponseItem{
					RequestID: item.RequestID,
					Response: &runtime.FetchResponseEnvelope{
						ChannelKey: item.Request.ChannelKey,
						Epoch:      item.Request.Epoch,
						Generation: item.Request.Generation,
						LeaderHW:   item.Request.FetchOffset,
					},
				})
			}
			return encodeFetchBatchResponse(resp)
		}

		for _, item := range req.Items {
			resp.Items = append(resp.Items, runtime.FetchBatchResponseItem{
				RequestID: item.RequestID,
				Response: &runtime.FetchResponseEnvelope{
					ChannelKey: item.Request.ChannelKey,
					Epoch:      item.Request.Epoch,
					Generation: item.Request.Generation,
					LeaderHW:   item.Request.FetchOffset,
				},
			})
		}
		return encodeFetchBatchResponse(resp)
	})
	mux.Handle(RPCServiceFetch, func(ctx context.Context, body []byte) ([]byte, error) {
		mu.Lock()
		singleCalls++
		mu.Unlock()
		return encodeFetchResponse(runtime.FetchResponseEnvelope{
			ChannelKey: "g-missing-single",
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

	client := transport.NewClient(transport.NewPool(staticDiscovery{
		addrs: map[uint64]string{2: server.Listener().Addr().String()},
	}, 1, time.Second))
	defer client.Stop()

	adapter := newAdapterWithTestTimeout(t, client, time.Second)
	session := adapter.SessionManager().Session(2)
	for i := 0; i < 8; i++ {
		req := fetchRequestEnvelopeWithRequestIDForTest(fmt.Sprintf("g-missing-%d", i), uint64(i+1))
		if !session.TryBatch(req) {
			t.Fatalf("TryBatch(%d) returned false", i)
		}
	}

	if err := session.Flush(); err != nil {
		t.Fatalf("session.Flush() error = %v", err)
	}

	mu.Lock()
	gotBatchSizes := append([]int(nil), batchSizes...)
	gotSingleCalls := singleCalls
	mu.Unlock()

	if gotSingleCalls != 0 {
		t.Fatalf("single fetch rpc calls = %d, want 0", gotSingleCalls)
	}
	if fmt.Sprint(gotBatchSizes) != fmt.Sprint([]int{8, 4}) {
		t.Fatalf("batch sizes = %v, want [8 4]", gotBatchSizes)
	}
}

func TestPeerSessionChunkFailureFallsBackOnlyAffectedRequestsToSingles(t *testing.T) {
	prevSchedule := scheduleFetchBatchFlush
	prevEager := runEagerFetchBatchFlush
	scheduleFetchBatchFlush = func(delay time.Duration, fn func()) {}
	runEagerFetchBatchFlush = func(fn func()) {}
	t.Cleanup(func() {
		scheduleFetchBatchFlush = prevSchedule
		runEagerFetchBatchFlush = prevEager
	})

	server := transport.NewServer()
	mux := transport.NewRPCMux()

	var (
		mu            sync.Mutex
		batchOffsets  [][]uint64
		singleOffsets []uint64
	)

	mux.Handle(RPCServiceFetchBatch, func(ctx context.Context, body []byte) ([]byte, error) {
		req, err := decodeFetchBatchRequest(body)
		if err != nil {
			return nil, err
		}

		offsets := make([]uint64, 0, len(req.Items))
		for _, item := range req.Items {
			offsets = append(offsets, item.Request.FetchOffset)
		}

		mu.Lock()
		batchOffsets = append(batchOffsets, offsets)
		mu.Unlock()

		if len(offsets) > 4 {
			return nil, errors.New("root batch failed")
		}
		if fmt.Sprint(offsets) == fmt.Sprint([]uint64{5, 6, 7, 8}) {
			return nil, errors.New("chunk failed")
		}

		resp := runtime.FetchBatchResponseEnvelope{
			Items: make([]runtime.FetchBatchResponseItem, 0, len(req.Items)),
		}
		for _, item := range req.Items {
			resp.Items = append(resp.Items, runtime.FetchBatchResponseItem{
				RequestID: item.RequestID,
				Response: &runtime.FetchResponseEnvelope{
					ChannelKey: item.Request.ChannelKey,
					Epoch:      item.Request.Epoch,
					Generation: item.Request.Generation,
					LeaderHW:   item.Request.FetchOffset,
				},
			})
		}
		return encodeFetchBatchResponse(resp)
	})
	mux.Handle(RPCServiceFetch, func(ctx context.Context, body []byte) ([]byte, error) {
		req, err := decodeFetchRequest(body)
		if err != nil {
			return nil, err
		}

		mu.Lock()
		singleOffsets = append(singleOffsets, req.FetchOffset)
		mu.Unlock()

		return encodeFetchResponse(runtime.FetchResponseEnvelope{
			ChannelKey: req.ChannelKey,
			Epoch:      req.Epoch,
			Generation: req.Generation,
			LeaderHW:   req.FetchOffset,
		})
	})
	server.HandleRPCMux(mux)
	if err := server.Start("127.0.0.1:0"); err != nil {
		t.Fatalf("server.Start() error = %v", err)
	}
	defer server.Stop()

	client := transport.NewClient(transport.NewPool(staticDiscovery{
		addrs: map[uint64]string{2: server.Listener().Addr().String()},
	}, 1, time.Second))
	defer client.Stop()

	adapter := newAdapterWithTestTimeout(t, client, time.Second)
	session := adapter.SessionManager().Session(2)
	for i := 0; i < 8; i++ {
		req := fetchRequestEnvelopeWithOffsetForTest(fmt.Sprintf("g-chunk-fail-%d", i), uint64(i+1), uint64(i+1))
		if !session.TryBatch(req) {
			t.Fatalf("TryBatch(%d) returned false", i)
		}
	}

	if err := session.Flush(); err != nil {
		t.Fatalf("session.Flush() error = %v", err)
	}

	mu.Lock()
	gotBatchOffsets := append([][]uint64(nil), batchOffsets...)
	gotSingleOffsets := append([]uint64(nil), singleOffsets...)
	mu.Unlock()

	if fmt.Sprint(gotBatchOffsets) != fmt.Sprint([][]uint64{{1, 2, 3, 4, 5, 6, 7, 8}, {1, 2, 3, 4}, {5, 6, 7, 8}}) {
		t.Fatalf("batch offsets = %v", gotBatchOffsets)
	}
	if fmt.Sprint(gotSingleOffsets) != fmt.Sprint([]uint64{5, 6, 7, 8}) {
		t.Fatalf("single offsets = %v, want [5 6 7 8]", gotSingleOffsets)
	}
}

func TestPeerSessionReconcileProbeSharesFetchShardConnection(t *testing.T) {
	server := transport.NewServer()
	mux := transport.NewRPCMux()
	mux.Handle(RPCServiceFetch, func(ctx context.Context, body []byte) ([]byte, error) {
		req, err := decodeFetchRequest(body)
		if err != nil {
			return nil, err
		}
		return encodeFetchResponse(runtime.FetchResponseEnvelope{
			ChannelKey: req.ChannelKey,
			Epoch:      req.Epoch,
			Generation: req.Generation,
			LeaderHW:   req.FetchOffset,
		})
	})
	mux.Handle(RPCServiceReconcileProbe, func(ctx context.Context, body []byte) ([]byte, error) {
		req, err := decodeReconcileProbeRequest(body)
		if err != nil {
			return nil, err
		}
		return encodeReconcileProbeResponse(runtime.ReconcileProbeResponseEnvelope{
			ChannelKey:   req.ChannelKey,
			Epoch:        req.Epoch,
			Generation:   req.Generation,
			ReplicaID:    req.ReplicaID,
			LogEndOffset: 11,
		})
	})
	server.HandleRPCMux(mux)
	if err := server.Start("127.0.0.1:0"); err != nil {
		t.Fatalf("server.Start() error = %v", err)
	}
	defer server.Stop()

	pool := transport.NewPool(staticDiscovery{
		addrs: map[uint64]string{2: server.Listener().Addr().String()},
	}, 2, time.Second)
	client := transport.NewClient(pool)
	defer client.Stop()

	adapter := newAdapterWithTestTimeout(t, client, time.Second)
	session := adapter.SessionManager().Session(2)
	channelKey := channelKeyForShard(t, 1, 2)

	if err := session.Send(fetchRequestEnvelopeForTest(string(channelKey))); err != nil {
		t.Fatalf("session.Send(fetch) error = %v", err)
	}
	if err := session.Send(runtime.Envelope{
		Peer:       2,
		ChannelKey: channelKey,
		Epoch:      3,
		Generation: 7,
		RequestID:  2,
		Kind:       runtime.MessageKindReconcileProbeRequest,
		ReconcileProbeRequest: &runtime.ReconcileProbeRequestEnvelope{
			ChannelKey: channelKey,
			Epoch:      3,
			Generation: 7,
			ReplicaID:  1,
		},
	}); err != nil {
		t.Fatalf("session.Send(reconcile probe) error = %v", err)
	}

	stats := pool.Stats()
	if len(stats) != 1 {
		t.Fatalf("pool stats peers = %d, want 1", len(stats))
	}
	if stats[0].Active != 1 {
		t.Fatalf("pool active connections = %d, want 1", stats[0].Active)
	}
}

func TestPeerSessionBackpressureRemainsHardAtQueuedLimitAfterEagerFlush(t *testing.T) {
	prevSchedule := scheduleFetchBatchFlush
	prevEager := runEagerFetchBatchFlush
	scheduleFetchBatchFlush = func(delay time.Duration, fn func()) {}
	eagerDrained := make(chan struct{}, 1)
	blockLaterEager := make(chan struct{})
	var eagerCalls atomic.Int32
	runEagerFetchBatchFlush = func(fn func()) {
		go func() {
			if eagerCalls.Add(1) > 1 {
				<-blockLaterEager
			}
			fn()
			eagerDrained <- struct{}{}
		}()
	}
	t.Cleanup(func() {
		close(blockLaterEager)
		scheduleFetchBatchFlush = prevSchedule
		runEagerFetchBatchFlush = prevEager
	})

	client := transport.NewClient(transport.NewPool(staticDiscovery{
		addrs: map[uint64]string{},
	}, 1, time.Second))
	defer client.Stop()

	adapter := newAdapterWithTestTimeout(t, client, time.Second)
	session := adapter.SessionManager().Session(2).(*peerSession)

	for i := 0; i < 8; i++ {
		req := fetchRequestEnvelopeWithOffsetForTest(fmt.Sprintf("g-eager-hard-%d", i), uint64(i+1), uint64(i+1))
		if !session.TryBatch(req) {
			t.Fatalf("TryBatch(%d) returned false", i)
		}
	}

	select {
	case <-eagerDrained:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for eager flush drain")
	}

	session.mu.Lock()
	firstQueueDepth := len(session.batchFetchQueue)
	session.mu.Unlock()
	if firstQueueDepth != 0 {
		t.Fatalf("queue depth after eager flush = %d, want 0", firstQueueDepth)
	}

	for i := 0; i < session.maxQueuedFetchRequests(); i++ {
		req := fetchRequestEnvelopeWithOffsetForTest(fmt.Sprintf("g-eager-hard-next-%d", i), uint64(100+i), uint64(100+i))
		if !session.TryBatch(req) {
			t.Fatalf("TryBatch(next %d) returned false", i)
		}
	}

	state := session.Backpressure()
	if state.Level != runtime.BackpressureHard {
		t.Fatalf("Backpressure().Level = %v, want hard", state.Level)
	}
	if state.PendingRequests < session.maxQueuedFetchRequests() {
		t.Fatalf("PendingRequests = %d, want at least %d", state.PendingRequests, session.maxQueuedFetchRequests())
	}
}

func TestPeerSessionBackpressureIncludesQueuedBatchRequests(t *testing.T) {
	prevSchedule := scheduleFetchBatchFlush
	prevEager := runEagerFetchBatchFlush
	scheduleFetchBatchFlush = func(delay time.Duration, fn func()) {}
	runEagerFetchBatchFlush = func(fn func()) {}
	t.Cleanup(func() {
		scheduleFetchBatchFlush = prevSchedule
		runEagerFetchBatchFlush = prevEager
	})

	client := transport.NewClient(transport.NewPool(staticDiscovery{
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
	if state.Level != runtime.BackpressureHard {
		t.Fatalf("Backpressure().Level = %v, want hard", state.Level)
	}
}

type staticDiscovery struct {
	addrs map[uint64]string
}

func (d staticDiscovery) Resolve(nodeID uint64) (string, error) {
	addr, ok := d.addrs[nodeID]
	if !ok {
		return "", transport.ErrNodeNotFound
	}
	return addr, nil
}

type fetchServiceFunc func(context.Context, runtime.FetchRequestEnvelope) (runtime.FetchResponseEnvelope, error)

func (f fetchServiceFunc) ServeFetch(ctx context.Context, req runtime.FetchRequestEnvelope) (runtime.FetchResponseEnvelope, error) {
	return f(ctx, req)
}

type longPollServiceFunc func(context.Context, LongPollFetchRequest) (LongPollFetchResponse, error)

func (f longPollServiceFunc) ServeLongPollFetch(ctx context.Context, req LongPollFetchRequest) (LongPollFetchResponse, error) {
	return f(ctx, req)
}

func newAdapterWithTestTimeout(t *testing.T, client *transport.Client, timeout time.Duration) *Transport {
	t.Helper()

	opts := Options{
		LocalNode:  1,
		Client:     client,
		RPCMux:     transport.NewRPCMux(),
		RPCTimeout: timeout,
		FetchService: fetchServiceFunc(func(ctx context.Context, req runtime.FetchRequestEnvelope) (runtime.FetchResponseEnvelope, error) {
			return runtime.FetchResponseEnvelope{}, nil
		}),
	}

	adapter, err := New(opts)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	return adapter
}

func fetchRequestEnvelopeForTest(channelKey string) runtime.Envelope {
	return fetchRequestEnvelopeWithRequestIDForTest(channelKey, 1)
}

func fetchRequestEnvelopeWithRequestIDForTest(channelKey string, requestID uint64) runtime.Envelope {
	return fetchRequestEnvelopeWithOffsetForTest(channelKey, requestID, 11)
}

func fetchRequestEnvelopeWithOffsetForTest(channelKey string, requestID uint64, fetchOffset uint64) runtime.Envelope {
	key := channel.ChannelKey(channelKey)
	return runtime.Envelope{
		Peer:       2,
		ChannelKey: key,
		Epoch:      3,
		Generation: 7,
		RequestID:  requestID,
		Kind:       runtime.MessageKindFetchRequest,
		FetchRequest: &runtime.FetchRequestEnvelope{
			ChannelKey:  key,
			Epoch:       3,
			Generation:  7,
			ReplicaID:   1,
			FetchOffset: fetchOffset,
			OffsetEpoch: 3,
			MaxBytes:    4096,
		},
	}
}

func channelKeyForShard(t *testing.T, shard int, poolSize int) channel.ChannelKey {
	t.Helper()

	for i := 0; i < 1024; i++ {
		key := channel.ChannelKey(fmt.Sprintf("g-shard-%d", i))
		if fetchShardForTest(key, poolSize) == shard {
			return key
		}
	}
	t.Fatalf("no group key found for shard %d", shard)
	return ""
}

func fetchShardForTest(channelKey channel.ChannelKey, poolSize int) int {
	hasher := fnv.New64a()
	_, _ = hasher.Write([]byte(channelKey))
	return int(hasher.Sum64() % uint64(poolSize))
}

func encodeRPCResponseForTest(requestID uint64, data []byte) []byte {
	buf := make([]byte, 9+len(data))
	binary.BigEndian.PutUint64(buf[:8], requestID)
	copy(buf[9:], data)
	return buf
}
