package transport

import (
	"context"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/channel/runtime"
	baseTransport "github.com/WuKongIM/WuKongIM/pkg/transport"
)

func TestLongPollIntegrationPeerSessionDeliversLanePollResponse(t *testing.T) {
	server := baseTransport.NewServer()
	mux := baseTransport.NewRPCMux()
	server.HandleRPCMux(mux)
	if err := server.Start("127.0.0.1:0"); err != nil {
		t.Fatalf("server.Start() error = %v", err)
	}
	defer server.Stop()

	got := make(chan LongPollFetchRequest, 1)
	mux.Handle(RPCServiceLongPollFetch, func(ctx context.Context, body []byte) ([]byte, error) {
		req, err := decodeLongPollFetchRequest(body)
		if err != nil {
			return nil, err
		}
		got <- req
		return encodeLongPollFetchResponse(LongPollFetchResponse{
			Status:       LanePollStatusOK,
			SessionID:    701,
			SessionEpoch: 2,
			TimedOut:     true,
		})
	})

	client := baseTransport.NewClient(baseTransport.NewPool(staticDiscovery{
		addrs: map[uint64]string{2: server.Listener().Addr().String()},
	}, 1, time.Second))
	defer client.Stop()

	adapter, err := New(Options{
		LocalNode:       1,
		Client:          client,
		RPCMux:          baseTransport.NewRPCMux(),
		ReplicationMode: "long_poll",
		FetchService: fetchServiceFunc(func(ctx context.Context, req runtime.FetchRequestEnvelope) (runtime.FetchResponseEnvelope, error) {
			return runtime.FetchResponseEnvelope{}, nil
		}),
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer adapter.Close()

	delivered := make(chan runtime.Envelope, 1)
	adapter.RegisterHandler(func(env runtime.Envelope) {
		delivered <- env
	})

	err = adapter.SessionManager().Session(2).Send(runtime.Envelope{
		Peer: 2,
		Kind: runtime.MessageKindLanePollRequest,
		LanePollRequest: &runtime.LanePollRequestEnvelope{
			LaneID:          4,
			LaneCount:       8,
			Op:              runtime.LanePollOpOpen,
			ProtocolVersion: 1,
			MaxWait:         time.Millisecond,
			MaxBytes:        64 * 1024,
			MaxChannels:     64,
			FullMembership: []runtime.LaneMembership{
				{ChannelKey: "g1", ChannelEpoch: 11},
			},
		},
	})
	if err != nil {
		t.Fatalf("session.Send() error = %v", err)
	}

	select {
	case req := <-got:
		if req.LaneID != 4 || req.Op != LanePollOpOpen {
			t.Fatalf("request = %+v, want open lane=4", req)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for outbound long poll request")
	}

	select {
	case env := <-delivered:
		if env.Kind != runtime.MessageKindLanePollResponse {
			t.Fatalf("response kind = %v, want lane poll response", env.Kind)
		}
		if env.LanePollResponse == nil || !env.LanePollResponse.TimedOut {
			t.Fatalf("lane poll response = %+v, want timed out response", env.LanePollResponse)
		}
		if env.LanePollResponse.SessionID != 701 || env.LanePollResponse.SessionEpoch != 2 {
			t.Fatalf("lane poll response = %+v, want session 701/2", env.LanePollResponse)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for delivered lane poll response")
	}
}
