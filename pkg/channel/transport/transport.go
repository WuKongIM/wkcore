package transport

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/channel"
	"github.com/WuKongIM/WuKongIM/pkg/channel/runtime"
	wktransport "github.com/WuKongIM/WuKongIM/pkg/transport"
)

const defaultRPCTimeout = 5 * time.Second

type Options struct {
	LocalNode          channel.NodeID
	Client             *wktransport.Client
	RPCMux             *wktransport.RPCMux
	FetchService       runtime.FetchService
	RPCTimeout         time.Duration
	MaxPendingFetchRPC int
}

type Transport struct {
	localNode  channel.NodeID
	client     *wktransport.Client
	rpcMux     *wktransport.RPCMux
	rpcTimeout time.Duration
	maxPending int

	mu           sync.RWMutex
	handler      func(runtime.Envelope)
	fetchService runtime.FetchService
	statusSource channel.HandlerRuntime
	closeOnce    sync.Once

	sessions *sessionManager
}

var _ runtime.Transport = (*Transport)(nil)
var _ runtime.PeerSessionManager = (*Transport)(nil)

func New(opts Options) (*Transport, error) {
	if opts.LocalNode == 0 {
		return nil, fmt.Errorf("channeltransport: local node must be set")
	}
	if opts.Client == nil {
		return nil, fmt.Errorf("channeltransport: client must be set")
	}
	if opts.RPCMux == nil {
		return nil, fmt.Errorf("channeltransport: rpc mux must be set")
	}
	if opts.RPCTimeout <= 0 {
		opts.RPCTimeout = defaultRPCTimeout
	}
	if opts.MaxPendingFetchRPC <= 0 {
		opts.MaxPendingFetchRPC = 1
	}

	transport := &Transport{
		localNode:    opts.LocalNode,
		client:       opts.Client,
		rpcMux:       opts.RPCMux,
		rpcTimeout:   opts.RPCTimeout,
		maxPending:   opts.MaxPendingFetchRPC,
		fetchService: opts.FetchService,
	}
	if source, ok := opts.FetchService.(channel.HandlerRuntime); ok {
		transport.statusSource = source
	}
	transport.sessions = newSessionManager(transport)
	opts.RPCMux.Handle(RPCServiceFetch, transport.handleRPC)
	opts.RPCMux.Handle(RPCServiceFetchBatch, transport.handleFetchBatchRPC)
	opts.RPCMux.Handle(RPCServiceProgressAck, transport.handleProgressAckRPC)
	return transport, nil
}

func (t *Transport) Close() error {
	t.closeOnce.Do(func() {
		if t.rpcMux == nil {
			return
		}
		t.rpcMux.Unhandle(RPCServiceFetch)
		t.rpcMux.Unhandle(RPCServiceFetchBatch)
		t.rpcMux.Unhandle(RPCServiceProgressAck)
	})
	return nil
}

func (t *Transport) BindFetchService(service runtime.FetchService) {
	t.mu.Lock()
	t.fetchService = service
	if source, ok := service.(channel.HandlerRuntime); ok {
		t.statusSource = source
	}
	t.mu.Unlock()
}

func (t *Transport) RegisterHandler(fn func(runtime.Envelope)) {
	t.mu.Lock()
	t.handler = fn
	t.mu.Unlock()
}

func (t *Transport) Send(peer channel.NodeID, env runtime.Envelope) error {
	return t.Session(peer).Send(env)
}

func (t *Transport) Session(peer channel.NodeID) runtime.PeerSession {
	return t.sessions.Session(peer)
}

func (t *Transport) SessionManager() runtime.PeerSessionManager {
	return t
}

func (t *Transport) handleRPC(ctx context.Context, body []byte) ([]byte, error) {
	req, err := decodeFetchRequest(body)
	if err != nil {
		return nil, err
	}
	service, err := t.boundFetchService()
	if err != nil {
		return nil, err
	}
	resp, err := service.ServeFetch(ctx, req)
	if err != nil {
		return nil, err
	}
	return encodeFetchResponse(resp)
}

func (t *Transport) handleFetchBatchRPC(ctx context.Context, body []byte) ([]byte, error) {
	req, err := decodeFetchBatchRequest(body)
	if err != nil {
		return nil, err
	}
	service, err := t.boundFetchService()
	if err != nil {
		return nil, err
	}
	resp := runtime.FetchBatchResponseEnvelope{
		Items: make([]runtime.FetchBatchResponseItem, 0, len(req.Items)),
	}
	for _, item := range req.Items {
		itemResp := runtime.FetchBatchResponseItem{RequestID: item.RequestID}
		fetchResp, fetchErr := service.ServeFetch(ctx, item.Request)
		if fetchErr != nil {
			itemResp.Error = fetchErr.Error()
		} else {
			fetchRespCopy := fetchResp
			itemResp.Response = &fetchRespCopy
		}
		resp.Items = append(resp.Items, itemResp)
	}
	return encodeFetchBatchResponse(resp)
}

func (t *Transport) handleProgressAckRPC(ctx context.Context, body []byte) ([]byte, error) {
	ack, err := decodeProgressAck(body)
	if err != nil {
		return nil, err
	}
	t.deliver(runtime.Envelope{
		Peer:        ack.ReplicaID,
		ChannelKey:  ack.ChannelKey,
		Epoch:       ack.Epoch,
		Generation:  ack.Generation,
		Kind:        runtime.MessageKindProgressAck,
		ProgressAck: &ack,
	})
	leaderHW := t.waitForLeaderHW(ctx, ack)
	return encodeProgressAckResponse(progressAckResponseEnvelope{LeaderHW: leaderHW})
}

func (t *Transport) deliver(env runtime.Envelope) {
	t.mu.RLock()
	handler := t.handler
	t.mu.RUnlock()
	if handler != nil {
		handler(env)
	}
}

func (t *Transport) boundFetchService() (runtime.FetchService, error) {
	t.mu.RLock()
	service := t.fetchService
	t.mu.RUnlock()
	if service == nil {
		return nil, fmt.Errorf("channeltransport: fetch service must be bound")
	}
	return service, nil
}

func (t *Transport) waitForLeaderHW(ctx context.Context, ack runtime.ProgressAckEnvelope) uint64 {
	t.mu.RLock()
	source := t.statusSource
	t.mu.RUnlock()
	if source == nil {
		return 0
	}

	leaderHW, committed := currentLeaderHW(source, ack)
	if committed || ack.MatchOffset == 0 {
		return leaderHW
	}

	ticker := time.NewTicker(2 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return leaderHW
		case <-ticker.C:
			leaderHW, committed = currentLeaderHW(source, ack)
			if committed {
				return leaderHW
			}
		}
	}
}

func currentLeaderHW(source channel.HandlerRuntime, ack runtime.ProgressAckEnvelope) (uint64, bool) {
	handle, ok := source.Channel(ack.ChannelKey)
	if !ok {
		return 0, false
	}
	state := handle.Status()
	if state.Epoch != ack.Epoch {
		return state.HW, false
	}
	return state.HW, state.HW >= ack.MatchOffset
}
