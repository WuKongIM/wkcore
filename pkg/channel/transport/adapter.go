package transport

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/channel/isr"
	isrnode "github.com/WuKongIM/WuKongIM/pkg/channel/node"
	"github.com/WuKongIM/WuKongIM/pkg/transport"
)

const defaultRPCTimeout = 5 * time.Second

type Options struct {
	LocalNode          isr.NodeID
	Client             *transport.Client
	RPCMux             *transport.RPCMux
	FetchService       isrnode.FetchService
	RPCTimeout         time.Duration
	MaxPendingFetchRPC int
}

type Adapter struct {
	localNode    isr.NodeID
	client       *transport.Client
	fetchService isrnode.FetchService
	rpcTimeout   time.Duration
	maxPending   int

	mu      sync.RWMutex
	handler func(isrnode.Envelope)

	sessions *sessionManager
}

var _ isrnode.Transport = (*Adapter)(nil)

func New(opts Options) (*Adapter, error) {
	if opts.LocalNode == 0 {
		return nil, fmt.Errorf("isrnodetransport: local node must be set")
	}
	if opts.Client == nil {
		return nil, fmt.Errorf("isrnodetransport: client must be set")
	}
	if opts.RPCMux == nil {
		return nil, fmt.Errorf("isrnodetransport: rpc mux must be set")
	}
	if opts.FetchService == nil {
		return nil, fmt.Errorf("isrnodetransport: fetch service must be set")
	}
	if opts.RPCTimeout <= 0 {
		opts.RPCTimeout = defaultRPCTimeout
	}
	if opts.MaxPendingFetchRPC <= 0 {
		opts.MaxPendingFetchRPC = 1
	}

	adapter := &Adapter{
		localNode:    opts.LocalNode,
		client:       opts.Client,
		fetchService: opts.FetchService,
		rpcTimeout:   opts.RPCTimeout,
		maxPending:   opts.MaxPendingFetchRPC,
	}
	adapter.sessions = newSessionManager(adapter)
	opts.RPCMux.Handle(RPCServiceFetch, adapter.handleRPC)
	opts.RPCMux.Handle(RPCServiceFetchBatch, adapter.handleFetchBatchRPC)
	opts.RPCMux.Handle(RPCServiceProgressAck, adapter.handleProgressAckRPC)
	return adapter, nil
}

func (a *Adapter) RegisterHandler(fn func(isrnode.Envelope)) {
	a.mu.Lock()
	a.handler = fn
	a.mu.Unlock()
}

func (a *Adapter) Send(peer isr.NodeID, env isrnode.Envelope) error {
	return a.SessionManager().Session(peer).Send(env)
}

func (a *Adapter) SessionManager() isrnode.PeerSessionManager {
	return a.sessions
}

func (a *Adapter) handleRPC(ctx context.Context, body []byte) ([]byte, error) {
	req, err := decodeFetchRequest(body)
	if err != nil {
		return nil, err
	}
	resp, err := a.fetchService.ServeFetch(ctx, req)
	if err != nil {
		return nil, err
	}
	return encodeFetchResponse(resp)
}

func (a *Adapter) handleFetchBatchRPC(ctx context.Context, body []byte) ([]byte, error) {
	req, err := decodeFetchBatchRequest(body)
	if err != nil {
		return nil, err
	}
	resp := isrnode.FetchBatchResponseEnvelope{
		Items: make([]isrnode.FetchBatchResponseItem, 0, len(req.Items)),
	}
	for _, item := range req.Items {
		itemResp := isrnode.FetchBatchResponseItem{RequestID: item.RequestID}
		fetchResp, fetchErr := a.fetchService.ServeFetch(ctx, item.Request)
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

func (a *Adapter) handleProgressAckRPC(ctx context.Context, body []byte) ([]byte, error) {
	ack, err := decodeProgressAck(body)
	if err != nil {
		return nil, err
	}
	a.deliver(isrnode.Envelope{
		Peer:        ack.ReplicaID,
		ChannelKey:  ack.ChannelKey,
		Epoch:       ack.Epoch,
		Generation:  ack.Generation,
		Kind:        isrnode.MessageKindProgressAck,
		ProgressAck: &ack,
	})
	return nil, nil
}

func (a *Adapter) deliver(env isrnode.Envelope) {
	a.mu.RLock()
	handler := a.handler
	a.mu.RUnlock()
	if handler != nil {
		handler(env)
	}
}
