package isrnodetransport

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/replication/isr"
	"github.com/WuKongIM/WuKongIM/pkg/replication/isrnode"
	"github.com/WuKongIM/WuKongIM/pkg/transport/nodetransport"
)

const defaultRPCTimeout = 5 * time.Second

type Options struct {
	LocalNode    isr.NodeID
	Client       *nodetransport.Client
	RPCMux       *nodetransport.RPCMux
	FetchService isrnode.FetchService
}

type Adapter struct {
	localNode    isr.NodeID
	client       *nodetransport.Client
	fetchService isrnode.FetchService

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

	adapter := &Adapter{
		localNode:    opts.LocalNode,
		client:       opts.Client,
		fetchService: opts.FetchService,
	}
	adapter.sessions = newSessionManager(adapter)
	opts.RPCMux.Handle(RPCServiceFetch, adapter.handleRPC)
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

func (a *Adapter) deliver(env isrnode.Envelope) {
	a.mu.RLock()
	handler := a.handler
	a.mu.RUnlock()
	if handler != nil {
		handler(env)
	}
}
