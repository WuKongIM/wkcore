package transport

import (
	"context"
	"net"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

const tcpKeepAlivePeriod = 30 * time.Second

type PoolConfig struct {
	Discovery   Discovery
	Size        int
	DialTimeout time.Duration
	QueueSizes  [numPriorities]int
	DefaultPri  Priority
	Observer    ObserverHooks
}

// Pool manages outbound TCP connections to remote nodes.
type Pool struct {
	cfg    PoolConfig
	size   int
	nodes  sync.Map // NodeID -> *nodeConnSet
	nextID atomic.Uint64
}

type nodeConnSet struct {
	addr   string
	conns  []atomic.Pointer[MuxConn]
	dialMu sync.Mutex
}

func NewPool(args ...any) *Pool {
	var cfg PoolConfig
	switch len(args) {
	case 1:
		cfg = args[0].(PoolConfig)
	case 3:
		cfg = PoolConfig{
			Discovery:   args[0].(Discovery),
			Size:        args[1].(int),
			DialTimeout: args[2].(time.Duration),
		}
	default:
		panic("nodetransport: invalid NewPool arguments")
	}
	if cfg.Size <= 0 {
		cfg.Size = 1
	}
	return &Pool{cfg: cfg, size: cfg.Size}
}

func (p *Pool) Send(nodeID NodeID, shardKey uint64, msgType uint8, body []byte) error {
	mc, err := p.acquire(nodeID, shardKey)
	if err != nil {
		return err
	}
	return mc.Send(p.cfg.DefaultPri, msgType, body)
}

func (p *Pool) RPC(ctx context.Context, nodeID NodeID, shardKey uint64, payload []byte) ([]byte, error) {
	mc, err := p.acquire(nodeID, shardKey)
	if err != nil {
		return nil, err
	}
	reqID := p.nextID.Add(1)
	wire := encodeRPCRequest(reqID, payload)
	return mc.RPC(ctx, p.cfg.DefaultPri, reqID, wire)
}

func (p *Pool) Close() {
	p.nodes.Range(func(_, value any) bool {
		set := value.(*nodeConnSet)
		for i := range set.conns {
			if mc := set.conns[i].Load(); mc != nil {
				mc.Close()
			}
		}
		return true
	})
}

func (p *Pool) Stats() []PoolPeerStats {
	if p == nil {
		return nil
	}

	stats := make([]PoolPeerStats, 0)
	p.nodes.Range(func(key, value any) bool {
		nodeID, ok := key.(NodeID)
		if !ok {
			return true
		}
		set, ok := value.(*nodeConnSet)
		if !ok || set == nil {
			return true
		}
		active := 0
		for i := range set.conns {
			if mc := set.conns[i].Load(); mc != nil && !mc.closed.Load() {
				active++
			}
		}
		idle := len(set.conns) - active
		if idle < 0 {
			idle = 0
		}
		stats = append(stats, PoolPeerStats{
			NodeID: nodeID,
			Active: active,
			Idle:   idle,
		})
		return true
	})
	sort.Slice(stats, func(i, j int) bool {
		return stats[i].NodeID < stats[j].NodeID
	})
	return stats
}

func (p *Pool) acquire(nodeID NodeID, shardKey uint64) (*MuxConn, error) {
	set, err := p.getOrCreateNodeSet(nodeID)
	if err != nil {
		return nil, err
	}
	idx := int(shardKey % uint64(len(set.conns)))

	if mc := set.conns[idx].Load(); mc != nil && !mc.closed.Load() {
		return mc, nil
	}

	set.dialMu.Lock()
	defer set.dialMu.Unlock()

	if mc := set.conns[idx].Load(); mc != nil && !mc.closed.Load() {
		return mc, nil
	}

	raw, err := net.DialTimeout("tcp", set.addr, p.cfg.DialTimeout)
	if err != nil {
		return nil, err
	}
	setTCPKeepAlive(raw)
	mc := newMuxConn(raw, nil, ConnConfig{QueueSizes: p.cfg.QueueSizes, Observer: p.cfg.Observer})
	set.conns[idx].Store(mc)
	return mc, nil
}

func (p *Pool) getOrCreateNodeSet(nodeID NodeID) (*nodeConnSet, error) {
	if value, ok := p.nodes.Load(nodeID); ok {
		return value.(*nodeConnSet), nil
	}

	addr, err := p.cfg.Discovery.Resolve(nodeID)
	if err != nil {
		return nil, err
	}

	created := &nodeConnSet{
		addr:  addr,
		conns: make([]atomic.Pointer[MuxConn], p.cfg.Size),
	}
	actual, _ := p.nodes.LoadOrStore(nodeID, created)
	return actual.(*nodeConnSet), nil
}

func setTCPKeepAlive(conn net.Conn) {
	if tc, ok := conn.(*net.TCPConn); ok {
		_ = tc.SetKeepAlive(true)
		_ = tc.SetKeepAlivePeriod(tcpKeepAlivePeriod)
	}
}
