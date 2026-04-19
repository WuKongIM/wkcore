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
const poolDialCooldown = 50 * time.Millisecond

type PoolConfig struct {
	Discovery   Discovery
	Size        int
	DialTimeout time.Duration
	Dial        func(network, addr string, timeout time.Duration) (net.Conn, error)
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
	addr  string
	slots []*connSlot
	conns []atomic.Pointer[MuxConn]
}

type connSlot struct {
	conn         atomic.Pointer[MuxConn]
	mirror       *atomic.Pointer[MuxConn]
	mu           sync.Mutex
	ready        chan struct{}
	lastDialFail time.Time
	lastErr      error
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
		for i := range set.slots {
			if mc := set.slots[i].conn.Load(); mc != nil {
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
		for i := range set.slots {
			if mc := set.slots[i].conn.Load(); mc != nil && !mc.closed.Load() {
				active++
			}
		}
		idle := len(set.slots) - active
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
	slot := set.slots[int(shardKey%uint64(len(set.slots)))]
	for {
		if mc := slot.conn.Load(); mc != nil {
			if !mc.closed.Load() {
				return mc, nil
			}
			slot.clearClosedConn(mc)
		}

		ready, cachedErr := slot.waiterOrCooldown()
		if ready != nil {
			<-ready
			continue
		}
		if cachedErr != nil {
			return nil, cachedErr
		}

		ready = make(chan struct{})
		slot.mu.Lock()
		if mc := slot.conn.Load(); mc != nil {
			slot.mu.Unlock()
			continue
		}
		if slot.ready != nil {
			ready = slot.ready
			slot.mu.Unlock()
			<-ready
			continue
		}
		slot.ready = ready
		slot.mu.Unlock()

		dial := p.cfg.Dial
		if dial == nil {
			dial = net.DialTimeout
		}
		raw, dialErr := dial("tcp", set.addr, p.cfg.DialTimeout)
		if dialErr != nil {
			slot.finishDial(nil, dialErr, ready)
			return nil, dialErr
		}
		setTCPKeepAlive(raw)
		mc := newMuxConn(raw, nil, ConnConfig{QueueSizes: p.cfg.QueueSizes, Observer: p.cfg.Observer})
		slot.finishDial(mc, nil, ready)
		return mc, nil
	}
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
		slots: make([]*connSlot, p.cfg.Size),
		conns: make([]atomic.Pointer[MuxConn], p.cfg.Size),
	}
	for i := range created.slots {
		created.slots[i] = &connSlot{mirror: &created.conns[i]}
	}
	actual, _ := p.nodes.LoadOrStore(nodeID, created)
	return actual.(*nodeConnSet), nil
}

func (s *connSlot) clearClosedConn(mc *MuxConn) {
	s.mu.Lock()
	if s.conn.Load() == mc {
		s.conn.Store(nil)
		if s.mirror != nil {
			s.mirror.Store(nil)
		}
	}
	s.mu.Unlock()
}

func (s *connSlot) waiterOrCooldown() (chan struct{}, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ready != nil {
		return s.ready, nil
	}
	if s.lastErr != nil && time.Since(s.lastDialFail) < poolDialCooldown {
		return nil, s.lastErr
	}
	return nil, nil
}

func (s *connSlot) finishDial(mc *MuxConn, dialErr error, ready chan struct{}) {
	s.mu.Lock()
	if dialErr != nil {
		s.lastDialFail = time.Now()
		s.lastErr = dialErr
	} else {
		s.conn.Store(mc)
		if s.mirror != nil {
			s.mirror.Store(mc)
		}
		s.lastErr = nil
		s.lastDialFail = time.Time{}
	}
	if s.ready == ready {
		s.ready = nil
	}
	s.mu.Unlock()
	close(ready)
}

func setTCPKeepAlive(conn net.Conn) {
	if tc, ok := conn.(*net.TCPConn); ok {
		_ = tc.SetKeepAlive(true)
		_ = tc.SetKeepAlivePeriod(tcpKeepAlivePeriod)
	}
}
