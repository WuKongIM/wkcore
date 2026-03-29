package wktransport

import (
	"net"
	"sync"
	"time"
)

const tcpKeepAlivePeriod = 30 * time.Second

// Pool manages outbound TCP connections to remote nodes.
// Each Pool instance is fully independent — create separate instances
// for raft and business traffic to keep connections physically isolated.
type Pool struct {
	discovery   Discovery
	size        int
	dialTimeout time.Duration
	nodes       map[NodeID]*nodeConns
	mu          sync.RWMutex
}

type nodeConns struct {
	addr  string
	conns []net.Conn
	mu    []sync.Mutex
}

// NewPool creates a connection pool.
// size is the number of connections per remote node.
func NewPool(discovery Discovery, size int, dialTimeout time.Duration) *Pool {
	return &Pool{
		discovery:   discovery,
		size:        size,
		dialTimeout: dialTimeout,
		nodes:       make(map[NodeID]*nodeConns),
	}
}

// Get selects a connection by shardKey % size, creating one if needed.
// On success, the connection slot lock is held — caller MUST call Release.
// On error, the lock is NOT held — caller MUST NOT call Release.
func (p *Pool) Get(nodeID NodeID, shardKey uint64) (net.Conn, int, error) {
	nc, err := p.getOrCreateNodeConns(nodeID)
	if err != nil {
		return nil, 0, err
	}
	idx := int(shardKey % uint64(p.size))
	nc.mu[idx].Lock()
	if nc.conns[idx] == nil {
		conn, err := net.DialTimeout("tcp", nc.addr, p.dialTimeout)
		if err != nil {
			nc.mu[idx].Unlock()
			return nil, 0, err
		}
		setTCPKeepAlive(conn)
		nc.conns[idx] = conn
	}
	return nc.conns[idx], idx, nil
}

// Release unlocks the connection slot. Must be called after a successful Get.
func (p *Pool) Release(nodeID NodeID, idx int) {
	p.mu.RLock()
	nc, ok := p.nodes[nodeID]
	p.mu.RUnlock()
	if ok {
		nc.mu[idx].Unlock()
	}
}

// Reset closes and clears a connection slot. Caller must hold the slot lock
// (i.e., call Reset between Get and Release). Does not release the lock.
func (p *Pool) Reset(nodeID NodeID, idx int) {
	p.mu.RLock()
	nc, ok := p.nodes[nodeID]
	p.mu.RUnlock()
	if ok && nc.conns[idx] != nil {
		_ = nc.conns[idx].Close()
		nc.conns[idx] = nil
	}
}

// Close closes all connections in all pools.
func (p *Pool) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, nc := range p.nodes {
		for i := range nc.conns {
			nc.mu[i].Lock()
			if nc.conns[i] != nil {
				_ = nc.conns[i].Close()
				nc.conns[i] = nil
			}
			nc.mu[i].Unlock()
		}
	}
}

func (p *Pool) getOrCreateNodeConns(nodeID NodeID) (*nodeConns, error) {
	p.mu.RLock()
	nc, ok := p.nodes[nodeID]
	p.mu.RUnlock()
	if ok {
		return nc, nil
	}

	addr, err := p.discovery.Resolve(nodeID)
	if err != nil {
		return nil, err
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	if nc, ok = p.nodes[nodeID]; ok {
		return nc, nil
	}
	nc = &nodeConns{
		addr:  addr,
		conns: make([]net.Conn, p.size),
		mu:    make([]sync.Mutex, p.size),
	}
	p.nodes[nodeID] = nc
	return nc, nil
}

func setTCPKeepAlive(conn net.Conn) {
	if tc, ok := conn.(*net.TCPConn); ok {
		_ = tc.SetKeepAlive(true)
		_ = tc.SetKeepAlivePeriod(tcpKeepAlivePeriod)
	}
}
