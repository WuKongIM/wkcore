package wkcluster

import (
	"context"
	"net"
	"sync"
	"time"

	"github.com/WuKongIM/wraft/multiraft"
	"go.etcd.io/raft/v3/raftpb"
)

const tcpKeepAlivePeriod = 30 * time.Second

type connPool struct {
	addr        string
	size        int
	dialTimeout time.Duration
	conns       []net.Conn
	mu          []sync.Mutex
}

func newConnPool(addr string, size int, dialTimeout time.Duration) *connPool {
	return &connPool{
		addr:        addr,
		size:        size,
		dialTimeout: dialTimeout,
		conns:       make([]net.Conn, size),
		mu:          make([]sync.Mutex, size),
	}
}

// getByGroup returns a connection for the given group, creating one if needed.
// The caller MUST call release(idx) after use, even when subsequent I/O fails.
func (p *connPool) getByGroup(groupID multiraft.GroupID) (net.Conn, int, error) {
	idx := int(uint64(groupID) % uint64(p.size))
	p.mu[idx].Lock()
	if p.conns[idx] == nil {
		conn, err := net.DialTimeout("tcp", p.addr, p.dialTimeout)
		if err != nil {
			p.mu[idx].Unlock()
			return nil, idx, err
		}
		setTCPKeepAlive(conn)
		p.conns[idx] = conn
	}
	return p.conns[idx], idx, nil
}

func (p *connPool) release(idx int) {
	p.mu[idx].Unlock()
}

func (p *connPool) resetConn(idx int) {
	if p.conns[idx] != nil {
		_ = p.conns[idx].Close()
		p.conns[idx] = nil
	}
}

func (p *connPool) closeAll() {
	for i := range p.conns {
		p.mu[i].Lock()
		p.resetConn(i)
		p.mu[i].Unlock()
	}
}

// setTCPKeepAlive enables TCP keep-alive on the connection if it is a TCP connection.
func setTCPKeepAlive(conn net.Conn) {
	if tc, ok := conn.(*net.TCPConn); ok {
		_ = tc.SetKeepAlive(true)
		_ = tc.SetKeepAlivePeriod(tcpKeepAlivePeriod)
	}
}

// forwardHandler processes incoming forward requests on the leader side.
type forwardHandler interface {
	handleForward(ctx context.Context, groupID multiraft.GroupID, cmd []byte) ([]byte, uint8)
}

type Transport struct {
	nodeID         multiraft.NodeID
	discovery      Discovery
	poolSize       int
	dialTimeout    time.Duration
	forwardTimeout time.Duration
	pools          map[multiraft.NodeID]*connPool
	mu             sync.RWMutex
	runtime        *multiraft.Runtime
	listener       net.Listener
	handler        forwardHandler
	stopCh         chan struct{}
	wg             sync.WaitGroup

	// accepted tracks incoming connections so Stop() can close them,
	// unblocking handleConn goroutines stuck in readMessage.
	accepted   map[net.Conn]struct{}
	acceptedMu sync.Mutex
}

func NewTransport(nodeID multiraft.NodeID, discovery Discovery, poolSize int, dialTimeout, forwardTimeout time.Duration) *Transport {
	return &Transport{
		nodeID:         nodeID,
		discovery:      discovery,
		poolSize:       poolSize,
		dialTimeout:    dialTimeout,
		forwardTimeout: forwardTimeout,
		pools:          make(map[multiraft.NodeID]*connPool),
		accepted:       make(map[net.Conn]struct{}),
		stopCh:         make(chan struct{}),
	}
}

func (t *Transport) SetRuntime(rt *multiraft.Runtime) { t.runtime = rt }
func (t *Transport) SetHandler(h forwardHandler)      { t.handler = h }

func (t *Transport) Start(listenAddr string) error {
	ln, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return err
	}
	t.listener = ln
	t.wg.Add(1)
	go t.acceptLoop()
	return nil
}

func (t *Transport) Stop() {
	close(t.stopCh)
	if t.listener != nil {
		_ = t.listener.Close()
	}
	// Close all accepted (incoming) connections to unblock handleConn
	// goroutines stuck in readMessage.
	t.acceptedMu.Lock()
	for c := range t.accepted {
		_ = c.Close()
	}
	t.acceptedMu.Unlock()
	// Close outgoing connection pools.
	t.mu.RLock()
	for _, p := range t.pools {
		p.closeAll()
	}
	t.mu.RUnlock()
	t.wg.Wait()
}

// Send implements multiraft.Transport. Individual message delivery failures are
// silently skipped because the raft layer handles retransmission; only context
// cancellation is propagated to the caller.
func (t *Transport) Send(ctx context.Context, batch []multiraft.Envelope) error {
	for _, env := range batch {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		data, err := env.Message.Marshal()
		if err != nil {
			return err
		}

		target := multiraft.NodeID(env.Message.To)
		pool := t.getOrCreatePool(target)
		if pool == nil {
			continue // unknown node, skip
		}
		conn, idx, err := pool.getByGroup(env.GroupID)
		if err != nil {
			continue
		}
		err = writeRaftMessage(conn, uint64(env.GroupID), data)
		if err != nil {
			pool.resetConn(idx)
		}
		pool.release(idx)
	}
	return nil
}

func (t *Transport) getOrCreatePool(nodeID multiraft.NodeID) *connPool {
	t.mu.RLock()
	p, ok := t.pools[nodeID]
	t.mu.RUnlock()
	if ok {
		return p
	}

	addr, err := t.discovery.Resolve(uint64(nodeID))
	if err != nil {
		return nil
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	if p, ok = t.pools[nodeID]; ok {
		return p
	}
	p = newConnPool(addr, t.poolSize, t.dialTimeout)
	t.pools[nodeID] = p
	return p
}

func (t *Transport) acceptLoop() {
	defer t.wg.Done()
	for {
		conn, err := t.listener.Accept()
		if err != nil {
			select {
			case <-t.stopCh:
				return
			default:
				continue
			}
		}
		setTCPKeepAlive(conn)
		t.wg.Add(1)
		go t.handleConn(conn)
	}
}

func (t *Transport) handleConn(conn net.Conn) {
	defer t.wg.Done()

	t.acceptedMu.Lock()
	t.accepted[conn] = struct{}{}
	t.acceptedMu.Unlock()

	defer func() {
		conn.Close()
		t.acceptedMu.Lock()
		delete(t.accepted, conn)
		t.acceptedMu.Unlock()
	}()

	var writeMu sync.Mutex

	for {
		select {
		case <-t.stopCh:
			return
		default:
		}

		msgType, body, err := readMessage(conn)
		if err != nil {
			return
		}

		switch msgType {
		case msgTypeRaft:
			t.handleRaftMessage(body)
		case msgTypeForward:
			t.handleForwardAsync(conn, &writeMu, body)
		case msgTypeResp:
			// Responses are handled by the Forwarder on outgoing connections
		}
	}
}

func (t *Transport) handleRaftMessage(body []byte) {
	if t.runtime == nil {
		return
	}
	groupID, data, err := decodeRaftBody(body)
	if err != nil {
		return
	}
	var msg raftpb.Message
	if err := msg.Unmarshal(data); err != nil {
		return
	}
	_ = t.runtime.Step(context.Background(), multiraft.Envelope{
		GroupID: multiraft.GroupID(groupID),
		Message: msg,
	})
}

// handleForwardAsync processes a forwarded request in a separate goroutine
// so the connection read loop is not blocked during raft proposal execution.
// The writeMu serializes response writes on the same connection.
func (t *Transport) handleForwardAsync(conn net.Conn, writeMu *sync.Mutex, body []byte) {
	requestID, groupID, cmd, err := decodeForwardBody(body)
	if err != nil {
		return
	}
	t.wg.Add(1)
	go func() {
		defer t.wg.Done()
		var (
			errCode uint8
			data    []byte
		)
		if t.handler == nil {
			errCode = errCodeNoGroup
		} else {
			ctx, cancel := context.WithTimeout(context.Background(), t.forwardTimeout)
			data, errCode = t.handler.handleForward(ctx, multiraft.GroupID(groupID), cmd)
			cancel()
		}
		writeMu.Lock()
		_ = writeRespMessage(conn, requestID, errCode, data)
		writeMu.Unlock()
	}()
}
