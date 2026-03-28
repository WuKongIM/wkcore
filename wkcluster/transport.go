package wkcluster

import (
	"context"
	"io"
	"net"
	"sync"
	"time"

	"github.com/WuKongIM/wraft/multiraft"
	"go.etcd.io/raft/v3/raftpb"
)

type connPool struct {
	addr  string
	size  int
	conns []net.Conn
	mu    []sync.Mutex
}

func newConnPool(addr string, size int) *connPool {
	return &connPool{
		addr:  addr,
		size:  size,
		conns: make([]net.Conn, size),
		mu:    make([]sync.Mutex, size),
	}
}

func (p *connPool) getByGroup(groupID multiraft.GroupID) (net.Conn, int, error) {
	idx := int(uint64(groupID) % uint64(p.size))
	p.mu[idx].Lock()
	if p.conns[idx] == nil {
		conn, err := net.DialTimeout("tcp", p.addr, 5*time.Second)
		if err != nil {
			p.mu[idx].Unlock()
			return nil, idx, err
		}
		if tc, ok := conn.(*net.TCPConn); ok {
			_ = tc.SetKeepAlive(true)
			_ = tc.SetKeepAlivePeriod(30 * time.Second)
		}
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

// forwardHandler processes incoming forward requests on the leader side.
type forwardHandler interface {
	handleForward(ctx context.Context, groupID multiraft.GroupID, cmd []byte) ([]byte, uint8)
}

type Transport struct {
	nodeID    multiraft.NodeID
	discovery Discovery
	poolSize  int
	pools     map[multiraft.NodeID]*connPool
	mu        sync.RWMutex
	runtime   *multiraft.Runtime
	listener  net.Listener
	handler   forwardHandler
	stopCh    chan struct{}
	wg        sync.WaitGroup
}

func NewTransport(nodeID multiraft.NodeID, discovery Discovery, poolSize int) *Transport {
	return &Transport{
		nodeID:    nodeID,
		discovery: discovery,
		poolSize:  poolSize,
		pools:     make(map[multiraft.NodeID]*connPool),
		stopCh:    make(chan struct{}),
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
	t.mu.RLock()
	for _, p := range t.pools {
		p.closeAll()
	}
	t.mu.RUnlock()
	t.wg.Wait()
}

// Send implements multiraft.Transport.
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

	addr, err := t.discovery.Resolve(nodeID)
	if err != nil {
		return nil
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	if p, ok = t.pools[nodeID]; ok {
		return p
	}
	p = newConnPool(addr, t.poolSize)
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
		if tc, ok := conn.(*net.TCPConn); ok {
			_ = tc.SetKeepAlive(true)
			_ = tc.SetKeepAlivePeriod(30 * time.Second)
		}
		t.wg.Add(1)
		go t.handleConn(conn)
	}
}

func (t *Transport) handleConn(conn net.Conn) {
	defer t.wg.Done()
	defer conn.Close()

	var writeMu sync.Mutex

	for {
		select {
		case <-t.stopCh:
			return
		default:
		}

		msgType, body, err := readMessage(conn)
		if err != nil {
			if err == io.EOF {
				return
			}
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
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			data, errCode = t.handler.handleForward(ctx, multiraft.GroupID(groupID), cmd)
			cancel()
		}
		writeMu.Lock()
		_ = writeRespMessage(conn, requestID, errCode, data)
		writeMu.Unlock()
	}()
}
