package wkcluster

import (
	"context"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/WuKongIM/wraft/multiraft"
)

type forwardResp struct {
	errCode uint8
	data    []byte
}

type Forwarder struct {
	nodeID    multiraft.NodeID
	transport *Transport
	timeout   time.Duration
	nextReqID atomic.Uint64
	pending   sync.Map // requestID → chan forwardResp

	// readLoops tracks which connections have a reader goroutine
	readLoops sync.Map // "nodeID-connIdx" → net.Conn
	wg        sync.WaitGroup
	stopCh    chan struct{}
}

func NewForwarder(nodeID multiraft.NodeID, transport *Transport, timeout time.Duration) *Forwarder {
	return &Forwarder{
		nodeID:    nodeID,
		transport: transport,
		timeout:   timeout,
		stopCh:    make(chan struct{}),
	}
}

func (f *Forwarder) Stop() {
	close(f.stopCh)
	// Close all tracked connections to unblock readLoops
	f.readLoops.Range(func(key, value any) bool {
		if conn, ok := value.(net.Conn); ok {
			_ = conn.Close()
		}
		return true
	})
	// Cancel all pending requests
	f.pending.Range(func(key, value any) bool {
		ch := value.(chan forwardResp)
		select {
		case ch <- forwardResp{errCode: errCodeTimeout}:
		default:
		}
		return true
	})
	f.wg.Wait()
}

func (f *Forwarder) Forward(ctx context.Context, targetNode multiraft.NodeID, groupID multiraft.GroupID, cmdBytes []byte) ([]byte, error) {
	pool := f.transport.getOrCreatePool(targetNode)
	if pool == nil {
		return nil, ErrNodeNotFound
	}

	requestID := f.nextReqID.Add(1)
	respCh := make(chan forwardResp, 1)
	f.pending.Store(requestID, respCh)
	defer f.pending.Delete(requestID)

	conn, idx, err := pool.getByGroup(groupID)
	if err != nil {
		return nil, fmt.Errorf("connect to leader: %w", err)
	}

	// Ensure a read loop for this connection
	f.ensureReadLoop(targetNode, idx, conn)

	body := encodeForwardBody(requestID, uint64(groupID), cmdBytes)
	msg := encodeMessage(msgTypeForward, body)
	_, err = conn.Write(msg)
	pool.release(idx)
	if err != nil {
		pool.mu[idx].Lock()
		pool.resetConn(idx)
		pool.mu[idx].Unlock()
		return nil, fmt.Errorf("write forward: %w", err)
	}

	// Wait for response
	deadline := time.After(f.timeout)
	select {
	case resp := <-respCh:
		return f.handleResp(resp)
	case <-deadline:
		return nil, ErrTimeout
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-f.stopCh:
		return nil, ErrStopped
	}
}

func (f *Forwarder) handleResp(resp forwardResp) ([]byte, error) {
	switch resp.errCode {
	case errCodeOK:
		return resp.data, nil
	case errCodeNotLeader:
		return nil, ErrNotLeader
	case errCodeTimeout:
		return nil, ErrTimeout
	case errCodeNoGroup:
		return nil, ErrGroupNotFound
	default:
		return nil, fmt.Errorf("unknown error code: %d", resp.errCode)
	}
}

func (f *Forwarder) ensureReadLoop(nodeID multiraft.NodeID, idx int, conn net.Conn) {
	key := fmt.Sprintf("%d-%d", nodeID, idx)
	if _, loaded := f.readLoops.LoadOrStore(key, conn); loaded {
		return
	}
	f.wg.Add(1)
	go f.readLoop(key, conn)
}

func (f *Forwarder) readLoop(key string, conn net.Conn) {
	defer f.wg.Done()
	defer f.readLoops.Delete(key)

	r := io.Reader(conn)
	for {
		select {
		case <-f.stopCh:
			return
		default:
		}

		msgType, body, err := readMessage(r)
		if err != nil {
			return
		}
		if msgType != msgTypeResp {
			continue
		}

		requestID, errCode, data, err := decodeRespBody(body)
		if err != nil {
			continue
		}

		if v, ok := f.pending.LoadAndDelete(requestID); ok {
			ch := v.(chan forwardResp)
			ch <- forwardResp{errCode: errCode, data: data}
		}
	}
}
