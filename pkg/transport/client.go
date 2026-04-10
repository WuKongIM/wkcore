package transport

import (
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
)

// Client provides one-way Send and request/response RPC over a Pool.
type Client struct {
	pool      *Pool
	nextReqID atomic.Uint64
	pending   sync.Map // requestID → pendingRPC
	readLoops sync.Map // readLoopKey → net.Conn
	stopCh    chan struct{}
	wg        sync.WaitGroup
}

type rpcResponse struct {
	body []byte
	err  error
}

type pendingRPC struct {
	key uint64
	ch  chan rpcResponse
}

// NewClient creates a Client bound to the given Pool.
func NewClient(pool *Pool) *Client {
	return &Client{
		pool:   pool,
		stopCh: make(chan struct{}),
	}
}

// Send writes a one-way message. No response expected.
func (c *Client) Send(nodeID NodeID, shardKey uint64, msgType uint8, body []byte) error {
	conn, idx, err := c.pool.Get(nodeID, shardKey)
	if err != nil {
		return err
	}
	err = WriteMessage(conn, msgType, body)
	if err != nil {
		c.pool.Reset(nodeID, idx)
	}
	c.pool.Release(nodeID, idx)
	return err
}

// RPC sends a request and waits for a response.
func (c *Client) RPC(ctx context.Context, nodeID NodeID, shardKey uint64, payload []byte) ([]byte, error) {
	reqID := c.nextReqID.Add(1)
	conn, idx, err := c.pool.Get(nodeID, shardKey)
	if err != nil {
		return nil, err
	}

	key := readLoopKey(nodeID, idx)
	respCh := make(chan rpcResponse, 1)
	c.pending.Store(reqID, pendingRPC{key: key, ch: respCh})
	defer c.pending.Delete(reqID)

	c.ensureReadLoop(nodeID, idx, conn)

	reqBody := encodeRPCRequest(reqID, payload)
	err = WriteMessage(conn, MsgTypeRPCRequest, reqBody)
	if err != nil {
		c.pool.Reset(nodeID, idx)
	}
	c.pool.Release(nodeID, idx)
	if err != nil {
		return nil, err
	}

	select {
	case resp := <-respCh:
		if resp.err != nil {
			return nil, resp.err
		}
		return resp.body, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.stopCh:
		return nil, ErrStopped
	}
}

func (c *Client) RPCService(ctx context.Context, nodeID NodeID, shardKey uint64, serviceID uint8, payload []byte) ([]byte, error) {
	return c.RPC(ctx, nodeID, shardKey, encodeRPCServicePayload(serviceID, payload))
}

// Stop cancels all pending RPCs and waits for goroutines to exit.
func (c *Client) Stop() {
	close(c.stopCh)
	c.readLoops.Range(func(key, value any) bool {
		if conn, ok := value.(net.Conn); ok {
			_ = conn.Close()
		}
		return true
	})
	c.pending.Range(func(key, value any) bool {
		pending := value.(pendingRPC)
		select {
		case pending.ch <- rpcResponse{err: ErrStopped}:
		default:
		}
		return true
	})
	c.wg.Wait()
}

func readLoopKey(nodeID NodeID, idx int) uint64 {
	return nodeID<<32 | uint64(idx)
}

func (c *Client) ensureReadLoop(nodeID NodeID, idx int, conn net.Conn) {
	key := readLoopKey(nodeID, idx)
	for {
		existing, loaded := c.readLoops.LoadOrStore(key, conn)
		if !loaded {
			c.wg.Add(1)
			go c.readLoop(key, conn)
			return
		}
		if existing.(net.Conn) == conn {
			return
		}
		if c.readLoops.CompareAndSwap(key, existing, conn) {
			c.wg.Add(1)
			go c.readLoop(key, conn)
			return
		}
	}
}

func (c *Client) readLoop(key uint64, conn net.Conn) {
	defer c.wg.Done()
	defer c.readLoops.Delete(key)

	for {
		select {
		case <-c.stopCh:
			return
		default:
		}

		msgType, body, err := ReadMessage(conn)
		if err != nil {
			c.failPendingForKey(key, err)
			return
		}
		if msgType != MsgTypeRPCResponse {
			continue
		}

		if len(body) < 9 {
			continue
		}
		requestID := binary.BigEndian.Uint64(body[0:8])
		errCode := body[8]
		data := body[9:]

		if v, ok := c.pending.LoadAndDelete(requestID); ok {
			pending := v.(pendingRPC)
			var resp rpcResponse
			if errCode != 0 {
				resp.err = fmt.Errorf("nodetransport: remote handler error: %s", data)
				resp.body = data
			} else {
				resp.body = data
			}
			pending.ch <- resp
		}
	}
}

func (c *Client) failPendingForKey(key uint64, err error) {
	c.pending.Range(func(requestID, value any) bool {
		pending := value.(pendingRPC)
		if pending.key != key {
			return true
		}
		if v, ok := c.pending.LoadAndDelete(requestID); ok {
			pending = v.(pendingRPC)
			select {
			case pending.ch <- rpcResponse{err: err}:
			default:
			}
		}
		return true
	})
}
