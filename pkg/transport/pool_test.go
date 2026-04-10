package transport

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

type staticDiscovery struct {
	addrs map[NodeID]string
}

func (d staticDiscovery) Resolve(nodeID NodeID) (string, error) {
	addr, ok := d.addrs[nodeID]
	if !ok {
		return "", ErrNodeNotFound
	}
	return addr, nil
}

func TestPoolSendDeliversMessage(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	received := make(chan []byte, 1)
	go acceptFrames(t, ln, func(msgType uint8, body []byte) {
		if msgType == 9 {
			received <- append([]byte(nil), body...)
		}
	})

	pool := NewPool(PoolConfig{
		Discovery:   staticDiscovery{addrs: map[NodeID]string{2: ln.Addr().String()}},
		Size:        2,
		DialTimeout: time.Second,
		QueueSizes:  [numPriorities]int{4, 4, 4},
		DefaultPri:  PriorityRaft,
	})
	defer pool.Close()

	if err := pool.Send(2, 0, 9, []byte("hello")); err != nil {
		t.Fatalf("Send() error = %v", err)
	}

	select {
	case body := <-received:
		if string(body) != "hello" {
			t.Fatalf("body = %q, want %q", body, "hello")
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for send")
	}
}

func TestPoolRPCRoundTrip(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		msgType, body, release, err := readFrame(conn)
		if err != nil {
			return
		}
		release()
		if msgType != MsgTypeRPCRequest {
			return
		}
		reqID := decodeRequestID(body)
		var bufs net.Buffers
		writeFrame(&bufs, MsgTypeRPCResponse, encodeRPCResponse(reqID, 0, []byte("pong")))
		_, _ = bufs.WriteTo(conn)
	}()

	pool := NewPool(PoolConfig{
		Discovery:   staticDiscovery{addrs: map[NodeID]string{2: ln.Addr().String()}},
		Size:        1,
		DialTimeout: time.Second,
		QueueSizes:  [numPriorities]int{4, 4, 4},
		DefaultPri:  PriorityRPC,
	})
	defer pool.Close()

	resp, err := pool.RPC(context.Background(), 2, 0, []byte("ping"))
	if err != nil {
		t.Fatalf("RPC() error = %v", err)
	}
	if string(resp) != "pong" {
		t.Fatalf("resp = %q, want %q", resp, "pong")
	}
}

func TestPoolNodeNotFound(t *testing.T) {
	pool := NewPool(PoolConfig{
		Discovery:   staticDiscovery{addrs: map[NodeID]string{}},
		Size:        1,
		DialTimeout: time.Second,
		QueueSizes:  [numPriorities]int{4, 4, 4},
		DefaultPri:  PriorityRPC,
	})
	defer pool.Close()

	err := pool.Send(99, 0, 1, []byte("x"))
	if !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("Send() error = %v, want %v", err, ErrNodeNotFound)
	}
}

func TestPoolReusesConnectionForSameShard(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	var accepted atomic.Int32
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			accepted.Add(1)
			go drainConn(conn)
		}
	}()

	pool := NewPool(PoolConfig{
		Discovery:   staticDiscovery{addrs: map[NodeID]string{2: ln.Addr().String()}},
		Size:        2,
		DialTimeout: time.Second,
		QueueSizes:  [numPriorities]int{4, 4, 4},
		DefaultPri:  PriorityRaft,
	})
	defer pool.Close()

	if err := pool.Send(2, 0, 1, []byte("a")); err != nil {
		t.Fatalf("first Send() error = %v", err)
	}
	if err := pool.Send(2, 2, 1, []byte("b")); err != nil {
		t.Fatalf("second Send() error = %v", err)
	}

	requireEventually(t, func() bool { return accepted.Load() == 1 })
}

func TestPoolCloseFailsPendingRPC(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	started := make(chan struct{}, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		if _, _, release, err := readFrame(conn); err == nil {
			release()
			started <- struct{}{}
			time.Sleep(time.Second)
		}
	}()

	pool := NewPool(PoolConfig{
		Discovery:   staticDiscovery{addrs: map[NodeID]string{2: ln.Addr().String()}},
		Size:        1,
		DialTimeout: time.Second,
		QueueSizes:  [numPriorities]int{4, 4, 4},
		DefaultPri:  PriorityRPC,
	})
	defer pool.Close()

	errCh := make(chan error, 1)
	go func() {
		_, err := pool.RPC(context.Background(), 2, 0, []byte("req"))
		errCh <- err
	}()

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for rpc to start")
	}

	pool.Close()

	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("RPC() error = nil, want failure after Close")
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for pending rpc failure")
	}
}

func acceptFrames(t *testing.T, ln net.Listener, fn func(uint8, []byte)) {
	t.Helper()
	conn, err := ln.Accept()
	if err != nil {
		return
	}
	defer conn.Close()
	for {
		msgType, body, release, err := readFrame(conn)
		if err != nil {
			return
		}
		fn(msgType, body)
		release()
	}
}

func drainConn(conn net.Conn) {
	defer conn.Close()
	for {
		_, _, release, err := readFrame(conn)
		if err != nil {
			return
		}
		release()
	}
}

func requireEventually(t *testing.T, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("condition not satisfied before timeout")
}
