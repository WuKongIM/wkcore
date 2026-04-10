package transport

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"
)

func TestClientSend(t *testing.T) {
	s := NewServer()
	received := make(chan []byte, 1)
	s.Handle(1, func(body []byte) {
		received <- append([]byte(nil), body...)
	})
	if err := s.Start("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	defer s.Stop()

	pool := NewPool(PoolConfig{
		Discovery:   staticDiscovery{addrs: map[NodeID]string{2: s.Listener().Addr().String()}},
		Size:        1,
		DialTimeout: time.Second,
		QueueSizes:  [numPriorities]int{4, 4, 4},
		DefaultPri:  PriorityRaft,
	})
	defer pool.Close()

	client := NewClient(pool)
	if err := client.Send(2, 0, 1, []byte("hello")); err != nil {
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

func TestClientRPCService(t *testing.T) {
	s := NewServer()
	mux := NewRPCMux()
	mux.Handle(7, func(ctx context.Context, body []byte) ([]byte, error) {
		return append([]byte("ok:"), body...), nil
	})
	s.HandleRPCMux(mux)
	if err := s.Start("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	defer s.Stop()

	pool := NewPool(PoolConfig{
		Discovery:   staticDiscovery{addrs: map[NodeID]string{2: s.Listener().Addr().String()}},
		Size:        1,
		DialTimeout: time.Second,
		QueueSizes:  [numPriorities]int{4, 4, 4},
		DefaultPri:  PriorityRPC,
	})
	defer pool.Close()

	client := NewClient(pool)
	resp, err := client.RPCService(context.Background(), 2, 0, 7, []byte("ping"))
	if err != nil {
		t.Fatalf("RPCService() error = %v", err)
	}
	if string(resp) != "ok:ping" {
		t.Fatalf("resp = %q, want %q", resp, "ok:ping")
	}
}

func TestClientStopClosesPoolConnections(t *testing.T) {
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
		drainConn(conn)
	}()

	pool := NewPool(PoolConfig{
		Discovery:   staticDiscovery{addrs: map[NodeID]string{2: ln.Addr().String()}},
		Size:        1,
		DialTimeout: time.Second,
		QueueSizes:  [numPriorities]int{4, 4, 4},
		DefaultPri:  PriorityRaft,
	})
	client := NewClient(pool)

	if err := client.Send(2, 0, 1, []byte("hello")); err != nil {
		t.Fatalf("Send() error = %v", err)
	}
	set, err := pool.getOrCreateNodeSet(2)
	if err != nil {
		t.Fatalf("getOrCreateNodeSet() error = %v", err)
	}
	requireEventually(t, func() bool {
		return set.conns[0].Load() != nil
	})

	client.Stop()

	requireEventually(t, func() bool {
		mc := set.conns[0].Load()
		return mc != nil && mc.closed.Load()
	})
}

func TestClientPendingRPCFailsWhenPoolCloses(t *testing.T) {
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
	client := NewClient(pool)
	defer pool.Close()

	errCh := make(chan error, 1)
	go func() {
		_, err := client.RPC(context.Background(), 2, 0, []byte("req"))
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
			t.Fatal("RPC() error = nil, want failure")
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for rpc failure")
	}
}

func TestClientStopFailsPendingRPCWithErrStopped(t *testing.T) {
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
	client := NewClient(pool)

	errCh := make(chan error, 1)
	go func() {
		_, err := client.RPC(context.Background(), 2, 0, []byte("req"))
		errCh <- err
	}()

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for rpc to start")
	}

	client.Stop()

	select {
	case err := <-errCh:
		if !errors.Is(err, ErrStopped) {
			t.Fatalf("RPC() error = %v, want %v", err, ErrStopped)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for rpc failure")
	}
}
