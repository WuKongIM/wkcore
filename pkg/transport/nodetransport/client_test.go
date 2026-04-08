package nodetransport

import (
	"bytes"
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

func TestClient_Send(t *testing.T) {
	s := NewServer()
	var received atomic.Int32
	s.Handle(1, func(_ net.Conn, body []byte) {
		if string(body) == "hello" {
			received.Add(1)
		}
	})
	if err := s.Start("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	defer s.Stop()

	d := &staticDiscovery{addrs: map[NodeID]string{2: s.Listener().Addr().String()}}
	pool := NewPool(d, 2, 5*time.Second)
	defer pool.Close()
	client := NewClient(pool)
	defer client.Stop()

	if err := client.Send(2, 0, 1, []byte("hello")); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	if received.Load() != 1 {
		t.Fatalf("expected 1, got %d", received.Load())
	}
}

func TestClient_RPC_RoundTrip(t *testing.T) {
	s := NewServer()
	s.HandleRPC(func(ctx context.Context, body []byte) ([]byte, error) {
		return append([]byte("echo:"), body...), nil
	})
	if err := s.Start("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	defer s.Stop()

	d := &staticDiscovery{addrs: map[NodeID]string{2: s.Listener().Addr().String()}}
	pool := NewPool(d, 2, 5*time.Second)
	defer pool.Close()
	client := NewClient(pool)
	defer client.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := client.RPC(ctx, 2, 0, []byte("ping"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(resp, []byte("echo:ping")) {
		t.Fatalf("expected echo:ping, got %q", resp)
	}
}

func TestClient_RPC_ContextCancel(t *testing.T) {
	s := NewServer()
	s.HandleRPC(func(ctx context.Context, body []byte) ([]byte, error) {
		time.Sleep(5 * time.Second)
		return nil, nil
	})
	if err := s.Start("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	defer s.Stop()

	d := &staticDiscovery{addrs: map[NodeID]string{2: s.Listener().Addr().String()}}
	pool := NewPool(d, 2, 5*time.Second)
	defer pool.Close()
	client := NewClient(pool)
	defer client.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	_, err := client.RPC(ctx, 2, 0, []byte("slow"))
	if err != context.DeadlineExceeded {
		t.Fatalf("expected DeadlineExceeded, got: %v", err)
	}
}

func TestClient_Stop_CancelsPending(t *testing.T) {
	s := NewServer()
	s.HandleRPC(func(ctx context.Context, body []byte) ([]byte, error) {
		time.Sleep(10 * time.Second)
		return nil, nil
	})
	if err := s.Start("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	defer s.Stop()

	d := &staticDiscovery{addrs: map[NodeID]string{2: s.Listener().Addr().String()}}
	pool := NewPool(d, 2, 5*time.Second)
	defer pool.Close()
	client := NewClient(pool)

	errCh := make(chan error, 1)
	go func() {
		ctx := context.Background()
		_, err := client.RPC(ctx, 2, 0, []byte("long"))
		errCh <- err
	}()

	time.Sleep(50 * time.Millisecond)
	client.Stop()

	select {
	case err := <-errCh:
		if err != ErrStopped {
			t.Fatalf("expected ErrStopped, got: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for RPC to cancel")
	}
}

func TestClient_RPC_ServerCloseFailsPendingImmediately(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	started := make(chan struct{}, 1)
	serverDone := make(chan struct{})
	go func() {
		defer close(serverDone)

		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()

		if _, _, err := ReadMessage(conn); err != nil {
			return
		}
		select {
		case started <- struct{}{}:
		default:
		}
	}()

	d := &staticDiscovery{addrs: map[NodeID]string{2: ln.Addr().String()}}
	pool := NewPool(d, 2, 5*time.Second)
	defer pool.Close()
	client := NewClient(pool)
	defer client.Stop()

	errCh := make(chan error, 1)
	startedAt := time.Now()
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		_, err := client.RPC(ctx, 2, 0, []byte("close"))
		errCh <- err
	}()

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for server handler to receive request")
	}

	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("expected RPC to fail after server closes connection")
		}
		if err == context.DeadlineExceeded {
			t.Fatalf("expected connection-close failure before request timeout, got %v after %s", err, time.Since(startedAt))
		}
		if elapsed := time.Since(startedAt); elapsed >= 2*time.Second {
			t.Fatalf("expected pending RPC to fail quickly after server close, took %s with err=%v", elapsed, err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for RPC failure after server close")
	}

	select {
	case <-serverDone:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for test server to exit")
	}
}

func TestClient_RPC_MultipleSequential(t *testing.T) {
	s := NewServer()
	var count atomic.Int32
	s.HandleRPC(func(ctx context.Context, body []byte) ([]byte, error) {
		n := count.Add(1)
		return []byte{byte(n)}, nil
	})
	if err := s.Start("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	defer s.Stop()

	d := &staticDiscovery{addrs: map[NodeID]string{2: s.Listener().Addr().String()}}
	pool := NewPool(d, 2, 5*time.Second)
	defer pool.Close()
	client := NewClient(pool)
	defer client.Stop()

	ctx := context.Background()
	for i := 0; i < 5; i++ {
		resp, err := client.RPC(ctx, 2, 0, []byte("req"))
		if err != nil {
			t.Fatalf("RPC %d: %v", i, err)
		}
		if resp[0] != byte(i+1) {
			t.Fatalf("expected %d, got %d", i+1, resp[0])
		}
	}
}
