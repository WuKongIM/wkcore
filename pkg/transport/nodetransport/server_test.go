package nodetransport

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestServer_StartStop(t *testing.T) {
	s := NewServer()
	if err := s.Start("127.0.0.1:0"); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if s.Listener() == nil {
		t.Fatal("Listener is nil")
	}
	s.Stop()
}

func TestServer_HandleMessage(t *testing.T) {
	s := NewServer()

	var received atomic.Int32
	s.Handle(1, func(conn net.Conn, body []byte) {
		if string(body) == "ping" {
			received.Add(1)
		}
	})

	if err := s.Start("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	defer s.Stop()

	conn, err := net.Dial("tcp", s.Listener().Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	if err := WriteMessage(conn, 1, []byte("ping")); err != nil {
		t.Fatal(err)
	}

	time.Sleep(50 * time.Millisecond)
	if received.Load() != 1 {
		t.Fatalf("expected 1 message received, got %d", received.Load())
	}
}

func TestServer_HandleMultipleTypes(t *testing.T) {
	s := NewServer()

	var type1, type2 atomic.Int32
	s.Handle(1, func(_ net.Conn, _ []byte) { type1.Add(1) })
	s.Handle(2, func(_ net.Conn, _ []byte) { type2.Add(1) })

	if err := s.Start("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	defer s.Stop()

	conn, err := net.Dial("tcp", s.Listener().Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	WriteMessage(conn, 1, []byte("a"))
	WriteMessage(conn, 2, []byte("b"))
	WriteMessage(conn, 1, []byte("c"))

	time.Sleep(50 * time.Millisecond)
	if type1.Load() != 2 || type2.Load() != 1 {
		t.Fatalf("type1=%d type2=%d", type1.Load(), type2.Load())
	}
}

func TestServer_HandleRPC(t *testing.T) {
	s := NewServer()
	s.HandleRPC(func(ctx context.Context, body []byte) ([]byte, error) {
		return append([]byte("echo:"), body...), nil
	})

	if err := s.Start("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	defer s.Stop()

	conn, err := net.Dial("tcp", s.Listener().Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	reqBody := encodeRPCRequest(42, []byte("hello"))
	if err := WriteMessage(conn, MsgTypeRPCRequest, reqBody); err != nil {
		t.Fatal(err)
	}

	msgType, respBody, err := ReadMessage(conn)
	if err != nil {
		t.Fatal(err)
	}
	if msgType != MsgTypeRPCResponse {
		t.Fatalf("expected 0xFF, got %d", msgType)
	}
	reqID, errCode, data, err := decodeRPCResponse(respBody)
	if err != nil {
		t.Fatal(err)
	}
	if reqID != 42 || errCode != 0 || string(data) != "echo:hello" {
		t.Fatalf("unexpected: reqID=%d errCode=%d data=%q", reqID, errCode, data)
	}
}

func TestServer_UnregisteredType_Ignored(t *testing.T) {
	s := NewServer()
	if err := s.Start("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	defer s.Stop()

	conn, err := net.Dial("tcp", s.Listener().Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	WriteMessage(conn, 99, []byte("unknown"))
	time.Sleep(50 * time.Millisecond)
}

func TestServer_StopClosesConnections(t *testing.T) {
	s := NewServer()

	var connected sync.WaitGroup
	connected.Add(1)
	s.Handle(1, func(_ net.Conn, _ []byte) {
		connected.Done()
	})

	if err := s.Start("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}

	conn, err := net.Dial("tcp", s.Listener().Addr().String())
	if err != nil {
		t.Fatal(err)
	}

	WriteMessage(conn, 1, []byte("hi"))
	connected.Wait()

	s.Stop()

	_, _, err = ReadMessage(conn)
	if err == nil {
		t.Fatal("expected error after server Stop")
	}
	conn.Close()
}
