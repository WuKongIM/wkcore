package wkcluster

import (
	"net"
	"testing"
	"time"
)

func TestForwarder_RoundTrip(t *testing.T) {
	// Create a server that echoes forward requests as responses
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
		for {
			msgType, body, err := readMessage(conn)
			if err != nil {
				return
			}
			if msgType == msgTypeForward {
				reqID, _, cmd, _ := decodeForwardBody(body)
				resp := encodeRespBody(reqID, errCodeOK, cmd) // echo cmd as data
				_, _ = conn.Write(encodeMessage(msgTypeResp, resp))
			}
		}
	}()

	// Create forwarder with a direct connection
	d := NewStaticDiscovery([]NodeConfig{{NodeID: 2, Addr: ln.Addr().String()}})
	tr := NewTransport(1, d, 2)
	f := NewForwarder(1, tr, 5*time.Second)
	defer f.Stop()

	// Set up pool for nodeID=2
	pool := tr.getOrCreatePool(2)
	if pool == nil {
		t.Fatal("pool is nil")
	}

	// Get a connection and start read loop
	conn, idx, err := pool.getByGroup(1)
	if err != nil {
		t.Fatalf("getByGroup: %v", err)
	}
	pool.release(idx)
	f.ensureReadLoop(2, idx, conn)

	// Send forward
	requestID := f.nextReqID.Add(1)
	respCh := make(chan forwardResp, 1)
	f.pending.Store(requestID, respCh)

	body := encodeForwardBody(requestID, 1, []byte("test-cmd"))
	msg := encodeMessage(msgTypeForward, body)
	pool.mu[idx].Lock()
	_, err = conn.Write(msg)
	pool.mu[idx].Unlock()
	if err != nil {
		t.Fatal(err)
	}

	select {
	case resp := <-respCh:
		if resp.errCode != errCodeOK {
			t.Fatalf("expected errCodeOK, got %d", resp.errCode)
		}
		if string(resp.data) != "test-cmd" {
			t.Fatalf("expected echo, got %s", resp.data)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for response")
	}
}

func TestForwarder_Timeout(t *testing.T) {
	// Server that never responds
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	go func() {
		conn, _ := ln.Accept()
		if conn != nil {
			defer conn.Close()
			select {} // block forever
		}
	}()

	d := NewStaticDiscovery([]NodeConfig{{NodeID: 2, Addr: ln.Addr().String()}})
	tr := NewTransport(1, d, 2)
	f := NewForwarder(1, tr, 100*time.Millisecond)
	defer f.Stop()

	// Get connection
	pool := tr.getOrCreatePool(2)
	conn, idx, err := pool.getByGroup(1)
	if err != nil {
		t.Fatal(err)
	}
	pool.release(idx)
	f.ensureReadLoop(2, idx, conn)

	reqID := f.nextReqID.Add(1)
	respCh := make(chan forwardResp, 1)
	f.pending.Store(reqID, respCh)
	defer f.pending.Delete(reqID)

	body := encodeForwardBody(reqID, 1, []byte("test"))
	pool.mu[idx].Lock()
	_, _ = conn.Write(encodeMessage(msgTypeForward, body))
	pool.mu[idx].Unlock()

	select {
	case <-respCh:
		t.Fatal("should not receive response")
	case <-time.After(200 * time.Millisecond):
		// expected timeout
	}
}
