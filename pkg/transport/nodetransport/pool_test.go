package nodetransport

import (
	"net"
	"sync"
	"testing"
	"time"
)

// staticDiscovery is a test helper implementing Discovery.
type staticDiscovery struct {
	addrs map[NodeID]string
}

func (d *staticDiscovery) Resolve(nodeID NodeID) (string, error) {
	addr, ok := d.addrs[nodeID]
	if !ok {
		return "", ErrNodeNotFound
	}
	return addr, nil
}

func TestPool_GetRelease(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			defer conn.Close()
		}
	}()

	d := &staticDiscovery{addrs: map[NodeID]string{2: ln.Addr().String()}}
	pool := NewPool(d, 2, 5*time.Second)
	defer pool.Close()

	conn, idx, err := pool.Get(2, 0)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if conn == nil {
		t.Fatal("conn is nil")
	}
	pool.Release(2, idx)
}

func TestPool_GetError_NoRelease(t *testing.T) {
	d := &staticDiscovery{addrs: map[NodeID]string{2: "127.0.0.1:1"}}
	pool := NewPool(d, 2, 100*time.Millisecond)
	defer pool.Close()

	_, _, err := pool.Get(2, 0)
	if err == nil {
		t.Fatal("expected error for bad address")
	}
	_, _, err = pool.Get(2, 0)
	if err == nil {
		t.Fatal("expected error again")
	}
}

func TestPool_NodeNotFound(t *testing.T) {
	d := &staticDiscovery{addrs: map[NodeID]string{}}
	pool := NewPool(d, 2, 5*time.Second)
	defer pool.Close()

	_, _, err := pool.Get(99, 0)
	if err != ErrNodeNotFound {
		t.Fatalf("expected ErrNodeNotFound, got: %v", err)
	}
}

func TestPool_Reset_Reconnects(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			_ = conn
		}
	}()

	d := &staticDiscovery{addrs: map[NodeID]string{2: ln.Addr().String()}}
	pool := NewPool(d, 2, 5*time.Second)
	defer pool.Close()

	conn1, idx, err := pool.Get(2, 0)
	if err != nil {
		t.Fatal(err)
	}
	pool.Reset(2, idx)
	pool.Release(2, idx)

	conn2, idx2, err := pool.Get(2, 0)
	if err != nil {
		t.Fatal(err)
	}
	if conn1 == conn2 {
		t.Fatal("expected new connection after Reset")
	}
	pool.Release(2, idx2)
}

func TestPool_ShardKey(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			_ = conn
		}
	}()

	d := &staticDiscovery{addrs: map[NodeID]string{2: ln.Addr().String()}}
	pool := NewPool(d, 4, 5*time.Second)
	defer pool.Close()

	_, idx0, err := pool.Get(2, 0)
	if err != nil {
		t.Fatal(err)
	}
	pool.Release(2, idx0)

	_, idx4, err := pool.Get(2, 4)
	if err != nil {
		t.Fatal(err)
	}
	pool.Release(2, idx4)

	if idx0 != idx4 {
		t.Fatalf("expected same index for shardKey 0 and 4, got %d and %d", idx0, idx4)
	}
}

func TestPool_Concurrent(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			_ = conn
		}
	}()

	d := &staticDiscovery{addrs: map[NodeID]string{2: ln.Addr().String()}}
	pool := NewPool(d, 4, 5*time.Second)
	defer pool.Close()

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(key uint64) {
			defer wg.Done()
			conn, idx, err := pool.Get(2, key)
			if err != nil {
				t.Errorf("Get(%d): %v", key, err)
				return
			}
			if conn == nil {
				t.Errorf("Get(%d): nil conn", key)
			}
			pool.Release(2, idx)
		}(uint64(i))
	}
	wg.Wait()
}
