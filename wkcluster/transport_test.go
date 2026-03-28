package wkcluster

import (
	"context"
	"testing"
	"time"

	"github.com/WuKongIM/wraft/multiraft"
	"go.etcd.io/raft/v3/raftpb"
)

func TestTransport_StartStop(t *testing.T) {
	d := NewStaticDiscovery([]NodeConfig{
		{NodeID: 1, Addr: "127.0.0.1:0"},
	})
	tr := NewTransport(1, d, 4, defaultDialTimeout, defaultForwardTimeout)
	if err := tr.Start("127.0.0.1:0"); err != nil {
		t.Fatalf("Start: %v", err)
	}
	tr.Stop()
}

func TestTransport_SendReceiveRaft(t *testing.T) {
	// Start receiver
	recvDiscovery := NewStaticDiscovery(nil)
	recv := NewTransport(2, recvDiscovery, 4, defaultDialTimeout, defaultForwardTimeout)
	if err := recv.Start("127.0.0.1:0"); err != nil {
		t.Fatalf("Start recv: %v", err)
	}
	defer recv.Stop()
	recvAddr := recv.listener.Addr().String()

	// Start sender
	sendDiscovery := NewStaticDiscovery([]NodeConfig{
		{NodeID: 2, Addr: recvAddr},
	})
	sender := NewTransport(1, sendDiscovery, 4, defaultDialTimeout, defaultForwardTimeout)
	if err := sender.Start("127.0.0.1:0"); err != nil {
		t.Fatalf("Start sender: %v", err)
	}
	defer sender.Stop()

	// Send a raft message
	err := sender.Send(context.Background(), []multiraft.Envelope{
		{GroupID: 1, Message: raftpb.Message{To: 2, From: 1, Type: raftpb.MsgHeartbeat}},
	})
	if err != nil {
		t.Fatalf("Send: %v", err)
	}

	// Give time for delivery
	time.Sleep(50 * time.Millisecond)
}
