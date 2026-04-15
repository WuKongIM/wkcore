package cluster

import (
	"context"
	"testing"
)

func TestTransportLayerStartInitializesServerAndClients(t *testing.T) {
	cfg := validTestConfig()
	cfg.ListenAddr = "127.0.0.1:0"
	discovery := NewStaticDiscovery(cfg.Nodes)
	layer := newTransportLayer(cfg, discovery, nil)

	err := layer.Start(
		cfg.ListenAddr,
		func([]byte) {},
		func(context.Context, []byte) ([]byte, error) { return nil, nil },
		func(context.Context, []byte) ([]byte, error) { return nil, nil },
		func(context.Context, []byte) ([]byte, error) { return nil, nil },
	)
	if err != nil {
		t.Fatalf("transportLayer.Start() error = %v", err)
	}
	t.Cleanup(layer.Stop)

	if layer.server == nil || layer.server.Listener() == nil {
		t.Fatal("transportLayer.Start() did not initialize server listener")
	}
	if layer.rpcMux == nil {
		t.Fatal("transportLayer.Start() did not initialize rpc mux")
	}
	if layer.raftClient == nil || layer.fwdClient == nil {
		t.Fatal("transportLayer.Start() did not initialize clients")
	}
}
