package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLoadConfigParsesJSONFileIntoAppConfig(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "node-1")
	configPath := filepath.Join(t.TempDir(), "wukongim.json")

	payload := map[string]any{
		"node": map[string]any{
			"id":      1,
			"dataDir": dataDir,
		},
		"cluster": map[string]any{
			"listenAddr": "127.0.0.1:7000",
			"nodes": []map[string]any{
				{"id": 1, "addr": "127.0.0.1:7000"},
			},
			"groups": []map[string]any{
				{"id": 1, "peers": []uint64{1}},
			},
		},
		"gateway": map[string]any{
			"listeners": []map[string]any{
				{
					"name":      "tcp-wkproto",
					"network":   "tcp",
					"address":   "127.0.0.1:5100",
					"transport": "stdnet",
					"protocol":  "wkproto",
				},
			},
		},
	}

	body, err := json.Marshal(payload)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(configPath, body, 0o644))

	cfg, err := loadConfig(configPath)
	require.NoError(t, err)
	require.Equal(t, uint64(1), cfg.Node.ID)
	require.Equal(t, dataDir, cfg.Node.DataDir)
	require.Equal(t, "127.0.0.1:7000", cfg.Cluster.ListenAddr)
	require.Len(t, cfg.Cluster.Nodes, 1)
	require.Equal(t, uint64(1), cfg.Cluster.Nodes[0].ID)
	require.Equal(t, "127.0.0.1:7000", cfg.Cluster.Nodes[0].Addr)
	require.Len(t, cfg.Cluster.Groups, 1)
	require.Equal(t, uint32(1), cfg.Cluster.Groups[0].ID)
	require.Equal(t, []uint64{1}, cfg.Cluster.Groups[0].Peers)
	require.Len(t, cfg.Gateway.Listeners, 1)
	require.Equal(t, "tcp-wkproto", cfg.Gateway.Listeners[0].Name)
	require.Equal(t, "tcp", cfg.Gateway.Listeners[0].Network)
	require.Equal(t, "127.0.0.1:5100", cfg.Gateway.Listeners[0].Address)
	require.Equal(t, "stdnet", cfg.Gateway.Listeners[0].Transport)
	require.Equal(t, "wkproto", cfg.Gateway.Listeners[0].Protocol)
}

func TestLoadConfigRejectsMissingConfigPath(t *testing.T) {
	_, err := loadConfig("")
	require.ErrorContains(t, err, "config path")
}
