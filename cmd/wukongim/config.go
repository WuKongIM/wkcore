package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/app"
	"github.com/WuKongIM/WuKongIM/internal/gateway"
)

type wireConfig struct {
	Node    wireNodeConfig    `json:"node"`
	Storage wireStorageConfig `json:"storage"`
	Cluster wireClusterConfig `json:"cluster"`
	API     wireAPIConfig     `json:"api"`
	Gateway wireGatewayConfig `json:"gateway"`
}

type wireNodeConfig struct {
	ID      uint64 `json:"id"`
	Name    string `json:"name"`
	DataDir string `json:"dataDir"`
}

type wireStorageConfig struct {
	DBPath   string `json:"dbPath"`
	RaftPath string `json:"raftPath"`
}

type wireClusterConfig struct {
	ListenAddr          string              `json:"listenAddr"`
	GroupCount          uint32              `json:"groupCount"`
	Nodes               []wireNodeConfigRef `json:"nodes"`
	Groups              []wireGroupConfig   `json:"groups"`
	ForwardTimeout      string              `json:"forwardTimeout"`
	PoolSize            int                 `json:"poolSize"`
	TickInterval        string              `json:"tickInterval"`
	RaftWorkers         int                 `json:"raftWorkers"`
	ElectionTick        int                 `json:"electionTick"`
	HeartbeatTick       int                 `json:"heartbeatTick"`
	DialTimeout         string              `json:"dialTimeout"`
	DataPlaneRPCTimeout string              `json:"dataPlaneRPCTimeout"`
}

type wireNodeConfigRef struct {
	ID   uint64 `json:"id"`
	Addr string `json:"addr"`
}

type wireGroupConfig struct {
	ID    uint32   `json:"id"`
	Peers []uint64 `json:"peers"`
}

type wireGatewayConfig struct {
	TokenAuthOn    bool                      `json:"tokenAuthOn"`
	DefaultSession wireSessionOptions        `json:"defaultSession"`
	Listeners      []gateway.ListenerOptions `json:"listeners"`
}

type wireAPIConfig struct {
	ListenAddr string `json:"listenAddr"`
}

type wireSessionOptions struct {
	ReadBufferSize      int    `json:"readBufferSize"`
	WriteQueueSize      int    `json:"writeQueueSize"`
	MaxInboundBytes     int    `json:"maxInboundBytes"`
	MaxOutboundBytes    int    `json:"maxOutboundBytes"`
	IdleTimeout         string `json:"idleTimeout"`
	WriteTimeout        string `json:"writeTimeout"`
	CloseOnHandlerError *bool  `json:"closeOnHandlerError"`
}

func loadConfig(path string) (app.Config, error) {
	if strings.TrimSpace(path) == "" {
		return app.Config{}, fmt.Errorf("load config: config path is required")
	}

	body, err := os.ReadFile(path)
	if err != nil {
		return app.Config{}, fmt.Errorf("load config: read %s: %w", path, err)
	}

	var wire wireConfig
	dec := json.NewDecoder(strings.NewReader(string(body)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&wire); err != nil {
		return app.Config{}, fmt.Errorf("load config: decode %s: %w", path, err)
	}

	cfg, err := wire.toAppConfig()
	if err != nil {
		return app.Config{}, fmt.Errorf("load config: %w", err)
	}
	return cfg, nil
}

func (c wireConfig) toAppConfig() (app.Config, error) {
	forwardTimeout, err := parseDuration("cluster.forwardTimeout", c.Cluster.ForwardTimeout)
	if err != nil {
		return app.Config{}, err
	}
	tickInterval, err := parseDuration("cluster.tickInterval", c.Cluster.TickInterval)
	if err != nil {
		return app.Config{}, err
	}
	dialTimeout, err := parseDuration("cluster.dialTimeout", c.Cluster.DialTimeout)
	if err != nil {
		return app.Config{}, err
	}
	dataPlaneRPCTimeout, err := parseDuration("cluster.dataPlaneRPCTimeout", c.Cluster.DataPlaneRPCTimeout)
	if err != nil {
		return app.Config{}, err
	}
	idleTimeout, err := parseDuration("gateway.defaultSession.idleTimeout", c.Gateway.DefaultSession.IdleTimeout)
	if err != nil {
		return app.Config{}, err
	}
	writeTimeout, err := parseDuration("gateway.defaultSession.writeTimeout", c.Gateway.DefaultSession.WriteTimeout)
	if err != nil {
		return app.Config{}, err
	}

	cfg := app.Config{
		Node: app.NodeConfig{
			ID:      c.Node.ID,
			Name:    c.Node.Name,
			DataDir: c.Node.DataDir,
		},
		Storage: app.StorageConfig{
			DBPath:   c.Storage.DBPath,
			RaftPath: c.Storage.RaftPath,
		},
		Cluster: app.ClusterConfig{
			ListenAddr:          c.Cluster.ListenAddr,
			GroupCount:          c.Cluster.GroupCount,
			Nodes:               make([]app.NodeConfigRef, 0, len(c.Cluster.Nodes)),
			Groups:              make([]app.GroupConfig, 0, len(c.Cluster.Groups)),
			ForwardTimeout:      forwardTimeout,
			PoolSize:            c.Cluster.PoolSize,
			TickInterval:        tickInterval,
			RaftWorkers:         c.Cluster.RaftWorkers,
			ElectionTick:        c.Cluster.ElectionTick,
			HeartbeatTick:       c.Cluster.HeartbeatTick,
			DialTimeout:         dialTimeout,
			DataPlaneRPCTimeout: dataPlaneRPCTimeout,
		},
		API: app.APIConfig{
			ListenAddr: c.API.ListenAddr,
		},
		Gateway: app.GatewayConfig{
			TokenAuthOn: c.Gateway.TokenAuthOn,
			DefaultSession: gateway.SessionOptions{
				ReadBufferSize:      c.Gateway.DefaultSession.ReadBufferSize,
				WriteQueueSize:      c.Gateway.DefaultSession.WriteQueueSize,
				MaxInboundBytes:     c.Gateway.DefaultSession.MaxInboundBytes,
				MaxOutboundBytes:    c.Gateway.DefaultSession.MaxOutboundBytes,
				IdleTimeout:         idleTimeout,
				WriteTimeout:        writeTimeout,
				CloseOnHandlerError: c.Gateway.DefaultSession.CloseOnHandlerError,
			},
			Listeners: append([]gateway.ListenerOptions(nil), c.Gateway.Listeners...),
		},
	}

	for _, node := range c.Cluster.Nodes {
		cfg.Cluster.Nodes = append(cfg.Cluster.Nodes, app.NodeConfigRef{
			ID:   node.ID,
			Addr: node.Addr,
		})
	}
	for _, group := range c.Cluster.Groups {
		cfg.Cluster.Groups = append(cfg.Cluster.Groups, app.GroupConfig{
			ID:    group.ID,
			Peers: append([]uint64(nil), group.Peers...),
		})
	}

	return cfg, nil
}

func parseDuration(field, value string) (time.Duration, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0, nil
	}
	d, err := time.ParseDuration(value)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", field, err)
	}
	return d, nil
}
