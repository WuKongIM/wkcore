package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/app"
	"github.com/WuKongIM/WuKongIM/internal/gateway"
	"github.com/WuKongIM/WuKongIM/internal/gateway/binding"
	"github.com/spf13/viper"
)

var defaultConfigPaths = []string{
	"./wukongim.conf",
	"./conf/wukongim.conf",
	"/etc/wukongim/wukongim.conf",
}

func loadConfig(path string) (app.Config, error) {
	cfgv, foundFile, attemptedPaths, err := readConfig(path)
	if err != nil {
		return app.Config{}, err
	}

	cfg, err := buildAppConfig(cfgv)
	if err != nil {
		if !foundFile {
			return app.Config{}, missingDefaultConfigError(attemptedPaths, err)
		}
		return app.Config{}, fmt.Errorf("load config: %w", err)
	}
	if err := cfg.ApplyDefaultsAndValidate(); err != nil {
		if !foundFile {
			return app.Config{}, missingDefaultConfigError(attemptedPaths, err)
		}
		return app.Config{}, fmt.Errorf("load config: %w", err)
	}
	return cfg, nil
}

func readConfig(path string) (*viper.Viper, bool, []string, error) {
	v := viper.New()
	v.SetConfigType("env")
	v.AutomaticEnv()

	path = strings.TrimSpace(path)
	if path != "" {
		v.SetConfigFile(path)
		if err := v.ReadInConfig(); err != nil {
			return nil, false, nil, fmt.Errorf("load config: read %s: %w", path, err)
		}
		return v, true, nil, nil
	}

	attemptedPaths := append([]string(nil), defaultConfigPaths...)
	for _, candidate := range attemptedPaths {
		if _, err := os.Stat(candidate); err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return nil, false, attemptedPaths, fmt.Errorf("load config: stat %s: %w", candidate, err)
		}
		v.SetConfigFile(candidate)
		if err := v.ReadInConfig(); err != nil {
			return nil, false, attemptedPaths, fmt.Errorf("load config: read %s: %w", candidate, err)
		}
		return v, true, attemptedPaths, nil
	}

	return v, false, attemptedPaths, nil
}

func buildAppConfig(v *viper.Viper) (app.Config, error) {
	if raw := strings.TrimSpace(stringValue(v, "WK_CLUSTER_GROUP_COUNT")); raw != "" {
		return app.Config{}, fmt.Errorf("%w: WK_CLUSTER_GROUP_COUNT is no longer supported; use WK_CLUSTER_SLOT_COUNT", app.ErrInvalidConfig)
	}
	if raw := strings.TrimSpace(stringValue(v, "WK_CLUSTER_GROUP_REPLICA_N")); raw != "" {
		return app.Config{}, fmt.Errorf("%w: WK_CLUSTER_GROUP_REPLICA_N is no longer supported; use WK_CLUSTER_SLOT_REPLICA_N", app.ErrInvalidConfig)
	}
	nodeID, err := parseUint64(v, "WK_NODE_ID")
	if err != nil {
		return app.Config{}, err
	}
	slotCount, err := parseUint32(v, "WK_CLUSTER_SLOT_COUNT")
	if err != nil {
		return app.Config{}, err
	}
	forwardTimeout, err := parseDuration(v, "WK_CLUSTER_FORWARD_TIMEOUT")
	if err != nil {
		return app.Config{}, err
	}
	poolSize, err := parseInt(v, "WK_CLUSTER_POOL_SIZE")
	if err != nil {
		return app.Config{}, err
	}
	tickInterval, err := parseDuration(v, "WK_CLUSTER_TICK_INTERVAL")
	if err != nil {
		return app.Config{}, err
	}
	raftWorkers, err := parseInt(v, "WK_CLUSTER_RAFT_WORKERS")
	if err != nil {
		return app.Config{}, err
	}
	electionTick, err := parseInt(v, "WK_CLUSTER_ELECTION_TICK")
	if err != nil {
		return app.Config{}, err
	}
	heartbeatTick, err := parseInt(v, "WK_CLUSTER_HEARTBEAT_TICK")
	if err != nil {
		return app.Config{}, err
	}
	dialTimeout, err := parseDuration(v, "WK_CLUSTER_DIAL_TIMEOUT")
	if err != nil {
		return app.Config{}, err
	}
	dataPlaneRPCTimeout, err := parseDuration(v, "WK_CLUSTER_DATA_PLANE_RPC_TIMEOUT")
	if err != nil {
		return app.Config{}, err
	}
	controllerReplicaN, err := parseInt(v, "WK_CLUSTER_CONTROLLER_REPLICA_N")
	if err != nil {
		return app.Config{}, err
	}
	slotReplicaN, err := parseInt(v, "WK_CLUSTER_SLOT_REPLICA_N")
	if err != nil {
		return app.Config{}, err
	}
	tokenAuthOn, err := parseBool(v, "WK_GATEWAY_TOKEN_AUTH_ON")
	if err != nil {
		return app.Config{}, err
	}

	nodes, err := parseJSONValue[[]app.NodeConfigRef](v, "WK_CLUSTER_NODES")
	if err != nil {
		return app.Config{}, err
	}
	if raw := strings.TrimSpace(stringValue(v, "WK_CLUSTER_GROUPS")); raw != "" {
		return app.Config{}, fmt.Errorf("%w: WK_CLUSTER_GROUPS is no longer supported; remove static slot peers and keep WK_CLUSTER_SLOT_COUNT only", app.ErrInvalidConfig)
	}
	listeners, err := parseListeners(v)
	if err != nil {
		return app.Config{}, err
	}
	closeOnHandlerError, err := parseOptionalBool(v, "WK_GATEWAY_DEFAULT_SESSION_CLOSE_ON_HANDLER_ERROR")
	if err != nil {
		return app.Config{}, err
	}
	readBufferSize, err := parseInt(v, "WK_GATEWAY_DEFAULT_SESSION_READ_BUFFER_SIZE")
	if err != nil {
		return app.Config{}, err
	}
	writeQueueSize, err := parseInt(v, "WK_GATEWAY_DEFAULT_SESSION_WRITE_QUEUE_SIZE")
	if err != nil {
		return app.Config{}, err
	}
	maxInboundBytes, err := parseInt(v, "WK_GATEWAY_DEFAULT_SESSION_MAX_INBOUND_BYTES")
	if err != nil {
		return app.Config{}, err
	}
	maxOutboundBytes, err := parseInt(v, "WK_GATEWAY_DEFAULT_SESSION_MAX_OUTBOUND_BYTES")
	if err != nil {
		return app.Config{}, err
	}
	idleTimeout, err := parseDuration(v, "WK_GATEWAY_DEFAULT_SESSION_IDLE_TIMEOUT")
	if err != nil {
		return app.Config{}, err
	}
	writeTimeout, err := parseDuration(v, "WK_GATEWAY_DEFAULT_SESSION_WRITE_TIMEOUT")
	if err != nil {
		return app.Config{}, err
	}
	asyncSendDispatch, err := parseBool(v, "WK_GATEWAY_DEFAULT_SESSION_ASYNC_SEND_DISPATCH")
	if err != nil {
		return app.Config{}, err
	}
	logMaxSize, err := parseInt(v, "WK_LOG_MAX_SIZE")
	if err != nil {
		return app.Config{}, err
	}
	logMaxAge, err := parseInt(v, "WK_LOG_MAX_AGE")
	if err != nil {
		return app.Config{}, err
	}
	logMaxBackups, err := parseInt(v, "WK_LOG_MAX_BACKUPS")
	if err != nil {
		return app.Config{}, err
	}
	logCompress, err := parseBool(v, "WK_LOG_COMPRESS")
	if err != nil {
		return app.Config{}, err
	}
	logConsole, err := parseBool(v, "WK_LOG_CONSOLE")
	if err != nil {
		return app.Config{}, err
	}

	cfg := app.Config{
		Node: app.NodeConfig{
			ID:      nodeID,
			Name:    stringValue(v, "WK_NODE_NAME"),
			DataDir: stringValue(v, "WK_NODE_DATA_DIR"),
		},
		Storage: app.StorageConfig{
			DBPath:             stringValue(v, "WK_STORAGE_DB_PATH"),
			RaftPath:           stringValue(v, "WK_STORAGE_RAFT_PATH"),
			ChannelLogPath:     stringValue(v, "WK_STORAGE_CHANNEL_LOG_PATH"),
			ControllerMetaPath: stringValue(v, "WK_STORAGE_CONTROLLER_META_PATH"),
			ControllerRaftPath: stringValue(v, "WK_STORAGE_CONTROLLER_RAFT_PATH"),
		},
		Cluster: app.ClusterConfig{
			ListenAddr:          stringValue(v, "WK_CLUSTER_LISTEN_ADDR"),
			SlotCount:           slotCount,
			Nodes:               nodes,
			ControllerReplicaN:  controllerReplicaN,
			SlotReplicaN:        slotReplicaN,
			ForwardTimeout:      forwardTimeout,
			PoolSize:            poolSize,
			TickInterval:        tickInterval,
			RaftWorkers:         raftWorkers,
			ElectionTick:        electionTick,
			HeartbeatTick:       heartbeatTick,
			DialTimeout:         dialTimeout,
			DataPlaneRPCTimeout: dataPlaneRPCTimeout,
		},
		API: app.APIConfig{
			ListenAddr: defaultAPIListenAddr,
		},
		Gateway: app.GatewayConfig{
			TokenAuthOn: tokenAuthOn,
			DefaultSession: gateway.SessionOptions{
				ReadBufferSize:      readBufferSize,
				WriteQueueSize:      writeQueueSize,
				MaxInboundBytes:     maxInboundBytes,
				MaxOutboundBytes:    maxOutboundBytes,
				IdleTimeout:         idleTimeout,
				WriteTimeout:        writeTimeout,
				AsyncSendDispatch:   asyncSendDispatch,
				CloseOnHandlerError: closeOnHandlerError,
			},
			Listeners: listeners,
		},
		Log: app.LogConfig{
			Level:      stringValue(v, "WK_LOG_LEVEL"),
			Dir:        stringValue(v, "WK_LOG_DIR"),
			MaxSize:    logMaxSize,
			MaxAge:     logMaxAge,
			MaxBackups: logMaxBackups,
			Compress:   logCompress,
			Console:    logConsole,
			Format:     stringValue(v, "WK_LOG_FORMAT"),
		},
	}
	cfg.Log.SetExplicitFlags(stringValue(v, "WK_LOG_COMPRESS") != "", stringValue(v, "WK_LOG_CONSOLE") != "")

	if listenAddr := stringValue(v, "WK_API_LISTEN_ADDR"); listenAddr != "" {
		cfg.API.ListenAddr = listenAddr
	}

	return cfg, nil
}

func missingDefaultConfigError(attemptedPaths []string, err error) error {
	return fmt.Errorf(
		"load config: no config file found in default paths %s: %w",
		strings.Join(attemptedPaths, ", "),
		err,
	)
}

const defaultAPIListenAddr = "0.0.0.0:5001"

func defaultGatewayListeners() []gateway.ListenerOptions {
	return []gateway.ListenerOptions{
		binding.TCPWKProto("tcp-wkproto", "0.0.0.0:5100"),
		binding.WSJSONRPC("ws-jsonrpc", "0.0.0.0:5200"),
	}
}

func parseListeners(v *viper.Viper) ([]gateway.ListenerOptions, error) {
	raw := stringValue(v, "WK_GATEWAY_LISTENERS")
	if raw == "" {
		return defaultGatewayListeners(), nil
	}

	listeners, err := parseJSONValue[[]gateway.ListenerOptions](v, "WK_GATEWAY_LISTENERS")
	if err != nil {
		return nil, err
	}
	return listeners, nil
}

func parseJSONValue[T any](v *viper.Viper, key string) (T, error) {
	var zero T

	raw := stringValue(v, key)
	if raw == "" {
		return zero, nil
	}

	var value T
	if err := v.UnmarshalKey(key, &value); err == nil {
		return value, nil
	}

	if err := jsonUnmarshalString(raw, &value); err != nil {
		return zero, fmt.Errorf("parse %s as JSON: %w", key, err)
	}
	return value, nil
}

func parseUint64(v *viper.Viper, key string) (uint64, error) {
	raw := stringValue(v, key)
	if raw == "" {
		return 0, nil
	}

	value, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse %s: %w", key, err)
	}
	return value, nil
}

func parseUint32(v *viper.Viper, key string) (uint32, error) {
	raw := stringValue(v, key)
	if raw == "" {
		return 0, nil
	}

	value, err := strconv.ParseUint(raw, 10, 32)
	if err != nil {
		return 0, fmt.Errorf("parse %s: %w", key, err)
	}
	return uint32(value), nil
}

func parseInt(v *viper.Viper, key string) (int, error) {
	raw := stringValue(v, key)
	if raw == "" {
		return 0, nil
	}

	value, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("parse %s: %w", key, err)
	}
	return value, nil
}

func parseBool(v *viper.Viper, key string) (bool, error) {
	raw := stringValue(v, key)
	if raw == "" {
		return false, nil
	}

	value, err := strconv.ParseBool(raw)
	if err != nil {
		return false, fmt.Errorf("parse %s: %w", key, err)
	}
	return value, nil
}

func parseOptionalBool(v *viper.Viper, key string) (*bool, error) {
	raw := stringValue(v, key)
	if raw == "" {
		return nil, nil
	}

	value, err := strconv.ParseBool(raw)
	if err != nil {
		return nil, fmt.Errorf("parse %s: %w", key, err)
	}
	return &value, nil
}

func parseDuration(v *viper.Viper, key string) (time.Duration, error) {
	raw := stringValue(v, key)
	if raw == "" {
		return 0, nil
	}

	value, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("parse %s: %w", key, err)
	}
	return value, nil
}

func stringValue(v *viper.Viper, key string) string {
	return strings.TrimSpace(v.GetString(key))
}

func jsonUnmarshalString[T any](raw string, value *T) error {
	return json.Unmarshal([]byte(raw), value)
}
