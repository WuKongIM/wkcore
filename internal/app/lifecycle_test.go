package app

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"github.com/WuKongIM/WuKongIM/internal/gateway"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/raftcluster"
	"github.com/WuKongIM/WuKongIM/pkg/storage/metadb"
	"github.com/WuKongIM/WuKongIM/pkg/storage/raftstorage"
	"github.com/stretchr/testify/require"
)

func TestNewBuildsDBClusterStoreMessageAndGatewayAdapter(t *testing.T) {
	cfg := testConfig(t)

	app, err := New(cfg)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, app.RaftDB().Close())
		require.NoError(t, app.DB().Close())
	})

	require.NotNil(t, app.DB())
	require.NotNil(t, app.RaftDB())
	require.NotNil(t, app.Cluster())
	require.NotNil(t, app.Store())
	require.NotNil(t, app.Message())
	require.NotNil(t, app.GatewayHandler())
	require.NotNil(t, app.Gateway())
	require.Nil(t, app.API())
}

func TestNewBuildsOptionalAPIServerWhenConfigured(t *testing.T) {
	cfg := testConfig(t)
	cfg.API.ListenAddr = "127.0.0.1:0"

	app, err := New(cfg)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, app.RaftDB().Close())
		require.NoError(t, app.DB().Close())
	})

	require.NotNil(t, app.API())
}

func TestNewBuildsChannelLogDataPlane(t *testing.T) {
	cfg := testConfig(t)

	app, err := New(cfg)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, app.Stop())
	})

	require.NotNil(t, app.ChannelLogDB())
	require.NotNil(t, app.ISRRuntime())
	require.NotNil(t, app.ChannelLog())
}

func TestNewConfiguresISRMaxFetchInflightPeerWithMinimumConcurrency(t *testing.T) {
	cfg := testConfig(t)
	cfg.Cluster.PoolSize = 1

	app, err := New(cfg)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, app.Stop())
	})

	require.Equal(t, 2, appISRMaxFetchInflightPeerLimit(t, app))
}

func TestNewConfiguresISRMaxFetchInflightPeerFromClusterPoolSize(t *testing.T) {
	cfg := testConfig(t)
	cfg.Cluster.PoolSize = 4

	app, err := New(cfg)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, app.Stop())
	})

	require.Equal(t, 4, appISRMaxFetchInflightPeerLimit(t, app))
}

func TestNewConfiguresIndependentDataPlaneLimits(t *testing.T) {
	cfg := testConfig(t)
	cfg.Cluster.PoolSize = 1
	setClusterConfigIntField(t, &cfg.Cluster, "DataPlaneMaxFetchInflight", 7)

	app, err := New(cfg)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, app.Stop())
	})

	require.Equal(t, 7, appISRMaxFetchInflightPeerLimit(t, app))
}

func TestStartChannelMetaSyncUsesExplicitDataPlaneSettings(t *testing.T) {
	cfg := testConfig(t)
	cfg.Cluster.PoolSize = 1
	setClusterConfigIntField(t, &cfg.Cluster, "DataPlanePoolSize", 9)
	setClusterConfigIntField(t, &cfg.Cluster, "DataPlaneMaxPendingFetch", 11)

	app, err := New(cfg)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, app.Stop())
	})

	require.NoError(t, app.startCluster())
	app.clusterOn.Store(true)
	require.NoError(t, app.startChannelMetaSync())
	app.channelMetaOn.Store(true)
	require.Equal(t, 9, appDataPlanePoolSize(t, app))
	require.Equal(t, 11, appDataPlaneAdapterMaxPendingFetch(t, app))
}

func TestBuildCreatesPresenceAppAndNodeAccess(t *testing.T) {
	cfg := testConfig(t)

	app, err := New(cfg)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, app.Stop())
	})

	requireAppFieldNonNil(t, app, "presenceApp")
	requireAppFieldNonNil(t, app, "nodeClient")
	requireAppFieldNonNil(t, app, "nodeAccess")
	requireAppFieldNonNil(t, app, "presenceWorker")
}

func TestNewReturnsConfigErrorsBeforeOpeningResources(t *testing.T) {
	cfg := testConfig(t)
	cfg.Node.ID = 0

	_, err := New(cfg)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrInvalidConfig)

	_, dbErr := os.Stat(cfg.Storage.DBPath)
	require.ErrorIs(t, dbErr, os.ErrNotExist)

	_, raftErr := os.Stat(cfg.Storage.RaftPath)
	require.ErrorIs(t, raftErr, os.ErrNotExist)
}

func TestAccessorsExposeBuiltRuntime(t *testing.T) {
	cfg := testConfig(t)

	app, err := New(cfg)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, app.Stop())
	})

	require.Same(t, app.db, app.DB())
	require.Same(t, app.raftDB, app.RaftDB())
	require.Same(t, app.cluster, app.Cluster())
	require.Same(t, app.channelLogDB, app.ChannelLogDB())
	require.Same(t, app.isrRuntime, app.ISRRuntime())
	require.Same(t, app.channelLog, app.ChannelLog())
	require.Same(t, app.store, app.Store())
	require.Same(t, app.messageApp, app.Message())
	require.Same(t, app.gatewayHandler, app.GatewayHandler())
	require.Same(t, app.gateway, app.Gateway())
	require.Same(t, app.api, app.API())
}

func TestNewClosesOpenedStoresWhenGatewayBuildFails(t *testing.T) {
	cfg := testConfig(t)
	dup := cfg.Gateway.Listeners[0]
	dup.Name = dup.Name + "-dup"
	cfg.Gateway.Listeners = append(cfg.Gateway.Listeners, dup)

	_, err := New(cfg)
	require.Error(t, err)
	require.ErrorContains(t, err, "duplicate listener address")

	reopenedDB, dbOpenErr := openWKDBForTest(cfg.Storage.DBPath)
	require.NoError(t, dbOpenErr)
	require.NoError(t, reopenedDB.Close())

	reopenedRaft, raftOpenErr := openRaftDBForTest(cfg.Storage.RaftPath)
	require.NoError(t, raftOpenErr)
	require.NoError(t, reopenedRaft.Close())
}

func TestStartStartsClusterBeforeGateway(t *testing.T) {
	var calls []string

	app := &App{
		cluster: &raftcluster.Cluster{},
		gateway: &gateway.Gateway{},
		startClusterFn: func() error {
			calls = append(calls, "cluster.start")
			return nil
		},
		startGatewayFn: func() error {
			calls = append(calls, "gateway.start")
			return nil
		},
	}

	require.NoError(t, app.Start())
	require.Equal(t, []string{"cluster.start", "gateway.start"}, calls)
	require.True(t, app.started.Load())
}

func TestStartStartsAPIAfterGatewayWhenEnabled(t *testing.T) {
	var calls []string

	app := &App{
		cluster:         &raftcluster.Cluster{},
		channelMetaSync: &channelMetaSync{},
		gateway:         &gateway.Gateway{},
		startClusterFn: func() error {
			calls = append(calls, "cluster.start")
			return nil
		},
		startChannelMetaSyncFn: func() error {
			calls = append(calls, "meta.start")
			return nil
		},
		startAPIFn: func() error {
			calls = append(calls, "api.start")
			return nil
		},
		startGatewayFn: func() error {
			calls = append(calls, "gateway.start")
			return nil
		},
	}

	require.NoError(t, app.Start())
	require.Equal(t, []string{"cluster.start", "meta.start", "gateway.start", "api.start"}, calls)
}

func TestStartStartsChannelMetaSyncAfterClusterBeforeGateway(t *testing.T) {
	var calls []string

	app := &App{
		cluster:         &raftcluster.Cluster{},
		channelMetaSync: &channelMetaSync{},
		gateway:         &gateway.Gateway{},
		startClusterFn: func() error {
			calls = append(calls, "cluster.start")
			return nil
		},
		startChannelMetaSyncFn: func() error {
			calls = append(calls, "meta.start")
			return nil
		},
		startGatewayFn: func() error {
			calls = append(calls, "gateway.start")
			return nil
		},
	}

	require.NoError(t, app.Start())
	require.Equal(t, []string{"cluster.start", "meta.start", "gateway.start"}, calls)
}

func TestAppLifecycleStartsPresenceWorkerBeforeGateway(t *testing.T) {
	var calls []string

	app := &App{
		cluster: &raftcluster.Cluster{},
		gateway: &gateway.Gateway{},
		startClusterFn: func() error {
			calls = append(calls, "cluster.start")
			return nil
		},
		startGatewayFn: func() error {
			calls = append(calls, "gateway.start")
			return nil
		},
	}
	setAppFuncField(t, app, "startPresenceFn", func() error {
		calls = append(calls, "presence.start")
		return nil
	})

	require.NoError(t, app.Start())
	require.Equal(t, []string{"cluster.start", "presence.start", "gateway.start"}, calls)
}

func TestStartStopIncludesConversationProjector(t *testing.T) {
	var calls []string

	app := &App{
		cluster: &raftcluster.Cluster{},
		gateway: &gateway.Gateway{},
		startClusterFn: func() error {
			calls = append(calls, "cluster.start")
			return nil
		},
		startConversationProjectorFn: func() error {
			calls = append(calls, "conversation.start")
			return nil
		},
		startGatewayFn: func() error {
			calls = append(calls, "gateway.start")
			return nil
		},
		stopGatewayFn: func() error {
			calls = append(calls, "gateway.stop")
			return nil
		},
		stopConversationProjectorFn: func() error {
			calls = append(calls, "conversation.stop")
			return nil
		},
		stopClusterFn: func() {
			calls = append(calls, "cluster.stop")
		},
	}

	require.NoError(t, app.Start())
	require.Equal(t, []string{"cluster.start", "conversation.start", "gateway.start"}, calls)

	require.NoError(t, app.Stop())
	require.Equal(t, []string{"cluster.start", "conversation.start", "gateway.start", "gateway.stop", "conversation.stop", "cluster.stop"}, calls)
}

func TestStartRollsBackClusterWhenGatewayStartFails(t *testing.T) {
	var calls []string
	startErr := errors.New("gateway start failed")

	app := &App{
		cluster: &raftcluster.Cluster{},
		gateway: &gateway.Gateway{},
		startClusterFn: func() error {
			calls = append(calls, "cluster.start")
			return nil
		},
		startGatewayFn: func() error {
			calls = append(calls, "gateway.start")
			return startErr
		},
		stopClusterFn: func() {
			calls = append(calls, "cluster.stop")
		},
	}

	err := app.Start()
	require.ErrorIs(t, err, startErr)
	require.Equal(t, []string{"cluster.start", "gateway.start", "cluster.stop"}, calls)
	require.False(t, app.started.Load())
}

func TestStopIsSafeAfterFailedStartRollback(t *testing.T) {
	var calls []string
	startErr := errors.New("gateway start failed")

	app := &App{
		cluster: &raftcluster.Cluster{},
		gateway: &gateway.Gateway{},
		startClusterFn: func() error {
			calls = append(calls, "cluster.start")
			return nil
		},
		startGatewayFn: func() error {
			calls = append(calls, "gateway.start")
			return startErr
		},
		stopClusterFn: func() {
			calls = append(calls, "cluster.stop")
		},
		stopGatewayFn: func() error {
			calls = append(calls, "gateway.stop")
			return nil
		},
		closeRaftDBFn: func() error {
			calls = append(calls, "raft.close")
			return nil
		},
		closeWKDBFn: func() error {
			calls = append(calls, "metadb.close")
			return nil
		},
	}

	require.ErrorIs(t, app.Start(), startErr)
	require.NoError(t, app.Stop())
	require.Equal(t, []string{
		"cluster.start",
		"gateway.start",
		"cluster.stop",
		"raft.close",
		"metadb.close",
	}, calls)
}

func TestStopStopsGatewayBeforeClosingStorage(t *testing.T) {
	var calls []string

	app := &App{
		started:       atomicBool(true),
		clusterOn:     atomicBool(true),
		gatewayOn:     atomicBool(true),
		channelMetaOn: atomicBool(true),
		stopGatewayFn: func() error {
			calls = append(calls, "gateway.stop")
			return nil
		},
		stopChannelMetaSyncFn: func() error {
			calls = append(calls, "meta.stop")
			return nil
		},
		stopClusterFn: func() {
			calls = append(calls, "cluster.stop")
		},
		closeChannelLogDBFn: func() error {
			calls = append(calls, "channellog.close")
			return nil
		},
		closeRaftDBFn: func() error {
			calls = append(calls, "raft.close")
			return nil
		},
		closeWKDBFn: func() error {
			calls = append(calls, "metadb.close")
			return nil
		},
	}

	require.NoError(t, app.Stop())
	require.Equal(t, []string{"gateway.stop", "meta.stop", "cluster.stop", "channellog.close", "raft.close", "metadb.close"}, calls)
	require.False(t, app.started.Load())
}

func TestAppLifecycleStopsPresenceWorkerAfterGateway(t *testing.T) {
	var startCalls []string
	var stopCalls []string

	app := &App{
		cluster: &raftcluster.Cluster{},
		gateway: &gateway.Gateway{},
		startClusterFn: func() error {
			startCalls = append(startCalls, "cluster.start")
			return nil
		},
		startGatewayFn: func() error {
			startCalls = append(startCalls, "gateway.start")
			return nil
		},
		stopGatewayFn: func() error {
			stopCalls = append(stopCalls, "gateway.stop")
			return nil
		},
		stopClusterFn: func() {
			stopCalls = append(stopCalls, "cluster.stop")
		},
		closeRaftDBFn: func() error {
			stopCalls = append(stopCalls, "raft.close")
			return nil
		},
		closeWKDBFn: func() error {
			stopCalls = append(stopCalls, "metadb.close")
			return nil
		},
	}
	setAppFuncField(t, app, "startPresenceFn", func() error {
		startCalls = append(startCalls, "presence.start")
		return nil
	})
	setAppFuncField(t, app, "stopPresenceFn", func() error {
		stopCalls = append(stopCalls, "presence.stop")
		return nil
	})

	require.NoError(t, app.Start())
	require.Equal(t, []string{"cluster.start", "presence.start", "gateway.start"}, startCalls)

	require.NoError(t, app.Stop())
	require.Equal(t, []string{"gateway.stop", "presence.stop", "cluster.stop", "raft.close", "metadb.close"}, stopCalls)
}

func TestStopStopsAPIBeforeGatewayAndClusterClose(t *testing.T) {
	var calls []string

	app := &App{
		started:       atomicBool(true),
		clusterOn:     atomicBool(true),
		apiOn:         atomicBool(true),
		gatewayOn:     atomicBool(true),
		channelMetaOn: atomicBool(true),
		stopGatewayFn: func() error {
			calls = append(calls, "gateway.stop")
			return nil
		},
		stopAPIFn: func() error {
			calls = append(calls, "api.stop")
			return nil
		},
		stopChannelMetaSyncFn: func() error {
			calls = append(calls, "meta.stop")
			return nil
		},
		stopClusterFn: func() {
			calls = append(calls, "cluster.stop")
		},
		closeChannelLogDBFn: func() error {
			calls = append(calls, "channellog.close")
			return nil
		},
		closeRaftDBFn: func() error {
			calls = append(calls, "raft.close")
			return nil
		},
		closeWKDBFn: func() error {
			calls = append(calls, "metadb.close")
			return nil
		},
	}

	require.NoError(t, app.Stop())
	require.Equal(t, []string{"api.stop", "gateway.stop", "meta.stop", "cluster.stop", "channellog.close", "raft.close", "metadb.close"}, calls)
}

func TestStopIsIdempotent(t *testing.T) {
	var calls []string

	app := &App{
		started:   atomicBool(true),
		clusterOn: atomicBool(true),
		gatewayOn: atomicBool(true),
		stopGatewayFn: func() error {
			calls = append(calls, "gateway.stop")
			return nil
		},
		stopClusterFn: func() {
			calls = append(calls, "cluster.stop")
		},
		closeRaftDBFn: func() error {
			calls = append(calls, "raft.close")
			return nil
		},
		closeWKDBFn: func() error {
			calls = append(calls, "metadb.close")
			return nil
		},
	}

	require.NoError(t, app.Stop())
	require.NoError(t, app.Stop())
	require.Equal(t, []string{"gateway.stop", "cluster.stop", "raft.close", "metadb.close"}, calls)
	require.False(t, app.started.Load())
}

func TestStartReturnsAlreadyStartedAfterSuccess(t *testing.T) {
	var calls []string

	app := &App{
		cluster: &raftcluster.Cluster{},
		gateway: &gateway.Gateway{},
		startClusterFn: func() error {
			calls = append(calls, "cluster.start")
			return nil
		},
		startGatewayFn: func() error {
			calls = append(calls, "gateway.start")
			return nil
		},
	}

	require.NoError(t, app.Start())
	require.ErrorIs(t, app.Start(), ErrAlreadyStarted)
	require.Equal(t, []string{"cluster.start", "gateway.start"}, calls)
}

func TestStartReturnsStoppedAfterStop(t *testing.T) {
	app := &App{
		cluster: &raftcluster.Cluster{},
		gateway: &gateway.Gateway{},
		closeRaftDBFn: func() error {
			return nil
		},
		closeWKDBFn: func() error {
			return nil
		},
	}

	require.NoError(t, app.Stop())
	require.ErrorIs(t, app.Start(), ErrStopped)
}

func TestStopWaitsForInFlightStart(t *testing.T) {
	startGatewayEntered := make(chan struct{})
	releaseGatewayStart := make(chan struct{})
	startDone := make(chan error, 1)
	stopDone := make(chan error, 1)
	closeCalls := make(chan string, 2)

	app := &App{
		cluster: &raftcluster.Cluster{},
		gateway: &gateway.Gateway{},
		startClusterFn: func() error {
			return nil
		},
		startGatewayFn: func() error {
			close(startGatewayEntered)
			<-releaseGatewayStart
			return nil
		},
		stopGatewayFn: func() error {
			return nil
		},
		stopClusterFn: func() {},
		closeRaftDBFn: func() error {
			closeCalls <- "raft.close"
			return nil
		},
		closeWKDBFn: func() error {
			closeCalls <- "metadb.close"
			return nil
		},
	}

	go func() {
		startDone <- app.Start()
	}()

	<-startGatewayEntered

	go func() {
		stopDone <- app.Stop()
	}()

	select {
	case call := <-closeCalls:
		t.Fatalf("cleanup ran before start finished: %s", call)
	case <-time.After(100 * time.Millisecond):
	}

	close(releaseGatewayStart)

	require.NoError(t, <-startDone)
	require.NoError(t, <-stopDone)
}

func TestStopJoinsCleanupErrors(t *testing.T) {
	errGateway := errors.New("gateway stop")
	errRaft := errors.New("raft close")
	errMetaDB := errors.New("metadb close")

	app := &App{
		started:   atomicBool(true),
		clusterOn: atomicBool(true),
		gatewayOn: atomicBool(true),
		stopGatewayFn: func() error {
			return errGateway
		},
		stopClusterFn: func() {},
		closeRaftDBFn: func() error {
			return errRaft
		},
		closeWKDBFn: func() error {
			return errMetaDB
		},
	}

	joinedErr := app.Stop()
	require.ErrorIs(t, joinedErr, errGateway)
	require.ErrorIs(t, joinedErr, errRaft)
	require.ErrorIs(t, joinedErr, errMetaDB)
}

func testConfig(t *testing.T) Config {
	t.Helper()

	cfg := validConfig()
	clusterAddr := reserveTestTCPAddrs(t, 1)[1]
	cfg.Node.DataDir = t.TempDir()
	cfg.Storage = StorageConfig{
		DBPath:   filepath.Join(cfg.Node.DataDir, "data"),
		RaftPath: filepath.Join(cfg.Node.DataDir, "raft"),
	}
	cfg.Cluster.ListenAddr = clusterAddr
	cfg.Cluster.Nodes = []NodeConfigRef{{ID: cfg.Node.ID, Addr: clusterAddr}}
	cfg.Gateway.Listeners[0].Address = "127.0.0.1:0"
	return cfg
}

func openWKDBForTest(path string) (interface{ Close() error }, error) {
	return metadb.Open(path)
}

func openRaftDBForTest(path string) (interface{ Close() error }, error) {
	return raftstorage.Open(path)
}

func atomicBool(v bool) (flag atomic.Bool) {
	flag.Store(v)
	return flag
}

func requireAppFieldNonNil(t *testing.T, app *App, name string) {
	t.Helper()

	field := reflect.ValueOf(app).Elem().FieldByName(name)
	if !field.IsValid() {
		t.Fatalf("App is missing field %s", name)
	}
	switch field.Kind() {
	case reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice, reflect.Func:
		require.Falsef(t, field.IsNil(), "App field %s should not be nil", name)
	default:
		t.Fatalf("App field %s is %s; expected a nil-able field", name, field.Kind())
	}
}

func setAppFuncField(t *testing.T, app *App, name string, fn any) {
	t.Helper()

	field := reflect.ValueOf(app).Elem().FieldByName(name)
	if !field.IsValid() {
		t.Fatalf("App is missing field %s", name)
	}
	ptr := reflect.NewAt(field.Type(), unsafe.Pointer(field.UnsafeAddr())).Elem()
	ptr.Set(reflect.ValueOf(fn))
}

func appISRMaxFetchInflightPeerLimit(t *testing.T, app *App) int {
	t.Helper()

	rt := reflect.ValueOf(app.isrRuntime)
	if rt.Kind() != reflect.Pointer || rt.IsNil() {
		t.Fatalf("isrRuntime is %s, want non-nil pointer", rt.Kind())
	}
	cfgField := rt.Elem().FieldByName("cfg")
	if !cfgField.IsValid() {
		t.Fatal("isr runtime missing cfg field")
	}
	cfg := reflect.NewAt(cfgField.Type(), unsafe.Pointer(cfgField.UnsafeAddr())).Elem()
	limits := cfg.FieldByName("Limits")
	if !limits.IsValid() {
		t.Fatal("isr runtime config missing Limits field")
	}
	maxInflight := limits.FieldByName("MaxFetchInflightPeer")
	if !maxInflight.IsValid() {
		t.Fatal("isr runtime limits missing MaxFetchInflightPeer field")
	}
	return int(maxInflight.Int())
}

func setClusterConfigIntField(t *testing.T, cfg *ClusterConfig, name string, value int) {
	t.Helper()

	field := reflect.ValueOf(cfg).Elem().FieldByName(name)
	if !field.IsValid() {
		t.Fatalf("ClusterConfig is missing field %s", name)
	}
	ptr := reflect.NewAt(field.Type(), unsafe.Pointer(field.UnsafeAddr())).Elem()
	ptr.SetInt(int64(value))
}

func appDataPlanePoolSize(t *testing.T, app *App) int {
	t.Helper()

	require.NotNil(t, app.dataPlanePool)
	pool := reflect.ValueOf(app.dataPlanePool).Elem()
	size := pool.FieldByName("size")
	if !size.IsValid() {
		t.Fatal("dataPlanePool is missing size field")
	}
	return int(size.Int())
}

func appDataPlaneAdapterMaxPendingFetch(t *testing.T, app *App) int {
	t.Helper()

	require.NotNil(t, app.isrTransport)
	bridge := reflect.ValueOf(app.isrTransport).Elem()
	adapterField := bridge.FieldByName("adapter")
	if !adapterField.IsValid() {
		t.Fatal("isrTransportBridge is missing adapter field")
	}
	if adapterField.IsNil() {
		t.Fatal("isr transport adapter is nil")
	}
	adapter := adapterField.Elem()
	maxPending := adapter.FieldByName("maxPending")
	if !maxPending.IsValid() {
		t.Fatal("data plane adapter is missing maxPending field")
	}
	return int(maxPending.Int())
}
