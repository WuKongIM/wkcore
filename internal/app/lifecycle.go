package app

import (
	"context"
	"errors"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/channel/isr"
	isrnodetransport "github.com/WuKongIM/WuKongIM/pkg/channel/transport"
	raftcluster "github.com/WuKongIM/WuKongIM/pkg/cluster"
	"github.com/WuKongIM/WuKongIM/pkg/transport"
)

const (
	apiStopTimeout              = 5 * time.Second
	defaultDataPlaneDialTimeout = 5 * time.Second
	presenceLeaderReadyTimeout  = 10 * time.Second
)

func (a *App) Start() error {
	if a == nil || a.cluster == nil || a.gateway == nil {
		return ErrNotBuilt
	}
	a.lifecycle.Lock()
	defer a.lifecycle.Unlock()
	if a.stopped.Load() {
		return ErrStopped
	}
	if !a.started.CompareAndSwap(false, true) {
		return ErrAlreadyStarted
	}
	if err := a.startCluster(); err != nil {
		a.started.Store(false)
		return err
	}
	a.clusterOn.Store(true)
	if err := a.waitForManagedGroupsReady(); err != nil {
		_ = a.stopClusterWithError()
		a.started.Store(false)
		return err
	}
	if a.channelMetaSync != nil || a.startChannelMetaSyncFn != nil {
		if err := a.startChannelMetaSync(); err != nil {
			_ = a.stopClusterWithError()
			a.started.Store(false)
			return err
		}
		a.channelMetaOn.Store(true)
	}
	if a.presenceWorker != nil || a.startPresenceFn != nil {
		if err := a.startPresence(); err != nil {
			_ = a.stopChannelMetaSync()
			_ = a.stopClusterWithError()
			a.started.Store(false)
			return err
		}
		a.presenceOn.Store(true)
	}
	if a.conversationProjector != nil || a.startConversationProjectorFn != nil {
		if err := a.startConversationProjector(); err != nil {
			_ = a.stopPresence()
			_ = a.stopChannelMetaSync()
			_ = a.stopClusterWithError()
			a.started.Store(false)
			return err
		}
		a.conversationOn.Store(true)
	}
	if err := a.startGateway(); err != nil {
		_ = a.stopConversationProjector()
		_ = a.stopPresence()
		_ = a.stopChannelMetaSync()
		_ = a.stopClusterWithError()
		a.started.Store(false)
		return err
	}
	a.gatewayOn.Store(true)
	if err := a.startAPI(); err != nil {
		_ = a.stopGateway()
		_ = a.stopPresence()
		_ = a.stopChannelMetaSync()
		_ = a.stopClusterWithError()
		a.started.Store(false)
		return err
	}
	if a.api != nil || a.startAPIFn != nil {
		a.apiOn.Store(true)
	}
	return nil
}

func (a *App) Stop() error {
	if a == nil {
		return nil
	}
	a.lifecycle.Lock()
	defer a.lifecycle.Unlock()
	a.stopped.Store(true)

	var err error
	a.stopOnce.Do(func() {
		a.started.Store(false)
		err = errors.Join(
			a.stopAPI(),
			a.stopGateway(),
			a.stopConversationProjector(),
			a.stopPresence(),
			a.stopChannelMetaSync(),
			a.stopClusterWithError(),
			a.closeChannelLogDB(),
			a.closeRaftDB(),
			a.closeWKDB(),
		)
	})
	return err
}

func (a *App) startCluster() error {
	if a.startClusterFn != nil {
		return a.startClusterFn()
	}
	if a.cluster == nil {
		return ErrNotBuilt
	}
	return a.cluster.Start()
}

func (a *App) startGateway() error {
	if a.startGatewayFn != nil {
		return a.startGatewayFn()
	}
	if a.gateway == nil {
		return ErrNotBuilt
	}
	return a.gateway.Start()
}

func (a *App) startChannelMetaSync() error {
	if a.startChannelMetaSyncFn != nil {
		return a.startChannelMetaSyncFn()
	}
	if a.channelMetaSync == nil {
		return nil
	}
	if a.cluster == nil || a.isrTransport == nil || a.isrRuntime == nil {
		return ErrNotBuilt
	}
	if a.dataPlanePool == nil || a.dataPlaneClient == nil {
		discovery := raftcluster.NewStaticDiscovery(a.cfg.Cluster.runtimeNodes())
		poolSize := a.cfg.Cluster.DataPlanePoolSize
		dialTimeout := a.cfg.Cluster.DialTimeout
		if dialTimeout <= 0 {
			dialTimeout = defaultDataPlaneDialTimeout
		}
		a.dataPlanePool = transport.NewPool(discovery, poolSize, dialTimeout)
		a.dataPlaneClient = transport.NewClient(a.dataPlanePool)
		adapter, err := isrnodetransport.New(isrnodetransport.Options{
			LocalNode:          isr.NodeID(a.cfg.Node.ID),
			Client:             a.dataPlaneClient,
			RPCMux:             a.cluster.RPCMux(),
			FetchService:       a.isrRuntime,
			RPCTimeout:         a.cfg.Cluster.DataPlaneRPCTimeout,
			MaxPendingFetchRPC: a.cfg.Cluster.DataPlaneMaxPendingFetch,
		})
		if err != nil {
			a.dataPlaneClient.Stop()
			a.dataPlaneClient = nil
			a.dataPlanePool.Close()
			a.dataPlanePool = nil
			return err
		}
		a.isrTransport.Bind(adapter)
	}
	if err := a.channelMetaSync.Start(); err != nil {
		if a.dataPlaneClient != nil {
			a.dataPlaneClient.Stop()
			a.dataPlaneClient = nil
		}
		if a.dataPlanePool != nil {
			a.dataPlanePool.Close()
			a.dataPlanePool = nil
		}
		a.isrTransport.Unbind()
		return err
	}
	return nil
}

func (a *App) startAPI() error {
	if a.startAPIFn != nil {
		return a.startAPIFn()
	}
	if a.api == nil {
		return nil
	}
	return a.api.Start()
}

func (a *App) startPresence() error {
	if a.startPresenceFn != nil {
		return a.startPresenceFn()
	}
	if a.presenceWorker == nil {
		return nil
	}
	return a.presenceWorker.Start()
}

func (a *App) startConversationProjector() error {
	if a.startConversationProjectorFn != nil {
		return a.startConversationProjectorFn()
	}
	if a.conversationProjector == nil {
		return nil
	}
	return a.conversationProjector.Start()
}

func (a *App) stopGateway() error {
	if !a.gatewayOn.Swap(false) {
		return nil
	}
	if a.stopGatewayFn != nil {
		return a.stopGatewayFn()
	}
	if a.gateway == nil {
		return nil
	}
	return a.gateway.Stop()
}

func (a *App) stopPresence() error {
	if !a.presenceOn.Swap(false) {
		return nil
	}
	if a.stopPresenceFn != nil {
		return a.stopPresenceFn()
	}
	if a.presenceWorker == nil {
		return nil
	}
	return a.presenceWorker.Stop()
}

func (a *App) stopConversationProjector() error {
	if !a.conversationOn.Swap(false) {
		return nil
	}
	if a.stopConversationProjectorFn != nil {
		return a.stopConversationProjectorFn()
	}
	if a.conversationProjector == nil {
		return nil
	}
	return a.conversationProjector.Stop()
}

func (a *App) stopChannelMetaSync() error {
	if !a.channelMetaOn.Swap(false) {
		return nil
	}
	if a.stopChannelMetaSyncFn != nil {
		return a.stopChannelMetaSyncFn()
	}

	var err error
	if a.channelMetaSync != nil {
		err = errors.Join(err, a.channelMetaSync.Stop())
	}
	if a.dataPlaneClient != nil {
		a.dataPlaneClient.Stop()
		a.dataPlaneClient = nil
	}
	if a.dataPlanePool != nil {
		a.dataPlanePool.Close()
		a.dataPlanePool = nil
	}
	if a.isrTransport != nil {
		a.isrTransport.Unbind()
	}
	return err
}

func (a *App) stopAPI() error {
	if !a.apiOn.Swap(false) {
		return nil
	}
	if a.stopAPIFn != nil {
		return a.stopAPIFn()
	}
	if a.api == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), apiStopTimeout)
	defer cancel()
	return a.api.Stop(ctx)
}

func (a *App) stopCluster() {
	if !a.clusterOn.Swap(false) {
		return
	}
	if a.stopClusterFn != nil {
		a.stopClusterFn()
		return
	}
	if a.cluster == nil {
		return
	}
	a.cluster.Stop()
}

func (a *App) stopClusterWithError() error {
	a.stopCluster()
	return nil
}

func (a *App) waitForManagedGroupsReady() error {
	if a == nil || a.cluster == nil {
		return nil
	}
	readyCtx, cancel := context.WithTimeout(context.Background(), presenceLeaderReadyTimeout)
	defer cancel()
	return a.cluster.WaitForManagedGroupsReady(readyCtx)
}

func (a *App) closeRaftDB() error {
	if a.closeRaftDBFn != nil {
		return a.closeRaftDBFn()
	}
	if a.raftDB == nil {
		return nil
	}
	return a.raftDB.Close()
}

func (a *App) closeChannelLogDB() error {
	if a.closeChannelLogDBFn != nil {
		return a.closeChannelLogDBFn()
	}
	if a.channelLogDB == nil {
		return nil
	}
	return a.channelLogDB.Close()
}

func (a *App) closeWKDB() error {
	if a.closeWKDBFn != nil {
		return a.closeWKDBFn()
	}
	if a.db == nil {
		return nil
	}
	return a.db.Close()
}
