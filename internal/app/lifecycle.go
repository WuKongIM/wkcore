package app

import (
	"context"
	"errors"
	"time"
)

const apiStopTimeout = 5 * time.Second

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
	if err := a.startGateway(); err != nil {
		_ = a.stopClusterWithError()
		a.started.Store(false)
		return err
	}
	a.gatewayOn.Store(true)
	if err := a.startAPI(); err != nil {
		_ = a.stopGateway()
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
			a.stopClusterWithError(),
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

func (a *App) startAPI() error {
	if a.startAPIFn != nil {
		return a.startAPIFn()
	}
	if a.api == nil {
		return nil
	}
	return a.api.Start()
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

func (a *App) closeRaftDB() error {
	if a.closeRaftDBFn != nil {
		return a.closeRaftDBFn()
	}
	if a.raftDB == nil {
		return nil
	}
	return a.raftDB.Close()
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
