package app

import "errors"

func (a *App) Start() error {
	if a == nil || a.cluster == nil || a.gateway == nil {
		return ErrNotBuilt
	}
	if a.started.Load() {
		return ErrAlreadyStarted
	}
	if err := a.startCluster(); err != nil {
		return err
	}
	if err := a.startGateway(); err != nil {
		a.stopCluster()
		return err
	}
	a.started.Store(true)
	return nil
}

func (a *App) Stop() error {
	if a == nil {
		return nil
	}

	var err error
	a.stopOnce.Do(func() {
		a.started.Store(false)
		err = errors.Join(
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

func (a *App) stopGateway() error {
	if a.stopGatewayFn != nil {
		return a.stopGatewayFn()
	}
	if a.gateway == nil {
		return nil
	}
	return a.gateway.Stop()
}

func (a *App) stopCluster() {
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
