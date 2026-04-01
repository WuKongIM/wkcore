package app

import (
	"sync"
	"sync/atomic"

	"github.com/WuKongIM/WuKongIM/internal/gateway"
	"github.com/WuKongIM/WuKongIM/internal/service"
	"github.com/WuKongIM/WuKongIM/pkg/raftstore"
	"github.com/WuKongIM/WuKongIM/pkg/wkcluster"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/WuKongIM/WuKongIM/pkg/wkstore"
)

type App struct {
	cfg Config

	db      *wkdb.DB
	raftDB  *raftstore.DB
	cluster *wkcluster.Cluster
	store   *wkstore.Store
	service *service.Service
	gateway *gateway.Gateway

	startOnce sync.Once
	stopOnce  sync.Once
	started   atomic.Bool
}

func New(cfg Config) (*App, error) {
	return build(cfg)
}

func (a *App) DB() *wkdb.DB {
	if a == nil {
		return nil
	}
	return a.db
}

func (a *App) RaftDB() *raftstore.DB {
	if a == nil {
		return nil
	}
	return a.raftDB
}

func (a *App) Cluster() *wkcluster.Cluster {
	if a == nil {
		return nil
	}
	return a.cluster
}

func (a *App) Store() *wkstore.Store {
	if a == nil {
		return nil
	}
	return a.store
}

func (a *App) Service() *service.Service {
	if a == nil {
		return nil
	}
	return a.service
}

func (a *App) Gateway() *gateway.Gateway {
	if a == nil {
		return nil
	}
	return a.gateway
}
