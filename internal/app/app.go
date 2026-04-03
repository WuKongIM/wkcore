package app

import (
	"sync"
	"sync/atomic"

	accessapi "github.com/WuKongIM/WuKongIM/internal/access/api"
	accessgateway "github.com/WuKongIM/WuKongIM/internal/access/gateway"
	"github.com/WuKongIM/WuKongIM/internal/gateway"
	"github.com/WuKongIM/WuKongIM/internal/usecase/message"
	"github.com/WuKongIM/WuKongIM/pkg/controller/wkcluster"
	"github.com/WuKongIM/WuKongIM/pkg/storage/metadb"
	"github.com/WuKongIM/WuKongIM/pkg/storage/metastore"
	"github.com/WuKongIM/WuKongIM/pkg/storage/raftstorage"
)

type App struct {
	cfg Config

	db             *metadb.DB
	raftDB         *raftstorage.DB
	cluster        *wkcluster.Cluster
	store          *metastore.Store
	messageApp     *message.App
	api            *accessapi.Server
	gatewayHandler *accessgateway.Handler
	gateway        *gateway.Gateway

	stopOnce  sync.Once
	lifecycle sync.Mutex
	started   atomic.Bool
	stopped   atomic.Bool
	clusterOn atomic.Bool
	apiOn     atomic.Bool
	gatewayOn atomic.Bool

	startClusterFn func() error
	startAPIFn     func() error
	startGatewayFn func() error
	stopAPIFn      func() error
	stopGatewayFn  func() error
	stopClusterFn  func()
	closeRaftDBFn  func() error
	closeWKDBFn    func() error
}

func New(cfg Config) (*App, error) {
	return build(cfg)
}

func (a *App) DB() *metadb.DB {
	if a == nil {
		return nil
	}
	return a.db
}

func (a *App) RaftDB() *raftstorage.DB {
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

func (a *App) Store() *metastore.Store {
	if a == nil {
		return nil
	}
	return a.store
}

func (a *App) Message() *message.App {
	if a == nil {
		return nil
	}
	return a.messageApp
}

func (a *App) GatewayHandler() *accessgateway.Handler {
	if a == nil {
		return nil
	}
	return a.gatewayHandler
}

func (a *App) API() *accessapi.Server {
	if a == nil {
		return nil
	}
	return a.api
}

func (a *App) Gateway() *gateway.Gateway {
	if a == nil {
		return nil
	}
	return a.gateway
}
