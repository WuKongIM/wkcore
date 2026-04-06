package app

import (
	"sync"
	"sync/atomic"

	accessapi "github.com/WuKongIM/WuKongIM/internal/access/api"
	accessgateway "github.com/WuKongIM/WuKongIM/internal/access/gateway"
	accessnode "github.com/WuKongIM/WuKongIM/internal/access/node"
	"github.com/WuKongIM/WuKongIM/internal/gateway"
	deliveryruntime "github.com/WuKongIM/WuKongIM/internal/runtime/delivery"
	"github.com/WuKongIM/WuKongIM/internal/usecase/message"
	deliveryusecase "github.com/WuKongIM/WuKongIM/internal/usecase/delivery"
	"github.com/WuKongIM/WuKongIM/internal/usecase/presence"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/raftcluster"
	"github.com/WuKongIM/WuKongIM/pkg/replication/isrnode"
	"github.com/WuKongIM/WuKongIM/pkg/storage/channellog"
	"github.com/WuKongIM/WuKongIM/pkg/storage/metadb"
	"github.com/WuKongIM/WuKongIM/pkg/storage/metastore"
	"github.com/WuKongIM/WuKongIM/pkg/storage/raftstorage"
	"github.com/WuKongIM/WuKongIM/pkg/transport/nodetransport"
)

type App struct {
	cfg Config

	db              *metadb.DB
	raftDB          *raftstorage.DB
	channelLogDB    *channellog.DB
	cluster         *raftcluster.Cluster
	isrRuntime      isrnode.Runtime
	channelLog      channellog.Cluster
	channelMetaSync *channelMetaSync
	store           *metastore.Store
	presenceApp     *presence.App
	deliveryApp     *deliveryusecase.App
	deliveryRuntime *deliveryruntime.Manager
	messageApp      *message.App
	api             *accessapi.Server
	nodeClient      *accessnode.Client
	nodeAccess      *accessnode.Adapter
	presenceWorker  *presenceWorker
	gatewayHandler  *accessgateway.Handler
	gateway         *gateway.Gateway
	gatewayBootID   uint64

	isrTransport    *isrTransportBridge
	replicaFactory  *channelReplicaFactory
	dataPlanePool   *nodetransport.Pool
	dataPlaneClient *nodetransport.Client

	stopOnce      sync.Once
	lifecycle     sync.Mutex
	started       atomic.Bool
	stopped       atomic.Bool
	clusterOn     atomic.Bool
	channelMetaOn atomic.Bool
	presenceOn    atomic.Bool
	apiOn         atomic.Bool
	gatewayOn     atomic.Bool

	startClusterFn         func() error
	startChannelMetaSyncFn func() error
	startPresenceFn        func() error
	startAPIFn             func() error
	startGatewayFn         func() error
	stopAPIFn              func() error
	stopGatewayFn          func() error
	stopPresenceFn         func() error
	stopChannelMetaSyncFn  func() error
	stopClusterFn          func()
	closeChannelLogDBFn    func() error
	closeRaftDBFn          func() error
	closeWKDBFn            func() error
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

func (a *App) Cluster() *raftcluster.Cluster {
	if a == nil {
		return nil
	}
	return a.cluster
}

func (a *App) ChannelLogDB() *channellog.DB {
	if a == nil {
		return nil
	}
	return a.channelLogDB
}

func (a *App) ISRRuntime() isrnode.Runtime {
	if a == nil {
		return nil
	}
	return a.isrRuntime
}

func (a *App) ChannelLog() channellog.Cluster {
	if a == nil {
		return nil
	}
	return a.channelLog
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
