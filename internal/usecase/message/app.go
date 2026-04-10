package message

import (
	"context"
	"errors"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/runtime/online"
	metadb "github.com/WuKongIM/WuKongIM/pkg/slot/meta"
)

var (
	ErrUnauthenticatedSender = errors.New("usecase/message: unauthenticated sender")
	ErrClusterRequired       = errors.New("usecase/message: channel cluster required")
)

type Options struct {
	IdentityStore       IdentityStore
	ChannelStore        ChannelStore
	Cluster             ChannelCluster
	MetaRefresher       MetaRefresher
	Online              online.Registry
	Delivery            online.Delivery
	Recipients          RecipientDirectory
	RemoteDelivery      RemoteDelivery
	CommittedDispatcher CommittedMessageDispatcher
	DeliveryAck         DeliveryAck
	DeliveryOffline     DeliveryOffline
	LocalNodeID         uint64
	LocalBootID         uint64
	Now                 func() time.Time
}

type App struct {
	identities      IdentityStore
	channels        ChannelStore
	cluster         ChannelCluster
	refresher       MetaRefresher
	online          online.Registry
	delivery        online.Delivery
	recipients      RecipientDirectory
	remote          RemoteDelivery
	dispatcher      CommittedMessageDispatcher
	deliveryAck     DeliveryAck
	deliveryOffline DeliveryOffline
	localNodeID     uint64
	localBootID     uint64
	now             func() time.Time
}

func New(opts Options) *App {
	if opts.Online == nil {
		opts.Online = online.NewRegistry()
	}
	if opts.Delivery == nil {
		opts.Delivery = online.LocalDelivery{}
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}

	return &App{
		identities:      opts.IdentityStore,
		channels:        opts.ChannelStore,
		cluster:         opts.Cluster,
		refresher:       opts.MetaRefresher,
		online:          opts.Online,
		delivery:        opts.Delivery,
		recipients:      opts.Recipients,
		remote:          opts.RemoteDelivery,
		dispatcher:      opts.CommittedDispatcher,
		deliveryAck:     opts.DeliveryAck,
		deliveryOffline: opts.DeliveryOffline,
		localNodeID:     opts.LocalNodeID,
		localBootID:     opts.LocalBootID,
		now:             opts.Now,
	}
}

func (a *App) OnlineRegistry() online.Registry {
	if a == nil {
		return nil
	}
	return a.online
}

type IdentityStore interface {
	GetUser(ctx context.Context, uid string) (metadb.User, error)
}

type ChannelStore interface {
	GetChannel(ctx context.Context, channelID string, channelType int64) (metadb.Channel, error)
}
