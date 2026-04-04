package message

import (
	"context"
	"errors"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/runtime/online"
	"github.com/WuKongIM/WuKongIM/pkg/storage/metadb"
)

var (
	ErrUnauthenticatedSender = errors.New("usecase/message: unauthenticated sender")
	ErrClusterRequired       = errors.New("usecase/message: channel cluster required")
)

type Options struct {
	IdentityStore IdentityStore
	ChannelStore  ChannelStore
	Cluster       ChannelCluster
	MetaRefresher MetaRefresher
	Online        online.Registry
	Delivery      online.Delivery
	Now           func() time.Time
}

type App struct {
	identities IdentityStore
	channels   ChannelStore
	cluster    ChannelCluster
	refresher  MetaRefresher
	online     online.Registry
	delivery   online.Delivery
	now        func() time.Time
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
		identities: opts.IdentityStore,
		channels:   opts.ChannelStore,
		cluster:    opts.Cluster,
		refresher:  opts.MetaRefresher,
		online:     opts.Online,
		delivery:   opts.Delivery,
		now:        opts.Now,
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
