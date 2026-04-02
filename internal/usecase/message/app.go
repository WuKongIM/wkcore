package message

import (
	"context"
	"errors"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/runtime/online"
	"github.com/WuKongIM/WuKongIM/internal/runtime/sequence"
	"github.com/WuKongIM/WuKongIM/pkg/metadata/wkdb"
)

var ErrUnauthenticatedSender = errors.New("usecase/message: unauthenticated sender")

type Options struct {
	IdentityStore IdentityStore
	ChannelStore  ChannelStore
	ClusterPort   ClusterPort
	Online        online.Registry
	Delivery      online.Delivery
	Sequence      sequence.Allocator
	Now           func() time.Time
}

type App struct {
	identities IdentityStore
	channels   ChannelStore
	cluster    ClusterPort
	online     online.Registry
	delivery   online.Delivery
	sequence   sequence.Allocator
	now        func() time.Time
}

func New(opts Options) *App {
	if opts.Online == nil {
		opts.Online = online.NewRegistry()
	}
	if opts.Delivery == nil {
		opts.Delivery = online.LocalDelivery{}
	}
	if opts.Sequence == nil {
		opts.Sequence = &sequence.MemoryAllocator{}
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}

	return &App{
		identities: opts.IdentityStore,
		channels:   opts.ChannelStore,
		cluster:    opts.ClusterPort,
		online:     opts.Online,
		delivery:   opts.Delivery,
		sequence:   opts.Sequence,
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
	GetUser(ctx context.Context, uid string) (wkdb.User, error)
}

type ChannelStore interface {
	GetChannel(ctx context.Context, channelID string, channelType int64) (wkdb.Channel, error)
}

type ClusterPort interface{}
