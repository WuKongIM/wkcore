package gateway

import (
	"errors"
	"time"

	coregateway "github.com/WuKongIM/WuKongIM/internal/gateway"
	"github.com/WuKongIM/WuKongIM/internal/runtime/online"
	"github.com/WuKongIM/WuKongIM/internal/usecase/message"
)

var ErrUnsupportedFrame = errors.New("access/gateway: unsupported frame")
var ErrUnauthenticatedSession = errors.New("access/gateway: unauthenticated session")

type MessageUsecase interface {
	Send(cmd message.SendCommand) (message.SendResult, error)
	RecvAck(cmd message.RecvAckCommand) error
}

type Options struct {
	Online   online.Registry
	Messages MessageUsecase
	Now      func() time.Time
}

type Handler struct {
	online   online.Registry
	messages MessageUsecase
	now      func() time.Time
}

func New(opts Options) *Handler {
	if opts.Now == nil {
		opts.Now = time.Now
	}
	if opts.Online == nil {
		if provider, ok := opts.Messages.(interface{ OnlineRegistry() online.Registry }); ok {
			opts.Online = provider.OnlineRegistry()
		}
		if opts.Online == nil {
			opts.Online = online.NewRegistry()
		}
	}
	if opts.Messages == nil {
		opts.Messages = message.New(message.Options{
			Online: opts.Online,
			Now:    opts.Now,
		})
	}

	return &Handler{
		online:   opts.Online,
		messages: opts.Messages,
		now:      opts.Now,
	}
}

func (h *Handler) OnListenerError(string, error) {}

func (h *Handler) OnSessionOpen(ctx *coregateway.Context) error {
	if h == nil {
		return nil
	}

	conn, err := onlineConnFromContext(ctx, h.now())
	if err != nil {
		return err
	}
	return h.online.Register(conn)
}

func (h *Handler) OnSessionClose(ctx *coregateway.Context) error {
	if h == nil || ctx == nil || ctx.Session == nil {
		return nil
	}

	h.online.Unregister(ctx.Session.ID())
	return nil
}

func (h *Handler) OnSessionError(*coregateway.Context, error) {}
