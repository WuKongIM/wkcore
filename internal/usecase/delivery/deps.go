package delivery

import (
	"context"

	runtimedelivery "github.com/WuKongIM/WuKongIM/internal/runtime/delivery"
	"github.com/WuKongIM/WuKongIM/pkg/storage/channellog"
)

type Runtime interface {
	Submit(ctx context.Context, msg channellog.Message) error
	AckRoute(ctx context.Context, cmd runtimedelivery.RouteAck) error
	SessionClosed(ctx context.Context, cmd runtimedelivery.SessionClosed) error
}
