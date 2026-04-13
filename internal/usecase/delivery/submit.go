package delivery

import (
	"context"

	"github.com/WuKongIM/WuKongIM/pkg/channel"
)

func (a *App) SubmitCommitted(ctx context.Context, msg channel.Message) error {
	if a == nil || a.runtime == nil {
		return nil
	}
	return a.runtime.Submit(ctx, msg)
}

func (noopRuntime) Submit(context.Context, channel.Message) error {
	return nil
}
