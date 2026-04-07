package delivery

import (
	"context"

	"github.com/WuKongIM/WuKongIM/pkg/storage/channellog"
)

func (a *App) SubmitCommitted(ctx context.Context, msg channellog.Message) error {
	if a == nil || a.runtime == nil {
		return nil
	}
	return a.runtime.Submit(ctx, msg)
}

func (noopRuntime) Submit(context.Context, channellog.Message) error {
	return nil
}
