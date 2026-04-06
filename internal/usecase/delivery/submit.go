package delivery

import (
	"context"

	runtimedelivery "github.com/WuKongIM/WuKongIM/internal/runtime/delivery"
	"github.com/WuKongIM/WuKongIM/internal/usecase/message"
)

func (a *App) SubmitCommitted(ctx context.Context, env message.CommittedMessageEnvelope) error {
	if a == nil || a.runtime == nil {
		return nil
	}
	return a.runtime.Submit(ctx, committedEnvelopeFromMessage(env))
}

func (noopRuntime) Submit(context.Context, runtimedelivery.CommittedEnvelope) error {
	return nil
}
