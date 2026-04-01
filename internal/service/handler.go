package service

import (
	"github.com/WuKongIM/WuKongIM/internal/gateway"
)

func (s *Service) OnListenerError(string, error) {
}

func (s *Service) OnSessionOpen(ctx *gateway.Context) error {
	if s == nil {
		return nil
	}

	meta, err := sessionMetaFromContext(ctx, s.opts.Now())
	if err != nil {
		return err
	}

	return s.registry.Register(meta)
}

func (s *Service) OnSessionClose(ctx *gateway.Context) error {
	if s == nil || ctx == nil || ctx.Session == nil {
		return nil
	}

	s.registry.Unregister(ctx.Session.ID())
	return nil
}

func (s *Service) OnSessionError(*gateway.Context, error) {
}
