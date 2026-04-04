package types

import (
	"context"

	"github.com/WuKongIM/WuKongIM/internal/gateway/session"
	"github.com/WuKongIM/WuKongIM/pkg/protocol/wkframe"
)

type Handler interface {
	OnListenerError(listener string, err error)
	OnSessionOpen(ctx *Context) error
	OnFrame(ctx *Context, frame wkframe.Frame) error
	OnSessionClose(ctx *Context) error
	OnSessionError(ctx *Context, err error)
}

type Context struct {
	Session        session.Session
	Listener       string
	Network        string
	Transport      string
	Protocol       string
	CloseReason    CloseReason
	ReplyToken     string
	RequestContext context.Context
}

func (ctx *Context) WriteFrame(frame wkframe.Frame) error {
	if ctx == nil || ctx.Session == nil {
		return session.ErrSessionClosed
	}
	return ctx.Session.WriteFrame(frame, session.WithReplyToken(ctx.ReplyToken))
}
