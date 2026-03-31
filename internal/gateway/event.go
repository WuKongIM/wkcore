package gateway

import (
	"github.com/WuKongIM/WuKongIM/internal/gateway/session"
	"github.com/WuKongIM/WuKongIM/pkg/wkpacket"
)

type Handler interface {
	OnListenerError(listener string, err error)
	OnSessionOpen(ctx *Context) error
	OnFrame(ctx *Context, frame wkpacket.Frame) error
	OnSessionClose(ctx *Context) error
	OnSessionError(ctx *Context, err error)
}

type Context struct {
	Session     session.Session
	Listener    string
	Network     string
	Transport   string
	Protocol    string
	CloseReason CloseReason
	ReplyToken  string
}

func (ctx *Context) WriteFrame(frame wkpacket.Frame) error {
	if ctx == nil || ctx.Session == nil {
		return ErrSessionClosed
	}
	return ctx.Session.WriteFrame(frame, session.WithReplyToken(ctx.ReplyToken))
}
