package gateway

import "github.com/WuKongIM/WuKongIM/pkg/wkpacket"

type Handler interface {
	OnListenerError(listener string, err error)
	OnSessionOpen(ctx *Context) error
	OnFrame(ctx *Context, frame wkpacket.Frame) error
	OnSessionClose(ctx *Context) error
	OnSessionError(ctx *Context, err error)
}

type Session interface {
	WriteFrame(frame wkpacket.Frame, opts ...WriteOption) error
}

type Context struct {
	Session     Session
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
	return ctx.Session.WriteFrame(frame, WithReplyToken(ctx.ReplyToken))
}

type WriteOption interface {
	apply(*OutboundMeta)
}

type OutboundMeta struct {
	ReplyToken string
}

type replyTokenOption string

func (o replyTokenOption) apply(meta *OutboundMeta) {
	meta.ReplyToken = string(o)
}

func WithReplyToken(token string) WriteOption {
	return replyTokenOption(token)
}
