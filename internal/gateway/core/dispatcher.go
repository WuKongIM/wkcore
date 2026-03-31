package core

import (
	"github.com/WuKongIM/WuKongIM/internal/gateway"
	"github.com/WuKongIM/WuKongIM/pkg/wkpacket"
)

type dispatcher struct {
	handler gateway.Handler
}

func newDispatcher(handler gateway.Handler) dispatcher {
	return dispatcher{handler: handler}
}

func (d dispatcher) listenerError(listener string, err error) {
	if d.handler == nil || err == nil {
		return
	}
	d.handler.OnListenerError(listener, err)
}

func (d dispatcher) sessionOpen(state *sessionState) error {
	if d.handler == nil {
		return nil
	}
	return d.handler.OnSessionOpen(d.context(state, "", state.closeReason()))
}

func (d dispatcher) frame(state *sessionState, replyToken string, frame wkpacket.Frame) error {
	if d.handler == nil {
		return nil
	}
	return d.handler.OnFrame(d.context(state, replyToken, state.closeReason()), frame)
}

func (d dispatcher) sessionError(state *sessionState, reason gateway.CloseReason, err error) {
	if d.handler == nil || err == nil {
		return
	}
	d.handler.OnSessionError(d.context(state, "", reason), err)
}

func (d dispatcher) sessionClose(state *sessionState) error {
	if d.handler == nil {
		return nil
	}
	return d.handler.OnSessionClose(d.context(state, "", state.closeReason()))
}

func (d dispatcher) context(state *sessionState, replyToken string, reason gateway.CloseReason) *gateway.Context {
	if state == nil || state.listener == nil {
		return &gateway.Context{CloseReason: reason, ReplyToken: replyToken}
	}

	return &gateway.Context{
		Session:     state.session,
		Listener:    state.listener.options.Name,
		Network:     state.listener.options.Network,
		Transport:   state.listener.options.Transport,
		Protocol:    state.listener.options.Protocol,
		CloseReason: reason,
		ReplyToken:  replyToken,
	}
}
