package gateway

import (
	"fmt"
	"strings"
	"time"
)

type Options struct {
	Handler        Handler
	DefaultSession SessionOptions
	Listeners      []ListenerOptions
}

type ListenerOptions struct {
	Name      string
	Network   string
	Address   string
	Path      string
	Transport string
	Protocol  string
}

type SessionOptions struct {
	ReadBufferSize      int
	WriteQueueSize      int
	MaxInboundBytes     int
	MaxOutboundBytes    int
	IdleTimeout         time.Duration
	WriteTimeout        time.Duration
	CloseOnHandlerError bool
}

func DefaultSessionOptions() SessionOptions {
	return SessionOptions{
		ReadBufferSize:      4 << 10,
		WriteQueueSize:      64,
		MaxInboundBytes:     1 << 20,
		MaxOutboundBytes:    1 << 20,
		IdleTimeout:         30 * time.Second,
		WriteTimeout:        10 * time.Second,
		CloseOnHandlerError: true,
	}
}

func (o *Options) Validate() error {
	if o == nil {
		return fmt.Errorf("gateway: nil options")
	}
	o.DefaultSession = normalizeSessionOptions(o.DefaultSession)
	seen := make(map[string]struct{}, len(o.Listeners))
	for _, listener := range o.Listeners {
		name := strings.TrimSpace(listener.Name)
		network := strings.TrimSpace(listener.Network)
		address := strings.TrimSpace(listener.Address)
		transport := strings.TrimSpace(listener.Transport)
		protocol := strings.TrimSpace(listener.Protocol)

		if name == "" {
			return ErrListenerNameEmpty
		}
		if _, ok := seen[name]; ok {
			return ErrListenerNameDuplicate
		}
		seen[name] = struct{}{}
		if address == "" {
			return ErrListenerAddressEmpty
		}
		if network == "" {
			return ErrListenerNetworkEmpty
		}
		if transport == "" {
			return ErrListenerTransportEmpty
		}
		if protocol == "" {
			return ErrListenerProtocolEmpty
		}
		if strings.EqualFold(network, "websocket") && strings.TrimSpace(listener.Path) == "" {
			return ErrListenerWebsocketPath
		}
	}
	if o.Handler == nil {
		return ErrNilHandler
	}
	return nil
}

func normalizeSessionOptions(opt SessionOptions) SessionOptions {
	def := DefaultSessionOptions()
	zeroValue := opt == (SessionOptions{})
	if zeroValue {
		return def
	}
	if opt.ReadBufferSize == 0 {
		opt.ReadBufferSize = def.ReadBufferSize
	}
	if opt.WriteQueueSize == 0 {
		opt.WriteQueueSize = def.WriteQueueSize
	}
	if opt.MaxInboundBytes == 0 {
		opt.MaxInboundBytes = def.MaxInboundBytes
	}
	if opt.MaxOutboundBytes == 0 {
		opt.MaxOutboundBytes = def.MaxOutboundBytes
	}
	if opt.IdleTimeout == 0 {
		opt.IdleTimeout = def.IdleTimeout
	}
	if opt.WriteTimeout == 0 {
		opt.WriteTimeout = def.WriteTimeout
	}
	if zeroValue {
		opt.CloseOnHandlerError = def.CloseOnHandlerError
	}
	return opt
}
