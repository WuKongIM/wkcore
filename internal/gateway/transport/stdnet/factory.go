package stdnet

import (
	"fmt"

	"github.com/WuKongIM/WuKongIM/internal/gateway/transport"
)

const Name = "stdnet"

type Factory struct{}

func NewFactory() *Factory {
	return &Factory{}
}

func (f *Factory) Name() string {
	return Name
}

func (f *Factory) New(opts transport.ListenerOptions, handler transport.ConnHandler) (transport.Listener, error) {
	switch opts.Network {
	case "tcp":
		return NewTCPListener(opts, handler)
	case "websocket":
		return NewWSListener(opts, handler)
	default:
		return nil, fmt.Errorf("gateway/transport/stdnet: unsupported network %q", opts.Network)
	}
}
