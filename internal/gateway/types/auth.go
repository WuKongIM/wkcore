package types

import "github.com/WuKongIM/WuKongIM/pkg/proto/wkpacket"

type Authenticator interface {
	Authenticate(ctx *Context, connect *wkpacket.ConnectPacket) (*AuthResult, error)
}

type AuthenticatorFunc func(ctx *Context, connect *wkpacket.ConnectPacket) (*AuthResult, error)

func (f AuthenticatorFunc) Authenticate(ctx *Context, connect *wkpacket.ConnectPacket) (*AuthResult, error) {
	if f == nil {
		return nil, nil
	}
	return f(ctx, connect)
}

type AuthResult struct {
	Connack       *wkpacket.ConnackPacket
	SessionValues map[string]any
}
