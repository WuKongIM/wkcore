package types

import "github.com/WuKongIM/WuKongIM/pkg/protocol/wkframe"

type Authenticator interface {
	Authenticate(ctx *Context, connect *wkframe.ConnectPacket) (*AuthResult, error)
}

type AuthenticatorFunc func(ctx *Context, connect *wkframe.ConnectPacket) (*AuthResult, error)

func (f AuthenticatorFunc) Authenticate(ctx *Context, connect *wkframe.ConnectPacket) (*AuthResult, error) {
	if f == nil {
		return nil, nil
	}
	return f(ctx, connect)
}

type AuthResult struct {
	Connack       *wkframe.ConnackPacket
	SessionValues map[string]any
}
