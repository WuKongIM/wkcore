package binding

import "github.com/WuKongIM/WuKongIM/internal/gateway"

func TCPWKProto(name, address string) gateway.ListenerOptions {
	return listener(name, "tcp", address, "stdnet", "wkproto")
}

func WSJSONRPC(name, address string) gateway.ListenerOptions {
	opts := listener(name, "websocket", address, "stdnet", "jsonrpc")
	opts.Path = DefaultWSPath
	return opts
}
