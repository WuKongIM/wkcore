package transport

import "context"

// NodeID identifies a cluster node. Type alias for uint64 — zero-cost
// conversion with multiraft.NodeID (which is a named uint64 type, requiring
// an explicit cast at call sites).
type NodeID = uint64

// MessageHandler processes an inbound message of a specific type.
// The body is valid only for the duration of the callback; copy it if it must
// be retained asynchronously.
type MessageHandler func(body []byte)

// RPCHandler processes an inbound RPC request and returns a response body.
// The ctx passed by the Server is context.Background(). The handler is responsible
// for applying its own timeout.
type RPCHandler func(ctx context.Context, body []byte) ([]byte, error)

// Discovery resolves a NodeID to a network address.
type Discovery interface {
	Resolve(nodeID NodeID) (addr string, err error)
}

type ObserverHooks struct {
	OnSend    func(msgType uint8, bytes int)
	OnReceive func(msgType uint8, bytes int)
}

type PoolPeerStats struct {
	NodeID NodeID
	Active int
	Idle   int
}
