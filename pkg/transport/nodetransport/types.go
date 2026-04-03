package nodetransport

import (
	"context"
	"net"
)

// NodeID identifies a cluster node. Type alias for uint64 — zero-cost
// conversion with multiraft.NodeID (which is a named uint64 type, requiring
// an explicit cast at call sites).
type NodeID = uint64

// MessageHandler processes an inbound message of a specific type.
// conn is provided so the handler can write back on the same connection if needed.
type MessageHandler func(conn net.Conn, body []byte)

// RPCHandler processes an inbound RPC request and returns a response body.
// The ctx passed by the Server is context.Background(). The handler is responsible
// for applying its own timeout (e.g., wkcluster wraps with forwardTimeout).
type RPCHandler func(ctx context.Context, body []byte) ([]byte, error)

// Discovery resolves a NodeID to a network address.
type Discovery interface {
	Resolve(nodeID NodeID) (addr string, err error)
}
