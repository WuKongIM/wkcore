// Package isrnodetransport provides the concrete nodetransport-backed adapter
// for isrnode data-plane fetch traffic. Control-plane forwarding and data-plane
// fetch serving stay isolated behind fixed RPC service IDs on the shared node
// transport mux. V1 reuses one session per peer and intentionally allows at
// most one in-flight fetch RPC per peer, surfacing that pressure back to
// isrnode.
package isrnodetransport
