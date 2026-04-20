// Package transport provides the node-transport-backed adapter for the channel
// data plane. In `progress_ack` mode it keeps the legacy short
// Fetch/FetchBatch/ProgressAck path; in `long_poll` mode steady-state
// replication is driven by fixed `(peer,lane)` LongPollFetch RPCs that park on
// the leader and reissue from the follower. ReconcileProbe stays separate from
// the steady-state path in both modes.
package transport
