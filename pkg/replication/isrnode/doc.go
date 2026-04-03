// Package isrnode provides node-local orchestration for many ISR groups.
//
// V1 focuses on:
//   - durable per-group generation allocation
//   - active-group and tombstone registry management
//   - ordered multi-group task scheduling
//   - peer-session reuse with envelope demux by group and generation
//   - centralized group, fetch, and snapshot limits
//
// V1 intentionally defers hot-group priority scheduling, more aggressive
// batching heuristics, and finer-grained per-group isolation to later phases.
package isrnode
