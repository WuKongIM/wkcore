# AGENTS.md

Module: `github.com/WuKongIM/wraft` | Go 1.23

## Packages

- `multiraft` — Multi-group Raft runtime (etcd/raft RawNode)
- `wkdb` — Pebble-backed slot-scoped KV store, snapshot, raft storage
- `wkdbraft` — Adapter bridging wkdb and multiraft

**Slot = Group**: `multiraft.GroupID` == `wkdb` slot ID. All data slot-prefixed.

## Test

```bash
go test ./...          # all
go test -race ./...    # with race detector
```

TDD: write failing test first, then implement. Tests co-located as `*_test.go`.

## Commit

`type: description` — types: feat, fix, refactor, test, docs, chore

## Rules

1. No circular dependencies between three packages
2. All business keys MUST be slot-prefixed
3. Raft keyspace separate from business keyspace
4. Default English; Chinese only if requested
