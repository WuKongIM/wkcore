# Conversation Sync Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a high-performance conversation system with user catalog, hot/cold channel update index, cold wakeup handling, and a new `/conversation/sync` API that preserves legacy conversation item fields while replacing offset paging with cursor sync.

**Architecture:** Conversation correctness comes from `UserConversationState + channellog`, acceleration comes from `ChannelUpdateLog`, and cold channel resurrection comes from `UserConversationWakeup`. The HTTP API stays thin in `internal/access/api`, orchestration lives in `internal/usecase/conversation`, and all durable state changes flow through `pkg/storage/metastore` and `pkg/storage/metafsm`.

**Tech Stack:** Go, Gin, Pebble-backed `metadb`, Raft-backed `metastore`, existing `channellog`, current async committed dispatcher in `internal/app`

---

### Task 1: Add durable metadata tables and shard-store APIs

**Files:**
- Modify: `pkg/storage/metadb/catalog.go`
- Modify: `pkg/storage/metadb/codec.go`
- Modify: `pkg/storage/metadb/batch.go`
- Create: `pkg/storage/metadb/user_conversation.go`
- Create: `pkg/storage/metadb/channel_update_log.go`
- Create: `pkg/storage/metadb/user_conversation_wakeup.go`
- Test: `pkg/storage/metadb/user_conversation_test.go`
- Test: `pkg/storage/metadb/channel_update_log_test.go`
- Test: `pkg/storage/metadb/user_conversation_wakeup_test.go`

- [ ] **Step 1: Write failing storage tests**

Add tests for:
- idempotent ensure of user conversation rows
- monotonic `read_seq` and `deleted_to_seq` updates
- channel update log upsert and ordered range scan
- wakeup row upsert and list/delete semantics

- [ ] **Step 2: Run targeted metadb tests and verify they fail**

Run: `go test ./pkg/storage/metadb -run 'Test(UserConversation|ChannelUpdateLog|UserConversationWakeup)'`

- [ ] **Step 3: Implement new table descriptors and codecs**

Add table IDs, column IDs, key encoders, family encoders, and decode helpers for:
- `user_conversation`
- `channel_update_log`
- `user_conversation_wakeup`

- [ ] **Step 4: Implement shard store CRUD/query APIs**

Expose:
- ensure/list/get user conversations
- monotonic read/delete updates
- upsert/list recent channel update log
- upsert/list/delete wakeups

- [ ] **Step 5: Extend write batch helpers**

Add batch operations used by raft apply paths so later tasks can update durable conversation metadata atomically.

- [ ] **Step 6: Re-run targeted metadb tests**

Run: `go test ./pkg/storage/metadb -run 'Test(UserConversation|ChannelUpdateLog|UserConversationWakeup)'`

### Task 2: Add metafsm commands and distributed metastore APIs

**Files:**
- Modify: `pkg/storage/metafsm/command.go`
- Modify: `pkg/storage/metafsm/state_machine_test.go`
- Modify: `pkg/storage/metastore/store.go`
- Create: `pkg/storage/metastore/user_conversation_rpc.go`
- Create: `pkg/storage/metastore/channel_update_log_rpc.go`
- Create: `pkg/storage/metastore/user_conversation_wakeup_rpc.go`
- Test: `pkg/storage/metastore/integration_test.go`

- [ ] **Step 1: Write failing authoritative metastore tests**

Cover:
- user owner slot reads and writes for conversation state
- range reads for channel update log
- wakeup reads/cleanup on remote authoritative slot

- [ ] **Step 2: Run targeted metastore tests and verify they fail**

Run: `go test ./pkg/storage/metastore -run 'TestStore(Conversation|ChannelUpdateLog|Wakeup)'`

- [ ] **Step 3: Add new metafsm command types**

Add encode/decode/apply support for:
- ensure/upsert user conversation
- update read seq
- update deleted seq
- upsert channel update log
- upsert/delete wakeup rows

- [ ] **Step 4: Add metastore public APIs and RPC handlers**

Expose authoritative APIs for:
- `EnsureUserConversation`
- `BatchEnsureUserConversations`
- `ListUserConversations`
- `UpdateUserConversationReadSeq`
- `UpdateUserConversationDeletedToSeq`
- `UpsertChannelUpdateLog`
- `ListChannelUpdateLogSince`
- `UpsertUserConversationWakeups`
- `ListUserConversationWakeups`
- `DeleteUserConversationWakeups`

- [ ] **Step 5: Re-run targeted metastore tests**

Run: `go test ./pkg/storage/metastore -run 'TestStore(Conversation|ChannelUpdateLog|Wakeup)'`

### Task 3: Add conversation use case and sync orchestration

**Files:**
- Create: `internal/usecase/conversation/app.go`
- Create: `internal/usecase/conversation/deps.go`
- Create: `internal/usecase/conversation/types.go`
- Create: `internal/usecase/conversation/sync.go`
- Create: `internal/usecase/conversation/direct_bootstrap.go`
- Test: `internal/usecase/conversation/sync_test.go`

- [ ] **Step 1: Write failing use case tests**

Cover:
- active working set + wakeups + client-known union
- `only_unread` filtering
- person channel real/fake ID mapping
- fallback to `channellog` when update log misses
- cursor-based pagination with stable ordering

- [ ] **Step 2: Run targeted conversation use case tests and verify they fail**

Run: `go test ./internal/usecase/conversation -run 'TestSync'`

- [ ] **Step 3: Implement conversation ports and core sync result types**

Define storage and channel-inspection interfaces with explicit methods instead of reusing unrelated message interfaces.

- [ ] **Step 4: Implement sync orchestration**

Implement:
- candidate gathering from working set, wakeups, and client-known channels
- update-log-assisted filtering
- `channellog` status and recent lookup
- unread/read/delete calculations
- cursor generation and parsing

- [ ] **Step 5: Implement person direct bootstrap hook**

Add a small helper for brand-new person channel pairs so offline recipients do not lose new conversations.

- [ ] **Step 6: Re-run targeted conversation use case tests**

Run: `go test ./internal/usecase/conversation -run 'TestSync'`

### Task 4: Add hot update index projector and wakeup worker

**Files:**
- Create: `internal/usecase/conversation/projector.go`
- Create: `internal/usecase/conversation/hotindex.go`
- Create: `internal/usecase/conversation/wakeup.go`
- Create: `internal/access/node/conversation_hot_rpc.go`
- Modify: `internal/access/node/options.go`
- Modify: `internal/access/node/service_ids.go`
- Modify: `internal/access/node/client.go`
- Test: `internal/usecase/conversation/projector_test.go`
- Test: `internal/access/node/conversation_hot_rpc_test.go`

- [ ] **Step 1: Write failing projector and hot-index tests**

Cover:
- in-memory coalescing
- periodic flush collapsing many messages into one durable upsert
- hot lookup RPC
- cold-to-hot detection creates wakeup tasks once

- [ ] **Step 2: Run targeted projector tests and verify they fail**

Run: `go test ./internal/usecase/conversation -run 'Test(Projector|HotIndex|Wakeup)' ./internal/access/node -run 'TestConversationHot'`

- [ ] **Step 3: Implement hot index and flush loop**

Implement:
- owner-local overwrite map
- timer/threshold flush
- best-effort shutdown flush

- [ ] **Step 4: Implement cold wakeup flow**

Add:
- cold-threshold detection
- subscriber snapshot expansion for groups
- batched wakeup upserts

- [ ] **Step 5: Implement node RPC for hot index lookups**

Allow sync requests on a user owner node to ask channel owners for recent hot updates when `version > 0`.

- [ ] **Step 6: Re-run targeted projector tests**

Run: `go test ./internal/usecase/conversation -run 'Test(Projector|HotIndex|Wakeup)' ./internal/access/node -run 'TestConversationHot'`

### Task 5: Wire conversation into message send path and app composition

**Files:**
- Modify: `internal/usecase/message/app.go`
- Modify: `internal/usecase/message/deps.go`
- Modify: `internal/usecase/message/send.go`
- Modify: `internal/usecase/message/send_test.go`
- Modify: `internal/app/build.go`
- Modify: `internal/app/deliveryrouting.go`

- [ ] **Step 1: Write failing wiring tests**

Cover:
- direct person bootstrap invoked only when needed
- committed dispatcher reaches conversation projector asynchronously
- app build wires all required dependencies

- [ ] **Step 2: Run targeted message/app tests and verify they fail**

Run: `go test ./internal/usecase/message -run 'TestSend' ./internal/app -run 'Test(Build|DeliveryRouting)'`

- [ ] **Step 3: Add optional conversation hooks to message use case**

Add narrow ports instead of leaking full conversation app into message package.

- [ ] **Step 4: Wire projector into committed dispatcher path**

Integrate conversation projector alongside delivery dispatch without blocking append success.

- [ ] **Step 5: Wire full conversation app in composition root**

Construct:
- conversation use case
- projector
- hot index RPC handler
- API dependency injection

- [ ] **Step 6: Re-run targeted message/app tests**

Run: `go test ./internal/usecase/message -run 'TestSend' ./internal/app -run 'Test(Build|DeliveryRouting)'`

### Task 6: Add HTTP API and legacy field mapping

**Files:**
- Modify: `internal/access/api/server.go`
- Modify: `internal/access/api/routes.go`
- Create: `internal/access/api/conversation_sync.go`
- Create: `internal/access/api/conversation_legacy_model.go`
- Modify: `internal/access/api/server_test.go`
- Modify: `internal/access/api/integration_test.go`

- [ ] **Step 1: Write failing API tests**

Cover:
- request validation
- legacy request field parsing
- response item field compatibility
- cursor/sync_version response envelope
- direct chat sync for an offline recipient

- [ ] **Step 2: Run targeted API tests and verify they fail**

Run: `go test ./internal/access/api -run 'Test(Conversation|APIServerConversation)'`

- [ ] **Step 3: Add conversation use case interface to API server**

Keep HTTP handler thin and independent of storage details.

- [ ] **Step 4: Implement `/conversation/sync` handler**

Map request JSON to use case query and convert use case output to the legacy item format plus new cursor envelope.

- [ ] **Step 5: Register route and integration wiring**

Add the route to the API server and ensure app startup exposes it when API is enabled.

- [ ] **Step 6: Re-run targeted API tests**

Run: `go test ./internal/access/api -run 'Test(Conversation|APIServerConversation)'`

### Task 7: Run focused verification and full regression checks

**Files:**
- Modify: `docs/superpowers/specs/2026-04-07-conversation-sync-design.md` if implementation-level clarifications are needed

- [ ] **Step 1: Run focused package tests**

Run:
- `go test ./pkg/storage/metadb`
- `go test ./pkg/storage/metastore`
- `go test ./internal/usecase/conversation`
- `go test ./internal/access/node`
- `go test ./internal/access/api`
- `go test ./internal/usecase/message`
- `go test ./internal/app`

- [ ] **Step 2: Run broader regression suite**

Run: `go test ./internal/... ./pkg/...`

- [ ] **Step 3: Sanity-check no unrelated API regressions**

Run: `go test ./cmd/... ./internal/access/...`

- [ ] **Step 4: Update spec/plan notes if implementation diverged**

Record any deliberate divergence from the approved design.
