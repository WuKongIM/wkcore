# Presence Memory Leader Design

## Goal

Add a cluster-aware online routing mechanism for person-channel realtime delivery with:

- users allowed to connect to any gateway node
- slot-leader-authoritative online routing during normal operation
- no replicated presence storage
- no client-side reconnect catch-up in this version
- bounded steady-state network and CPU cost at large connection counts

This design only covers routing online sessions for realtime push.

## Non-Goals

- replicated or durable presence storage
- zero-gap realtime delivery across slot leader failover
- client reconnect catch-up or offline gap repair
- full remote gateway delivery implementation details outside the routing contract

## Problem

`internal/runtime/online.Registry` is node-local memory only.
That is enough for local fanout but not enough for a slot leader on another node to answer:

- which node a user is connected to
- how many devices are online
- which session ids belong to those devices

The current repository intentionally excludes cross-node online fanout from the multi-node durable write scope.

At the same time, a naive per-connection periodic refresh design does not scale.
If a node holds 100k sessions and refreshes all of them individually to a remote authority, steady-state control traffic and authority CPU become linear in connection count.

## Decision

V1 will use an in-memory authoritative directory owned by the current slot leader.

Rules:

- presence authority is partitioned by `uid` slot
- only the current slot leader for that group owns the authoritative in-memory directory
- gateway connect success requires successful presence registration on the current authority
- steady-state liveness is maintained by gateway-to-authority aggregated heartbeats per `(groupID, gatewayNodeID, gatewayBootID)`, not by per-session refresh
- when the authority reports missing state, the gateway first repairs by batch replay of affected local routes
- client disconnect is only a last-resort repair path

This gives a strong normal-path invariant:

```text
client sees connect success => current slot leader knows this route
```

But it does not provide durable failover continuity:

- if the slot leader changes, the new leader starts with an empty in-memory directory
- gateways must replay affected local routes to rebuild authority state
- during rebuild, realtime push can be degraded

## Why Not Replicated Presence

Replicated presence would remove failover state loss, but it is explicitly out of scope for this version.

The chosen design keeps implementation smaller by avoiding:

- new replicated metadb tables
- new metafsm commands
- authoritative metastore persistence APIs
- replicated presence recovery logic

## Why Not Per-Connection Refresh

Per-connection refresh is rejected for steady-state scalability reasons.

With 100k local sessions on one gateway node:

- every refresh interval would require scanning and encoding 100k route records
- the authority would decode and update 100k route entries even when nothing changed
- multiple large gateways would create linear control-plane load on the authority leader

That is the wrong cost model.

V1 must make steady-state work proportional to:

- active authority groups on the gateway node
- route churn

not proportional to:

- total live connection count

## Architecture

### Node-Local Runtime

`internal/runtime/online` remains the source of truth for local live sessions.

It owns:

- local `session.Session`
- local session lookup by `sessionID`
- local user-to-sessions membership

It does not become a distributed directory.

### Presence Usecase

Add a new `internal/usecase/presence` package.

It owns:

- connect-path route registration orchestration
- disconnect-path unregister orchestration
- authority heartbeat and replay orchestration
- query API for online route lookup by uid

It does not own wire protocols or local session objects.

### Node Access Layer

Add a new `internal/access/node` package for node-to-node routing RPC.

It owns:

- authority-side presence RPC handlers
- remote route delivery RPC handlers

It does not own presence business policy.

### App Composition

`internal/app` remains the only composition root.

It wires:

- local `online.Registry`
- `presence` usecase
- gateway pre-ack activation hook
- node RPC handlers on shared cluster transport

## Data Model

### Route Identity

Each routable online session is identified by:

```text
(nodeID, bootID, sessionID)
```

`sessionID` alone is not globally safe because it is process-local.

`bootID` is a random process incarnation id created at gateway startup.

This identity is used for:

- authority route registration
- replay payloads
- remote delivery fencing

### Route Record

The authority leader stores in memory:

```go
type Route struct {
    UID         string
    NodeID      uint64
    BootID      uint64
    SessionID   uint64
    DeviceFlag  uint8
    DeviceLevel uint8
    Listener    string
}
```

Recommended indexes:

- `uid -> []Route`
- `groupID + gatewayNodeID + bootID -> lease`
- `groupID + gatewayNodeID + bootID -> []RouteRef`

### Gateway Lease

The authority does not require per-route refresh.

Instead it tracks a gateway lease:

```go
type GatewayLease struct {
    GroupID        uint64
    GatewayNodeID  uint64
    GatewayBootID  uint64
    RouteCount     int
    LeaseUntilUnix int64
}
```

The lease means:

- this gateway still claims ownership of a known set of routes for this group
- all routes attached to that lease remain valid while the lease is alive

If a lease expires, the authority removes all routes attached to that gateway/group lease.

## Connect Activation

### Required Gateway Hook

Current gateway flow writes success `CONNACK` before `OnSessionOpen`.
That is too late for authoritative route registration.

V1 requires a pre-ack activation hook after authentication succeeds and before success `CONNACK` is written.

Suggested interface:

```go
type SessionActivator interface {
    OnSessionActivate(ctx *types.Context) (*wkframe.ConnackPacket, error)
}
```

### Activation Sequence

For authenticated wkproto sessions:

1. gateway authenticates connect packet
2. gateway writes auth-derived session values
3. gateway constructs local route metadata
4. gateway registers the route in local `online.Registry`
5. gateway synchronously calls `presence.RegisterRoute(...)`
6. if authority registration succeeds, gateway writes success `CONNACK`
7. if authority registration fails, gateway rolls back local registration and rejects connect

This enforces:

```text
successful connect implies routable-at-authority
```

### Failure Behavior

If authority registration fails:

- remove the just-registered local route from `online.Registry`
- return a retryable connect failure
- do not enter connected state

## Authority RPCs

The authority side needs four logical operations.

### RegisterRoute

Used only on connect path.

Input:

- `groupID`
- full `Route`

Behavior:

- route request to current authority leader
- insert or replace the route
- attach the route to the gateway lease owner set
- create or extend the lease for that `(groupID, gatewayNodeID, gatewayBootID)`

### UnregisterRoute

Used on disconnect path as best effort.

Input:

- `groupID`
- `uid`
- `nodeID`
- `bootID`
- `sessionID`

Behavior:

- remove the route if present
- update owner set accounting

### HeartbeatLease

Used periodically per active `(groupID, gatewayNodeID, gatewayBootID)`.

Input:

- `groupID`
- `gatewayNodeID`
- `gatewayBootID`
- `routeCount`

Behavior:

- refresh the lease deadline
- verify that authority still has route state for that owner set
- return one of:
  - `ok`
  - `replay_required`
  - `not_leader(newLeader)`

Important:

- heartbeat does not carry all routes
- heartbeat does not update route membership one-by-one

### ReplayRoutes

Used only for repair after authority loss or lease inconsistency.

Input:

- `groupID`
- `gatewayNodeID`
- `gatewayBootID`
- full route list for that owner set

Behavior:

- replace authority-side route membership for that owner set
- rebuild `uid -> routes` entries for those routes
- set the gateway lease deadline

`ReplayRoutes` is intentionally batch-oriented and only used on repair paths, not steady state.

## Gateway-Side Indexing

Each gateway node must maintain local grouping so it can avoid scanning all connections for every heartbeat.

Recommended local indexes:

- `sessionID -> Route`
- `groupID -> map[sessionID]Route`
- `groupID + authorityNodeID -> owner state`

This enables:

- connect/disconnect updating one group bucket
- periodic heartbeat by group bucket
- batch replay of only affected groups

The gateway must not rescan all sessions globally every interval.

## Heartbeat Model

### Authority Discovery

The gateway determines each route's authority group using the same slot mapping as the rest of the cluster.

For each active local group bucket:

- find current leader
- send one lease heartbeat for that bucket

### Recommended Intervals

Start with:

- heartbeat interval: `10s`
- lease ttl: `30s`

Requirements:

- ttl must be comfortably larger than interval
- expiration must not flap under short network jitter

### Performance Model

Steady-state network volume scales with:

- number of non-empty local authority groups

Steady-state network volume does not scale with:

- total local routes inside those groups

That is the main scalability goal of the design.

## Leader Change And Memory Loss

### Expected Behavior

When authority leader changes:

- new leader starts with empty in-memory route directory
- existing gateway leases are absent
- the next heartbeat from each gateway gets `replay_required` or `not_leader`

### Repair Sequence

For an affected local group bucket:

1. gateway detects `not_leader(newLeader)` or `replay_required`
2. gateway resolves current leader
3. gateway immediately issues `ReplayRoutes` for that group bucket
4. subsequent heartbeats return `ok`

Repair is node-side.
This version does not depend on client reconnect to repair authority state.

## Why Replay Before Disconnect

Client reconnect is not the primary repair tool in this version.

Reasons:

- client reconnect catch-up is explicitly out of scope
- without catch-up, disconnecting sessions can create real user-visible message gaps
- authority state loss is an internal control-plane event and should first be repaired internally

Therefore:

- first attempt batch replay
- only disconnect as a last resort when replay repeatedly fails and the route can no longer be trusted

## Query Path

`internal/usecase/message` should not query local `online.Registry` for person-channel remote recipients anymore.

It should call the presence usecase:

```go
EndpointsByUID(ctx, uid) ([]Endpoint, error)
```

The presence usecase asks the current authority leader for that `uid`.

Result:

```go
type Endpoint struct {
    NodeID     uint64
    BootID     uint64
    SessionID  uint64
    DeviceFlag uint8
}
```

The sender groups endpoints by `NodeID`:

- local node: direct local write
- remote node: remote delivery RPC

## Remote Delivery Fencing

Remote delivery must target:

```text
(nodeID, bootID, sessionID)
```

The target node only writes the packet if:

- request `bootID` matches current local gateway boot id
- local `online.Registry` contains `sessionID`
- stored route `uid` still matches target `uid`

Otherwise the target silently drops that route target.

This prevents stale RPCs from reaching sessions after process restart.

## Failure Matrix

### Connect Path

- auth success + register success:
  - connect succeeds
- auth success + register failure:
  - connect fails
  - local route registration rolled back

### Disconnect Path

- unregister success:
  - route removed immediately
- unregister failure:
  - route remains until lease expiry or later replay correction

### Heartbeat Path

- heartbeat `ok`:
  - do nothing
- heartbeat `replay_required`:
  - issue batch replay for that group bucket
- heartbeat `not_leader(newLeader)`:
  - switch target leader and replay immediately

### Authority Loss

- leader memory lost:
  - authority responds with replay-needed state
  - gateway repairs with batch replay

### Repair Failure

If replay repeatedly fails for a specific group bucket:

- mark that bucket degraded
- continue retrying on bounded backoff
- optional last-resort session disconnect may be added later, but is not the primary V1 path

## Semantics

V1 guarantees:

- successful connect means the current authority knows the route
- steady-state liveness cost is aggregated by group lease, not per route
- authority memory loss can be repaired by gateway batch replay

V1 does not guarantee:

- uninterrupted realtime push during authority leader transition
- durable recovery of authority route state across leader restart
- message gap repair via client reconnect in this version

## Testing

### Gateway Core

Add tests for the new pre-ack activation hook:

- activation success writes success `CONNACK`
- activation failure rejects connect
- activation failure rolls back local route registration

### Presence Usecase

Add tests for:

- register attaches route to group owner bucket
- unregister removes route
- heartbeat `ok`
- heartbeat `replay_required`
- replay rebuilds missing authority state
- leader change triggers replay against new leader

### Node RPC

Add tests for:

- authority register/unregister
- authority heartbeat responses
- authority replay replace semantics
- remote delivery fencing on `bootID + sessionID`

### Multi-Node Integration

Add integration tests for:

- user connected on node 1 is discoverable by authority on node 2
- authority heartbeat remains one request per non-empty group bucket, not per session
- simulated leader change empties authority memory and is repaired by gateway replay
- realtime delivery resumes after replay repair

### Performance-Focused Tests

Add at least one test harness or benchmark showing:

- 100k local routes concentrated in one or a few groups do not produce 100k periodic refresh RPCs
- steady-state heartbeat count is bounded by active group buckets

## Rollout Notes

This design intentionally defers:

- replicated presence
- reconnect catch-up
- authoritative state transfer from old leader to new leader

Those can be added later without changing the core choice that steady-state liveness must be lease-based and aggregated rather than per-route refreshed.
