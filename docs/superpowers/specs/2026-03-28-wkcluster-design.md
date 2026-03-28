# wkcluster: Distributed Cluster Coordination Package

## Overview

`wkcluster/` is the top-level coordination package that assembles `multiraft`, `raftstore`, `wkfsm`, and `wkdb` into a functioning distributed cluster. It provides multi-node channel operations with automatic request routing, leader forwarding, and hash-based sharding.

First version scope: Channel CRUD only.

## Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Transport | Raw TCP | Lightweight, no external deps, max performance |
| Node discovery | Static config + Discovery interface | Simple first version, extensible to gossip later |
| Non-leader handling | Server-side forwarding | Client-transparent, simpler client logic |
| Sharding | hash(channelID) % groupCount + 1 | groupID == slot, consistent with wkdb/wkfsm |
| Hash function | crc32.ChecksumIEEE | Hardware-accelerated, already used in wkdb |
| Connection model | Connection pool, grouped by groupID | Avoids head-of-line blocking while preserving per-group ordering |

## Package Structure

```
wkcluster/
├── cluster.go          // Cluster main struct, lifecycle (Start/Stop)
├── transport.go        // TCP Transport, implements multiraft.Transport
├── codec.go            // Wire format encoding/decoding
├── router.go           // channelID → groupID → leaderNodeID routing
├── forward.go          // Non-leader request forwarding
├── discovery.go        // Discovery interface definition
├── static_discovery.go // Static config implementation
├── config.go           // Config types
└── api.go              // Channel CRUD public API
```

## Configuration

```go
type Config struct {
    NodeID     multiraft.NodeID   // This node's ID
    ListenAddr string             // TCP listen address, e.g. ":9001"
    GroupCount uint64             // Total group count, hash maps to groupID
    DataDir    string             // Pebble data directory
    Nodes      []NodeConfig       // Static node list
    Groups     []GroupConfig      // Raft group configuration
}

type NodeConfig struct {
    NodeID multiraft.NodeID
    Addr   string               // e.g. "10.0.0.1:9001"
}

type GroupConfig struct {
    GroupID multiraft.GroupID
    Peers   []multiraft.NodeID   // Member nodes for this group
}
```

Example: 3 nodes, 3 groups:

```
GroupCount: 3
Groups:
  - GroupID: 1, Peers: [1, 2, 3]
  - GroupID: 2, Peers: [1, 2, 3]
  - GroupID: 3, Peers: [1, 2, 3]

hash("channel_abc") % 3 + 1 = 2 → GroupID 2
```

## Discovery Interface

```go
type NodeInfo struct {
    NodeID multiraft.NodeID
    Addr   string
}

type NodeEvent struct {
    Type string    // "join" | "leave"
    Node NodeInfo
}

type Discovery interface {
    GetNodes() []NodeInfo
    Resolve(nodeID multiraft.NodeID) (string, error)
    Watch() <-chan NodeEvent   // Static returns nil
    Stop()
}
```

First version: `StaticDiscovery` backed by `[]NodeConfig`. All components (`Cluster`, `Transport`, `Router`) depend on `Discovery` interface, not static config directly. Future gossip implementation only requires a new `Discovery` implementation.

## Transport (TCP)

### Connection Model

Per node-pair connection pool with group-based connection selection:

```go
type Transport struct {
    nodeID    multiraft.NodeID
    discovery Discovery
    pools     map[multiraft.NodeID]*connPool
    mu        sync.RWMutex
    runtime   *multiraft.Runtime
    listener  net.Listener
}

type connPool struct {
    addr  string
    conns []net.Conn     // Fixed size, select by groupID % len(conns)
    size  int            // Default 4
    mu    []sync.Mutex   // One lock per connection
}

func (p *connPool) GetByGroup(groupID multiraft.GroupID) (net.Conn, *sync.Mutex)
```

Connection selection: `connIndex = groupID % poolSize`. Same group always uses the same connection, guaranteeing in-order delivery within a group while allowing cross-group parallelism.

Connections are established on first use and persist. Each connection has its own mutex to avoid write contention. Broken connections are rebuilt in the same slot.

### Wire Format

```
[msgType:1][bodyLen:4][body:N]
```

Message types:

| msgType | Value | Description |
|---------|-------|-------------|
| msgTypeRaft | 1 | Raft consensus messages |
| msgTypeForward | 2 | Client request forwarded to leader |
| msgTypeResp | 3 | Response to forwarded request |

### Implements multiraft.Transport

`Send(msgs []multiraft.Message)` routes each message through `groupID % poolSize` to select connection, encodes as `msgTypeRaft`, and writes to the connection.

Incoming `msgTypeRaft` messages are decoded and passed to `runtime.Step()`.

## Router

```go
type Router struct {
    groupCount uint64
    runtime    *multiraft.Runtime
    discovery  Discovery
    localNode  multiraft.NodeID
}

func (r *Router) SlotForChannel(channelID string) multiraft.GroupID {
    return multiraft.GroupID(crc32.ChecksumIEEE([]byte(channelID))%uint32(r.groupCount) + 1)
}

func (r *Router) LeaderOf(groupID multiraft.GroupID) (multiraft.NodeID, error)
func (r *Router) IsLocal(nodeID multiraft.NodeID) bool
```

Routing flow: `channelID → SlotForChannel() → groupID → LeaderOf() → leaderNodeID → IsLocal() → local propose or forward`

## Forwarder

```go
type Forwarder struct {
    nodeID    multiraft.NodeID
    transport *Transport
    router    *Router
}

func (f *Forwarder) Forward(groupID multiraft.GroupID, cmdBytes []byte) ([]byte, error)
```

### Forward Wire Format

```
msgTypeForward: [msgType:1][bodyLen:4][groupID:8][cmd:N]
msgTypeResp:    [msgType:1][bodyLen:4][errCode:1][data:N]
```

Error codes:

| errCode | Meaning |
|---------|---------|
| 0 | Success |
| 1 | Not leader (leader migrated, caller should retry) |
| 2 | Timeout |
| 3 | Group not found |

Forwarding is synchronous: the sender goroutine blocks until response arrives. Leader receives the forwarded command, calls `runtime.Propose()`, waits on the Future, and returns the result.

## Cluster (Main Struct)

```go
type Cluster struct {
    cfg       Config
    nodeID    multiraft.NodeID
    runtime   *multiraft.Runtime
    transport *Transport
    discovery Discovery
    router    *Router
    forwarder *Forwarder
    db        *wkdb.DB
}

func NewCluster(cfg Config) (*Cluster, error)
func (c *Cluster) Start() error
func (c *Cluster) Stop()
```

### Startup Sequence

1. Open pebble DB
2. Initialize StaticDiscovery
3. Start Transport (TCP listener)
4. Create multiraft.Runtime (inject transport)
5. For each GroupConfig: `runtime.OpenGroup()` or `BootstrapGroup()`

### Shutdown Sequence

Reverse of startup.

## Public API

```go
// Writes: route to leader
func (c *Cluster) CreateChannel(channelID string, channelType int64) error
func (c *Cluster) UpdateChannel(channelID string, channelType int64, ban int64) error
func (c *Cluster) DeleteChannel(channelID string, channelType int64) error

// Reads: local wkdb, eventual consistency
func (c *Cluster) GetChannel(channelID string, channelType int64) (*wkdb.Channel, error)
```

### Core Write Path

```go
func (c *Cluster) proposeOrForward(groupID multiraft.GroupID, cmd []byte) error {
    leaderID, err := c.router.LeaderOf(groupID)
    if err != nil {
        return err
    }
    if c.router.IsLocal(leaderID) {
        future := c.runtime.Propose(groupID, cmd)
        result := future.Wait()
        return result.Err
    }
    _, err = c.forwarder.Forward(groupID, cmd)
    return err
}
```

### Read Path

Reads go directly to local wkdb via `ShardStore`, accepting eventual consistency. No raft involved.

### New wkfsm Command Required

DeleteChannel requires adding `cmdTypeDeleteChannel` to `wkfsm/command.go`.

## Dependencies

```
         wkcluster/
        /    |    \
  multiraft  wkfsm  wkdb
       |
   raftstore
```

No circular dependencies. wkcluster is the top-level assembly layer.
