# wkcluster 存储分离设计

## 目标

将 `pkg/wkcluster/` 重构为纯粹的集群协调库,分布式业务存储拆分为独立的 `pkg/wkstore/` 包。开发者修改存储相关内容只需关注 `wkstore`,而 `wkcluster` 不再包含任何业务存储逻辑。

## 现状问题

`wkcluster` 目前混合了两类职责:

1. **集群协调**: leader 选举、路由、转发、transport 管理、节点发现
2. **业务存储**: `CreateChannel`/`UpdateChannel` 等 API、直接创建 `wkdb.DB`/`raftstore.DB`/`wkfsm.StateMachine`

这导致 `wkcluster` 的 import 图包含 `wkdb`、`raftstore`、`wkfsm`,开发者改业务存储逻辑需要进入协调层代码。

## 设计方案: 彻底解耦 + wkfsm 合并

### 拆分后依赖关系

```
上层应用 (组装层)
├── wkstore (业务分布式存储)
│   ├── wkcluster (纯集群协调)
│   │   ├── multiraft (raft 引擎)
│   │   └── wktransport (网络传输)
│   ├── wkdb (应用数据 KV)
│   └── raftstore (raft 日志 KV)
├── wkdb
└── raftstore
```

**关键改进**: `wkcluster` 的 import 中不再出现 `wkdb`、`raftstore`、`wkfsm`。

### 包职责

#### `pkg/wkcluster/` — 纯集群协调库

职责:
- 管理 `multiraft.Runtime` 生命周期
- 通用路由: `SlotForKey(key string) GroupID`(不绑定 channel 语义)
- Proposal 提交与 leader 转发: `Propose(ctx, groupID, cmd []byte) error`
- Transport 管理 (server, pool, client)
- 节点发现 (discovery)

不再依赖: `wkdb`, `raftstore`, `wkfsm`

#### `pkg/wkstore/` — 业务分布式存储 (新建)

职责:
- 命令编码/解码 (从 `wkfsm` 移入)
- `BatchStateMachine` 实现 (从 `wkfsm` 移入)
- 业务 API: `CreateChannel`, `UpdateChannel`, `DeleteChannel`, `GetChannel`
- 持有 `wkdb.DB` 引用

#### `pkg/wkfsm/` — 删除

整个包合并到 `wkstore`。

#### 不变的包

- `pkg/multiraft/` — 纯 raft 引擎
- `pkg/wktransport/` — 纯传输层
- `pkg/wkdb/` — 纯应用数据存储
- `pkg/raftstore/` — 纯 raft 日志存储

---

## 详细设计

### 1. wkcluster 新的 Config

```go
type Config struct {
    // 节点身份
    NodeID     multiraft.NodeID
    ListenAddr string

    // 集群拓扑
    GroupCount uint32
    Nodes      []NodeConfig
    Groups     []GroupConfig

    // 存储工厂 (外部注入)
    NewStorage      func(groupID multiraft.GroupID) multiraft.Storage
    NewStateMachine func(groupID multiraft.GroupID) multiraft.BatchStateMachine

    // 网络/Raft 调优
    ForwardTimeout time.Duration
    PoolSize       int
    TickInterval   time.Duration
    RaftWorkers    int
    ElectionTick   int
    HeartbeatTick  int
    DialTimeout    time.Duration
}
```

移除: `DataDir`, `RaftDataDir`
新增: `NewStorage`, `NewStateMachine` 工厂函数

### 2. wkcluster 新的公开 API

```go
// Propose 向指定 group 提交命令,自动处理 leader 转发
func (c *Cluster) Propose(ctx context.Context, groupID multiraft.GroupID, cmd []byte) error

// SlotForKey 通用路由: key -> groupID (CRC32 hash)
func (c *Cluster) SlotForKey(key string) multiraft.GroupID

// LeaderOf 查询 group 的当前 leader
func (c *Cluster) LeaderOf(groupID multiraft.GroupID) (multiraft.NodeID, error)

// IsLocal 判断节点是否是本节点
func (c *Cluster) IsLocal(nodeID multiraft.NodeID) bool
```

移除: `CreateChannel`, `UpdateChannel`, `DeleteChannel`, `GetChannel`

### 3. wkcluster Cluster struct 变更

```go
type Cluster struct {
    cfg        Config
    server     *wktransport.Server
    raftPool   *wktransport.Pool
    raftClient *wktransport.Client
    fwdClient  *wktransport.Client
    runtime    *multiraft.Runtime
    router     *Router
    discovery  *StaticDiscovery
    stopped    atomic.Bool
}
```

移除: `db *wkdb.DB`, `raftDB *raftstore.DB`

### 4. wkcluster Start() 变更

```go
func (c *Cluster) Start() error {
    // 1. 创建 discovery
    // 2. 创建 transport server, pool, clients
    // 3. 创建 multiraft.Runtime (使用注入的 NewStorage/NewStateMachine)
    // 4. 创建 router
    // 5. Open/Bootstrap groups
    // 不再: 打开 wkdb.DB, 打开 raftstore.DB
}
```

### 5. wkcluster Router 变更

```go
// SlotForKey 替代原来的 SlotForChannel,不绑定 channel 语义
func (r *Router) SlotForKey(key string) multiraft.GroupID {
    h := crc32.ChecksumIEEE([]byte(key))
    return multiraft.GroupID(h%r.groupCount) + 1
}
```

### 6. wkstore 包结构

```
pkg/wkstore/
├── store.go          // Store 主结构体 + 业务 API
├── command.go        // 命令编码/解码 (从 wkfsm 移入)
├── statemachine.go   // BatchStateMachine 实现 (从 wkfsm 移入)
└── store_test.go
```

### 7. wkstore Store 结构体

```go
type Store struct {
    cluster *wkcluster.Cluster
    db      *wkdb.DB
}

func New(cluster *wkcluster.Cluster, db *wkdb.DB) *Store {
    return &Store{cluster: cluster, db: db}
}
```

### 8. wkstore 业务 API

```go
func (s *Store) CreateChannel(ctx context.Context, channelID string, channelType uint8) error {
    groupID := s.cluster.SlotForKey(channelID)
    cmd := encodeUpsertChannelCommand(channelID, channelType, false)
    return s.cluster.Propose(ctx, groupID, cmd)
}

func (s *Store) UpdateChannel(ctx context.Context, channelID string, channelType uint8, ban bool) error {
    groupID := s.cluster.SlotForKey(channelID)
    cmd := encodeUpsertChannelCommand(channelID, channelType, ban)
    return s.cluster.Propose(ctx, groupID, cmd)
}

func (s *Store) DeleteChannel(ctx context.Context, channelID string, channelType uint8) error {
    groupID := s.cluster.SlotForKey(channelID)
    cmd := encodeDeleteChannelCommand(channelID, channelType)
    return s.cluster.Propose(ctx, groupID, cmd)
}

func (s *Store) GetChannel(ctx context.Context, channelID string, channelType uint8) (*wkdb.Channel, error) {
    slot := s.cluster.SlotForKey(channelID)
    return s.db.ForSlot(uint64(slot)).GetChannel(channelID, channelType)
}
```

### 9. wkstore StateMachine 工厂

```go
func NewStateMachineFactory(db *wkdb.DB) func(groupID multiraft.GroupID) multiraft.BatchStateMachine {
    return func(groupID multiraft.GroupID) multiraft.BatchStateMachine {
        return &stateMachine{db: db, groupID: uint64(groupID)}
    }
}
```

### 10. 上层组装示例

```go
// 打开存储
db := wkdb.Open(dataDir)
raftDB := raftstore.Open(raftDataDir)

// 创建集群 (纯协调,不知道业务)
cluster := wkcluster.New(wkcluster.Config{
    NodeID:          nodeID,
    ListenAddr:      ":5000",
    GroupCount:       16,
    Nodes:           nodes,
    Groups:          groups,
    NewStorage:      raftstore.NewStorageFactory(raftDB),
    NewStateMachine: wkstore.NewStateMachineFactory(db),
})

// 创建业务存储
store := wkstore.New(cluster, db)

// 启动
cluster.Start()
defer cluster.Stop()

// 使用
store.CreateChannel(ctx, "channel1", 1)
ch, _ := store.GetChannel(ctx, "channel1", 1)
```

---

## 文件变更清单

| 操作 | 文件 | 说明 |
|------|------|------|
| 修改 | `pkg/wkcluster/cluster.go` | 移除 `db`/`raftDB` 字段,Start() 改为使用注入的工厂函数 |
| 修改 | `pkg/wkcluster/config.go` | 移除 DataDir/RaftDataDir,新增 NewStorage/NewStateMachine |
| 修改 | `pkg/wkcluster/router.go` | `SlotForChannel` 改名为 `SlotForKey` |
| 删除 | `pkg/wkcluster/api.go` | 业务 API 移到 wkstore |
| 修改 | `pkg/wkcluster/errors.go` | 移除业务相关错误(如果有) |
| 新建 | `pkg/wkstore/store.go` | Store 主结构体 + 业务 API |
| 新建 | `pkg/wkstore/command.go` | 命令编码/解码 (从 wkfsm 移入) |
| 新建 | `pkg/wkstore/statemachine.go` | StateMachine 实现 (从 wkfsm 移入) |
| 新建 | `pkg/wkstore/store_test.go` | 测试 |
| 删除 | `pkg/wkfsm/` | 整个包合并到 wkstore |
| 修改 | 测试文件 | 更新 import 和初始化方式 |

## 验收标准

1. `pkg/wkcluster/` 的 import 中不出现 `wkdb`、`raftstore`、`wkfsm`
2. `wkcluster` 无 `CreateChannel` 等业务 API
3. `wkstore` 通过 `wkcluster.Propose` 提交命令,业务逻辑自包含
4. 所有现有测试迁移后通过
5. `pkg/wkfsm/` 目录已删除
