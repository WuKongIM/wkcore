# Channel ISR Cluster Design

## 目标

为 `WuKongIM` 设计一套面向 `channel` 的分布式消息集群方案,参考 Kafka 的 `leader + follower + ISR` 复制模型,但保持与当前仓库架构一致:

- `slot/controller` 集群继续承担控制面一致性
- `channel` 数据面不使用 Raft 复制消息日志
- 每个 `channel` 作为一个独立的 ISR 日志复制组
- 频道内 `messageSeq` 严格递增

本设计选择 **A 方案**:

> `一个 channel = 一个独立的 ISR 复制日志组`

并固定核心语义:

> `messageSeq = committed logOffset + 1`

其中:

- `logOffset` 从 `0` 开始
- 第一条已提交消息的 `messageSeq = 1`
- 只有 committed 的消息才允许对外可见

## 背景与约束

当前项目已经具备:

- 基于 `slot` 的 controller/control-plane 集群
- `wkcluster` 提供的控制面分片与 leader 路由
- `wkdb` 提供的 slot-scoped 元数据存储

但当前 `channel` 还停留在元数据 CRUD 层,没有真正进入类似 Kafka `topic-partition` 的数据复制与提交模型。

本设计需要满足以下约束:

1. `slot/controller` 集群不进入每条消息的热路径
2. `channel` 频道内需要严格顺序的 `messageSeq`
3. 旧 leader、网络分区、节点重启不能导致 `messageSeq` 回退或重号
4. 第一版优先保证 correctness,不追求热点 channel 的横向写扩展
5. 包边界应与当前仓库分层兼容,不把控制面和数据面揉成一个“大 service”

## 为什么选择 A 方案

曾考虑三个方向:

- `A`: 一个 `channel` = 一个复制日志组
- `B`: 一个 `channel` = 一个逻辑 topic,内部多 partition
- `C`: 对外是一个 channel,内部复制单元是 shard,默认单 shard

最终选择 `A`,原因如下:

1. **最容易保证频道内强顺序**
   同一 `channel` 只有一个 leader 和一条提交序列,`messageSeq` 可以直接建立在 committed log offset 之上。

2. **与 IM 语义最一致**
   多数 IM 场景天然假设“频道内消息严格有序”或“至少由系统给出单调递增的频道序号”。A 方案最贴近这一需求。

3. **第一版 correctness 成本最低**
   第一版最难的不是扩展性,而是故障切换、旧 leader fencing、提交语义、幂等重试这些 correctness 细节。A 方案把问题域压到了最小。

4. **与当前仓库演进方向兼容**
   现有 `slot/controller` 集群可以直接作为 `channel metadata quorum`。缺的是独立的数据面 runtime,而不是重新设计一套多 partition channel 模型。

代价也必须明确:

- 每个 `channel` 一个复制日志组,未来组数量会非常大
- A 方案不解决单个超热点 `channel` 的横向写扩展
- 长期如需热点频道拆分,应在 correctness 稳定后再向 `C` 演进

本设计接受这一代价,以换取第一版语义的确定性。

## 核心不变量

以下不变量必须在设计和实现中被严格保持:

1. 同一时刻只有当前 `channel leader` 能接收该频道的写入
2. follower 只复制日志,不分配 `messageSeq`
3. `messageSeq = committed logOffset + 1`
4. 对外可见的消息必须满足 `offset <= HW`
5. 新 leader 只能从 ISR 中选举
6. controller 不可用且 leader lease 过期后,必须停止新写
7. 允许截断的只有 `HW` 之后的未提交尾部
8. `channel data log` 第一版只存用户消息记录,不混入内部控制记录
9. 幂等去重必须独立建模,不能把“顺序”误当成“去重”

## 总体架构

系统拆成两层:

```text
                     +-----------------------------+
                     |   slot/controller cluster   |
                     |  (control-plane metadata)   |
                     +-----------------------------+
                                  |
             manage channel meta / leader / ISR / reassignment
                                  |
        +-----------------------------------------------------+
        |                 pkg/channelcluster                  |
        |         channel data-plane ISR runtime             |
        |  leader append / follower fetch / HW / snapshot    |
        +-----------------------------------------------------+
                                  |
                      serve message write and read
```

角色边界:

- `slot/controller cluster`
  只负责 `channel metadata` 的一致复制和裁决:
  - 创建/删除 channel
  - 副本分配
  - leader 选举
  - ISR 变更
  - reassignment
  - broker/node fence 与 drain

- `pkg/channelcluster`
  负责 `channel` 消息日志的数据复制:
  - leader append
  - follower fetch
  - LEO/HW 推进
  - lease 自我 fencing
  - 日志截断
  - snapshot / restore

controller 不参与每条消息的写入仲裁,不发放 `messageSeq`。

## messageSeq 与日志语义

### 定义

对单个 `channel`:

- `LEO`: log end offset,即下一条将写入的 offset
- `HW`: high watermark,当前已提交的最大 offset
- `messageSeq`: 用户可见的频道内消息序号

固定定义:

```text
messageSeq = committed logOffset + 1
```

例如:

| logOffset | 提交状态 | 对外 messageSeq |
|-----------|----------|-----------------|
| 0         | committed | 1 |
| 1         | committed | 2 |
| 2         | uncommitted | 不可见 |

### 为什么不直接让网关或 controller 发号

这样会破坏正确性:

- 网关本地发号: 多节点并发写必然冲突
- controller 发号: 控制面进入每条消息热路径,性能和复杂度都不可接受
- follower 发号: 会破坏 leader 单写模型

因此唯一安全的方式是:

> 由当前 leader 在单串行 append 临界区内确定 offset,再由 committed offset 派生 messageSeq

### 成功语义

客户端拿到成功响应时,应同时满足:

- 该消息已经被 leader 写入本地日志
- 该消息已经被足够多 ISR 副本复制
- `HW` 已推进覆盖该消息 offset
- 返回的 `messageSeq` 对应 committed offset

也就是等价于 Kafka 的:

```text
acks=all + minISR
```

## 控制面设计

### channel 元数据模型

建议定义:

```go
type ChannelMeta struct {
    ChannelID   string
    ChannelType uint8

    ChannelEpoch uint64
    LeaderEpoch  uint64

    Replicas []NodeID
    ISR      []NodeID
    Leader   NodeID

    MinISR            int
    ReplicationFactor int

    Status        ChannelStatus
    Reassignment  *ChannelReassignment
    CreateTime    int64
    UpdateTime    int64
}
```

字段含义:

- `ChannelEpoch`
  channel 结构性元数据版本。副本集、删除状态、迁移计划等变化都应递增。
- `LeaderEpoch`
  leader 身份版本。每次切主都应递增。
- `Replicas`
  当前目标副本集。
- `ISR`
  当前同步副本集。
- `Leader`
  当前 leader。

### slot 与 channel 的关系

设计固定为:

- `slot` 负责承载 `channel metadata`
- `channel` 数据复制组独立于 `slot`

即:

```text
channelKey -> slot(controller owner) -> ChannelMeta
ChannelMeta -> Leader / Replicas / ISR -> data-plane routing
```

不要让 `slot` 同时成为 metadata shard 和 data shard,否则控制面和数据面会被过度耦合。

### channel 生命周期

建议状态机:

```text
Creating -> Active -> Reassigning -> Active
Creating -> Active -> Deleting -> Deleted
```

要求:

- `Creating`
  metadata 已存在,但 leader 尚未 ready,不允许写入
- `Active`
  正常读写
- `Reassigning`
  迁移进行中,写入仍可继续
- `Deleting`
  停止新写,进入回收流程
- `Deleted`
  tombstone 状态,可延迟物理清理

### leader 选举

controller 负责裁决 leader,并保持以下规则:

1. 只从 `ISR` 中选 leader
2. `ISR` 为空时,`Leader=0`,channel 不可写
3. 每次切主必须:
   - `LeaderEpoch++`
   - 更新 `Leader`
   - 下发 `StepDown/BecomeLeader`
4. 不允许 unclean leader election

这是保证 `messageSeq` 不回退和“已成功返回的消息不丢”的核心安全条件。

### ISR 维护

ISR 不能由 leader 私下维护,应采用:

- 数据面负责上报事实:
  - follower `LEO`
  - fetch 延迟
  - leader 观察到的复制进度
- controller 负责裁决:
  - 剔除落后或失联副本
  - 将追平并稳定的新副本加入 ISR

也就是:

> 观测来自数据面,裁决来自控制面

## 数据面运行时设计

数据面包名固定为:

```text
pkg/channelcluster
```

原因:

- 它是一个明确的数据面基础设施层,不是单个 app 内部细节
- 未来会被 `internal/app`、`internal/access/gateway`、可能的调试工具和压测程序共同依赖
- 它不属于 `controller`,因此不适合放到 `pkg/controller/*`

### 单副本本地状态

每个承载该频道副本的节点维护一个 `ChannelReplica`:

```go
type ChannelReplica struct {
    Key         ChannelKey
    Role        ReplicaRole
    Leader      NodeID
    LeaderEpoch uint64

    LocalLEO       uint64
    HW             uint64
    LogStartOffset uint64

    Replicas []NodeID
    ISR      []NodeID

    LeaseDeadline time.Time
}
```

其中 leader 还需要额外维护:

```go
type FollowerProgress struct {
    MatchOffset  uint64
    LastFetchAt  time.Time
    LastAckAt    time.Time
}
```

不变量:

```text
LogStartOffset <= HW < LocalLEO
```

### 写入流程

写入必须全部经过 leader 的单串行 append 点:

1. 接入节点路由到当前 `channel leader`
2. leader 校验:
   - 本地仍是 leader
   - `LeaderEpoch` 最新
   - lease 未过期
   - `ISR >= MinISR`
   - 幂等键未命中
3. 在 append 临界区内:
   - `offset = LocalLEO`
   - `messageSeq = offset + 1`
4. leader 把记录写入本地日志,`LocalLEO++`
5. followers 通过 fetch 协议复制
6. 当足够多 ISR 副本复制到该 offset 后,leader 推进 `HW`
7. 仅此时返回成功和 `messageSeq`

关键点:

- `messageSeq` 可以在 leader append 时确定
- 但只有 committed 后才允许对外生效

### follower fetch 协议

建议采用 Kafka 风格 follower 主动拉取:

```text
Fetch(channel, leaderEpoch, fetchOffset=LocalLEO, maxBytes)
```

leader 返回从 `fetchOffset` 开始的连续批次。

优点:

- follower 自带背压
- 节点重启后能从本地 `LEO` 继续追赶
- leader 不需要维护复杂推送状态机

### HW 推进

leader 维护 ISR 内每个副本的 `MatchOffset`。

建议规则:

```text
HW = ISR 中第 MinISR 大的 MatchOffset
```

当 `MinISR == len(ISR)` 时,等价于 ISR 的最小 `MatchOffset`。

这个定义决定了:

- 什么时候返回成功
- 什么时候允许读取
- 故障切换后哪些消息绝不允许丢失

### 读路径

默认只允许读取 `offset <= HW` 的记录:

- 从 `messageSeq = N` 开始读,转换为 `offset = N - 1`
- leader 默认只返回 `HW` 以内的消息
- follower 只有在将来明确支持 stale read 时才可读,且也不得越过本地已知 `HW`

## leader lease 与 fencing

由于数据面不是 Raft,只靠“controller 曾经任命你为 leader”不够安全。

因此引入:

> `Leader = controller 授权的 leaderEpoch + 一个短周期 lease`

规则:

1. controller 选出 leader 后下发:
   - `Leader`
   - `LeaderEpoch`
   - `LeaseDeadline`
2. leader 必须周期性续租
3. 本地一旦超过 `LeaseDeadline` 仍未续租成功,立刻进入 `FencedLeader`
4. `FencedLeader` 必须停止接收新写
5. follower 永远不能自发升主,只能等待 controller 下发新元数据

没有 lease,follower + controller 的最终一致性不足以阻止旧 leader 在网络分区时继续接写。

## 故障切换与日志收敛

### leader 切换流程

1. controller 发现 leader 失联、drain、被 fence,或 lease 续约失败
2. 从 `ISR` 中选新 leader
3. `LeaderEpoch++`
4. 写入新的 `ChannelMeta`
5. 向新 leader 下发 `BecomeLeader(epoch)`
6. 向旧 leader 下发 `StepDown(epoch)` 或依赖其 lease 超时自我 fence

### 写请求 fencing

所有写请求应携带:

- `ChannelEpoch`
- `LeaderEpoch`

leader 校验:

- 请求 epoch 过旧 -> `ErrNotLeaderEpoch`
- 本地 lease 已过期 -> `ErrLeaderLeaseExpired`
- 本地非 leader -> `ErrNotLeader`
- `ISR < MinISR` -> `ErrInsufficientISR`

### 日志分叉与截断

旧 leader 或落后 follower 重新加入时,可能在 `HW` 之后保有未提交脏尾。

因此日志记录必须附带 `LeaderEpoch`,并维护 `epoch -> startOffset` 映射。

恢复流程:

1. follower 发 fetch,带上本地 `fetchOffset` 和已知 epoch
2. leader 检测是否发生分叉
3. 若分叉,先返回 `truncateTo(offset)`
4. follower 截断本地 `HW` 之后的脏尾
5. 再继续 fetch 追赶

原则:

- `HW` 以内数据绝不回退
- 只允许截断未提交尾部

### controller 不可用时

行为必须偏保守:

- 现任 leader 在 lease 未过期前可继续服务
- lease 过期后必须停止新写
- controller quorum 不可用时不允许产生新 leader
- 读可继续读取本地 `HW`

这会牺牲一部分写可用性,但可避免双主和 seq 回退。

## 幂等与重复提交

`messageSeq` 严格递增并不自动等于“客户端重试无重复”。

必须单独维护幂等索引,建议键为:

```text
(channelID, senderUID, clientMsgNo)
```

规则:

1. 首次写入:
   - 写入消息记录
   - 写入幂等索引
   - 返回新的 `messageSeq`
2. 重试命中:
   - 不再追加新记录
   - 返回第一次已分配的 `messageSeq`

幂等检查应进入 leader append 临界区,避免并发下双写。

## 重分配与迁移

### 设计边界

在 A 方案里,“扩容”不是拆分 channel,而是:

- 增减副本数
- leader 迁移
- 副本迁移到新节点

不包含:

- 把单个 channel 拆成多 partition
- 横向扩展单个热点 channel 的并发写入

### 元数据模型

建议显式保留迁移过程字段:

```go
type ChannelAssignment struct {
    Replicas         []NodeID
    ISR              []NodeID
    Leader           NodeID

    AddingReplicas   []NodeID
    RemovingReplicas []NodeID

    AssignmentEpoch  uint64
    LeaderEpoch      uint64
}
```

这样 controller 能表达:

- 谁是最终目标副本
- 谁正在加入
- 谁正在移除

而不是原地覆盖一份 `Replicas` 导致迁移中间状态不可恢复。

### 新副本加入流程

1. controller 发起 reassignment
2. 更新 metadata:
   - `Replicas = old + new`
   - `AddingReplicas = new`
   - `AssignmentEpoch++`
3. 新副本创建空本地 replica
4. 新副本:
   - 先装快照(如果有)
   - 再从快照尾或 `LogStartOffset` 开始 fetch 追赶
5. 追到当前 `HW` 且稳定一段窗口
6. controller 将其加入 ISR

原则:

> 先追平,再进 ISR; 先进 ISR,再移旧副本

### 旧副本移除流程

1. 若当前 leader 位于待移除副本中,先切 leader
2. 更新 metadata:
   - `RemovingReplicas = old`
   - `Replicas = new`
   - `AssignmentEpoch++`
3. 从 ISR 中剔除旧副本
4. 下发本地回收命令
5. 旧副本进入 tombstone 状态并延迟删除本地日志

### leader 迁移

leader 迁移应独立建模,不要作为 reassignment 的隐式副作用:

```go
type PreferredLeaderChange struct {
    ChannelID   string
    FromLeader  NodeID
    ToLeader    NodeID
    LeaderEpoch uint64
    Reason      string
}
```

适用于:

- 把 leader 从高负载节点迁出
- 在迁移中将 leader 切到新副本集
- 调整 leader 与热点用户的拓扑接近性

## 快照

若 channel 历史较长,仅靠从 offset 0 回放追赶代价过高。

建议第一版就预留:

```go
type ReplicaSnapshot struct {
    ChannelID      string
    ChannelType    uint8
    SnapshotOffset uint64
    LeaderEpoch    uint64
    Data           []byte
}
```

使用方式:

1. 新副本先加载 snapshot
2. 本地恢复到 `SnapshotOffset`
3. 再从 `SnapshotOffset + 1` 开始 fetch 增量日志

注意:

- 这是 `channel data snapshot`
- 与 controller metadata snapshot 分离

## 删除 channel

删除必须是两阶段:

### 阶段一: 逻辑删除

1. controller 将 `Status` 置为 `Deleting`
2. `ChannelEpoch++`
3. 停止接受新写
4. 读可按策略:
   - 立即禁止
   - 或短时间只读

### 阶段二: 物理回收

1. controller 向所有副本下发 `DeleteReplica(channel, epoch)`
2. 各副本停止服务并标记 tombstone
3. controller 收齐确认后将 metadata 置为 `Deleted`
4. 后台异步清理本地日志

不能只删 metadata,否则旧 leader 或残留副本可能继续对外提供服务。

## 包结构建议

### 控制面

```text
internal/usecase/channelmeta/
  app.go
  command.go
  result.go
  create.go
  delete.go
  leader.go
  isr.go
  reassignment.go
  deps.go
```

职责:

- channel metadata 的控制面用例
- create/delete/elect leader/update ISR/reassignment 等命令编排

### 数据面

```text
pkg/channelcluster/
  types.go
  manager.go
  replica.go
  leader.go
  follower.go
  fetch.go
  append.go
  hw.go
  lease.go
  epoch.go
  log.go
  snapshot.go
  transport.go
  errors.go
```

职责:

- `ChannelReplica` 运行时
- leader/follower 状态管理
- append / fetch / ack / HW 推进
- lease fencing
- truncate / snapshot / catch-up

### 组装层

```text
internal/app/
```

职责:

- 组装 `slot/controller` 控制面与 `pkg/channelcluster` 数据面
- 保持唯一组合根

## 接口边界建议

### 控制面接口

```go
type ChannelMetaStore interface {
    CreateChannel(ctx context.Context, meta ChannelMeta) error
    GetChannel(ctx context.Context, channelID string, channelType uint8) (ChannelMeta, error)
    UpdateChannel(ctx context.Context, meta ChannelMeta) error
    DeleteChannel(ctx context.Context, channelID string, channelType uint8) error
}
```

```go
type ChannelController interface {
    Create(ctx context.Context, req CreateChannelRequest) (ChannelMeta, error)
    ElectLeader(ctx context.Context, key ChannelKey, reason string) (ChannelMeta, error)
    UpdateISR(ctx context.Context, req UpdateISRRequest) (ChannelMeta, error)
    BeginReassignment(ctx context.Context, req BeginReassignmentRequest) (ChannelMeta, error)
    CompleteReassignment(ctx context.Context, req CompleteReassignmentRequest) (ChannelMeta, error)
}
```

### 数据面接口

```go
type ReplicaManager interface {
    EnsureReplica(meta ChannelMeta) error
    RemoveReplica(key ChannelKey, epoch uint64) error
    HandleBecomeLeader(cmd BecomeLeaderCommand) error
    HandleBecomeFollower(cmd BecomeFollowerCommand) error
    Append(ctx context.Context, req AppendRequest) (AppendResult, error)
    Fetch(ctx context.Context, req FetchRequest) (FetchResult, error)
}
```

```go
type ReplicaLog interface {
    Append(records []LogRecord) (baseOffset uint64, err error)
    Read(from uint64, maxBytes int) ([]LogRecord, error)
    Truncate(offset uint64) error
    Snapshot() (ReplicaSnapshot, error)
    Restore(ReplicaSnapshot) error
}
```

## 第一阶段实现范围

### 必做

- channel metadata 模型与控制面命令
- 单 channel leader/follower ISR 复制
- `LEO/HW/messageSeq` 正确语义
- `acks=all + minISR`
- `leaderEpoch + lease fencing`
- clean leader election
- follower fetch
- 基础 reassignment 骨架
- 基础 snapshot + catch-up
- 幂等去重

### 明确不做

- rack-aware replica placement
- follower stale read
- tiered storage
- 自动热点迁移
- 多 partition channel
- 复杂消费组模型

第一阶段目标应写成:

> 做出一个 correctness 可证明的单 channel ISR 日志复制系统

## 测试策略

至少覆盖以下测试:

1. `controller metadata` 一致性测试
   - leaderEpoch 递增
   - ISR 变更
   - reassignment 状态转换

2. `single channel replication` 集成测试
   - append
   - fetch
   - HW 推进
   - `messageSeq` 连续

3. `failover` 集成测试
   - leader 崩溃
   - 新 leader 续号
   - 无回退无重号

4. `stale leader fencing` 测试
   - lease 过期后旧 leader 不能继续写

5. `idempotent retry` 测试
   - 超时重试返回同一 `messageSeq`

6. `reassignment` 测试
   - 不停写迁移
   - 迁移前后 seq 连续

7. `truncate on divergence` 测试
   - 旧 leader 复活后只能截断未提交脏尾

这类测试优先级高于性能调优,因为最容易出错的部分是切主与迁移的正确性。

## 风险与后续演进

### 已知风险

1. 每个 channel 一个复制日志组,组数量会快速增长
2. runtime 数量、fetch session 数量、文件句柄和内存索引都会成为容量瓶颈
3. controller metadata 规模也会明显上升

### 后续优化方向

在不改变 A 方案语义前提下,后续可以探索:

- 冷 channel 的共享物理日志容器或轻量化 replica 载体
- log segment 合并与低频副本调度
- 更高效的 snapshot 和 catch-up
- leader placement 优化

只有当 correctness 稳定且容量问题被验证为主要矛盾时,才应考虑演进到更复杂的 shard/partition 设计。

## 总结

本设计将 `channel` 集群明确拆成:

- `slot/controller` 控制面
- `pkg/channelcluster` ISR 数据面

并用以下两条定义固定整个系统的正确性:

1. `一个 channel = 一个独立的 ISR 复制日志组`
2. `messageSeq = committed logOffset + 1`

在这两条约束下:

- 频道内顺序语义清晰
- 故障切换后的续号逻辑简单且可验证
- controller 不进入消息热路径
- 与现有仓库的分层方向兼容

这是一个明显偏 correctness-first 的设计。它不是最终扩展性的终点,但它是一个足够严谨、足够优雅、且最适合作为第一版落地的起点。
