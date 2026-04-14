# pkg/channel 流程文档

## 1. 职责定位

基于 ISR 复制状态机的分布式频道模型。负责消息的追加、获取、多副本复制、持久化存储和故障恢复。
**不负责**: 频道的 Slot 分配（由 controller 负责）、元数据的分布式存储（由 slot 负责）。

## 2. 子包分工

| 子包 | 入口/核心类型 | 职责 |
|------|-------------|------|
| `handler/` | `handler.New()` → `Service` | 请求校验、幂等去重、DurableMessage 编解码、消息序列号管理 |
| `replica/` | `replica.NewReplica()` → `Replica` | 副本状态机：Group Commit 追加、HW 推进、分歧检测、角色转换、恢复 |
| `runtime/` | `runtime.Build()` → `Runtime` | 频道生命周期管理、复制调度（三优先级）、多级背压、墓碑清理 |
| `store/` | `store.NewEngine()` → `Engine` | Pebble KV：日志、检查点、Epoch 历史、幂等表持久化 |
| `transport/` | `transport.Build()` | Fetch / ProgressAck RPC、PeerSession 批量合并（200µs/32个） |

## 3. 对外接口

```go
// channel.go — 对外唯一入口
type Cluster interface {
    ApplyMeta(meta Meta) error                                         // 控制面下发元数据变更
    Append(ctx context.Context, req AppendRequest) (AppendResult, error) // 追加消息
    Fetch(ctx context.Context, req FetchRequest) (FetchResult, error)    // 获取已提交消息
    Status(id ChannelID) (ChannelRuntimeStatus, error)                 // 查询频道状态
    Close() error
}

// 组装: channel.New(Config) → Cluster
// 构建顺序: Transport → Runtime → BindFetchService → Handler → cluster
```

## 4. 关键类型

| 类型 | 文件 | 说明 |
|------|------|------|
| `Meta` | types.go | 频道元数据：Key, Epoch, Leader, Replicas, ISR, MinISR, LeaseUntil, Status |
| `Message` | types.go | 消息：MessageID, MessageSeq, FromUID, ClientMsgNo, Payload |
| `ReplicaState` | types.go | 副本状态：Role(Leader/Follower/Fenced/Tombstoned), HW, LEO, Epoch |
| `AppendRequest` | types.go | 追加请求：ChannelID, Message, ExpectedChannelEpoch, ExpectedLeaderEpoch |
| `Checkpoint` | types.go | 检查点：Epoch + LogStartOffset + HW（恢复用） |

## 5. 核心流程

### 5.1 消息追加（Append）

入口: `channel.go:253` → `handler/append.go:13 service.Append`

```
Handler 层:
  ① 解码 ChannelKey → handler/key.go:KeyFromChannelID
  ② 加载 Meta，校验 Epoch → handler/meta.go:metaForKey
  ③ 幂等检查 (FromUID+ClientMsgNo+PayloadHash) → handler/append.go:loadMessageViewAtOffset
     命中且 Hash 匹配 → 返回旧结果；Hash 不匹配 → ErrIdempotencyConflict
  ④ 分配 MessageID → MessageIDGenerator.Next()
  ⑤ 编码 DurableMessage(45B头+变长字段+Payload) → handler/codec.go:encodeMessage

Replica 层 (replica/append.go):
  ⑥ Group Commit 收集 (1ms窗口/64条/64KB) → collectAppendBatch
  ⑦ 批量 fsync 到 LogStore → flushAppendBatch
  ⑧ 注册 Waiter (目标HW = LEO + recordCount)
  ⑨ 等待 HW ≥ 目标 → 返回 CommitResult
```

### 5.2 消息获取（Fetch）

入口: `handler/fetch.go:9 service.Fetch`

```
  ① 校验 Limit > 0, MaxBytes > 0
  ② 加载 Meta，检查频道状态非 Deleting/Deleted
  ③ 从 Runtime 获取 HandlerChannel → runtime.Channel(key)
  ④ 读取 LogRecord（仅 HW 以内已提交数据）→ handler/seq_read.go:LoadNextRangeMsgs
  ⑤ 解码 DurableMessage → Message → handler/codec.go:decodeMessageRecord
  ⑥ 返回 Messages + NextSeq + CommittedSeq
```

### 5.3 副本复制（Replication）

发起: `runtime/replicator.go` | 接收: `replica/replication.go:ApplyFetch`

```
Leader 侧:
  ① 新数据写入 → Runtime 检测需要复制 → 调度器排入 Follower 任务
  ② 构建 FetchRequest{ChannelKey, Epoch, FetchOffset, MaxBytes}
  ③ Transport 批量合并 (200µs窗口) → transport/session.go → 发送到 Follower

Follower 侧:
  ④ replica/fetch.go:Fetch → 从日志读取记录，检测分歧(Epoch History)
  ⑤ 返回 FetchResponse{Records, LeaderHW, TruncateTo}

Leader 处理响应:
  ⑥ replica/replication.go:ApplyFetch → 更新 Progress Map
  ⑦ replica/progress.go:advanceHW → 取 ISR 中第 MinISR 高的 MatchOffset
  ⑧ 通知 Waiter → 消息提交完成
```

### 5.4 角色转换

入口: `replica/replica.go`

```
BecomeLeader (replica.go:153):
  ① 验证 Meta.Leader 是本节点
  ② 截断 HW 后未提交日志 → history.go:truncateLogToLocked
  ③ 追加 EpochPoint → history.go:appendEpochPointLocked
  ④ 初始化 Progress: 本节点=LEO, 其他ISR=HW → progress.go:seedLeaderProgressLocked
  ⑤ 检查 Lease → 过期则 FencedLeader

BecomeFollower (replica.go:209):
  ① 应用新 Meta → 失败所有待处理 Append (failOutstandingAppendWorkLocked)

Tombstone (replica.go:230):
  ① 标记 Tombstoned → 拒绝所有后续操作
```

### 5.5 故障恢复

入口: `replica/recovery.go:11 recoverFromStores`

```
  ① 加载 Checkpoint{Epoch, LogStartOffset, HW} → store/checkpoint.go
  ② 加载 Epoch History → store/history.go
  ③ 校验一致性 (HW ≤ LEO, Epoch 匹配)
  ④ LEO > HW → 截断日志到 HW（丢弃未提交数据）
  ⑤ 标记 recovered = true
  → Runtime.EnsureChannel(meta) → ApplyMeta → BecomeLeader/BecomeFollower
```

## 6. 运行时架构要点

- **64 路分片**: Runtime 按 FNV(ChannelKey) 分 64 个 shard，每个 shard 独立 RWMutex → `runtime/runtime.go`
- **三优先级调度**: High(Leader变更) / Normal(周期复制) / Low(快照) → `runtime/scheduler.go`
- **三级背压**: 无背压(立即发送) / 软(批量合并) / 硬(排队+重试调度) → `runtime/backpressure.go`
- **墓碑管理**: 频道删除后加入墓碑(带TTL)，防止过期响应生效 → `runtime/tombstone.go`

## 7. 存储键空间

```
Log         (0x10): prefix + key + offset(8B)     — 日志记录
Checkpoint  (0x11): prefix + key                   — 检查点(24B: Epoch+LogStart+HW)
History     (0x12): prefix + key + offset(8B)      — Epoch 历史
Snapshot    (0x13): prefix + key                   — 快照
Idempotency (0x14): prefix + key + fromUID + msgNo — 幂等条目
```

详见 `store/keys.go`

## 8. 避坑清单

- **per-channel 互斥锁**: `channel.go:lockApplyMeta` 使用引用计数的 per-key 锁，保证同一频道的 ApplyMeta 串行执行。Handler → Runtime 失败时会 RestoreMeta 回滚。
- **幂等 PayloadHash 校验**: 仅 ClientMsgNo 相同不够，必须校验 FNV-64a PayloadHash 一致，否则返回 `ErrIdempotencyConflict`。见 `handler/append.go`。
- **Group Commit 窗口**: 默认 1ms/64条/64KB，配置在 `replica/replica.go:effectiveAppendGroupCommit*`。调大会增加延迟但提高吞吐。
- **HW 推进不可回退**: HW 只能前进不能后退。`progress.go:advanceHW` 会检查 newHW ≥ currentHW。
- **Lease 过期自动降级**: Leader Lease 过期后自动变为 FencedLeader，拒绝所有写入但不影响读取。见 `replica/append.go:appendableLocked`。
- **ApplyFetch Coordinator**: `store/commit.go` 使用 200µs 窗口跨频道合并 Pebble 写入，减少写放大。单频道的 writeMu 仍然串行。
- **Transport RPC 分片**: FetchRequest 按 FNV-64a(ChannelKey) 路由到固定 RPC 流，保证同一频道有序。见 `transport/session.go`。
