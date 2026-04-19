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
| `transport/` | `transport.Build()` | Fetch / ProgressAck / ReconcileProbe RPC、PeerSession 批量合并（200µs 定时刷批，8 条或 32 KiB 立即刷批，失败后按 4 条分块回退） |

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
| `ReplicaState` | types.go | 副本状态：Role、运行时 CommitHW(`HW`)、持久化 CheckpointHW、`CommitReady`、LEO、Epoch |
| `AppendRequest` | types.go | 追加请求：ChannelID, Message, ExpectedChannelEpoch, ExpectedLeaderEpoch |
| `Checkpoint` | types.go | 检查点：Epoch + LogStartOffset + HW；用于冷恢复下界与 reconcile 后的持久化提交水位 |

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
  ⑥ 先检查 leader 是否 `CommitReady=true`；若刚启动/刚完成 leader transfer 但尚未完成 reconcile，则直接返回 ErrNotReady
  ⑥ Group Commit 收集 (1ms窗口/64条/64KB) → collectAppendBatch
  ⑦ Leader LogStore 在单 channel `writeMu` 下先 prepare 记录，再把 synced append 提交给 `store/commit.go`
     的跨频道 coordinator，用 200µs 窗口合并 Pebble Sync；sync 完成前不发布新的 LEO
  ⑧ sync 成功后执行 publish：更新 durable commit 计数、发布 LEO，并注册 Waiter (目标 CommitHW = LEO + recordCount)
  ⑨ `progress.go:advanceHW` 取 ISR 中第 MinISR 高的 MatchOffset，满足 quorum 后先推进运行时 `HW(CommitHW)`、完成 Waiter / sendack
  ⑩ Checkpoint 持久化由后台 publisher 异步 coalescing；写盘成功后才推进 `CheckpointHW`
```

### 5.2 消息获取（Fetch）

入口: `handler/fetch.go:9 service.Fetch`

```
  ① 校验 Limit > 0, MaxBytes > 0
  ② 加载 Meta，检查频道状态非 Deleting/Deleted
  ③ 从 Runtime 获取 HandlerChannel → runtime.Channel(key)
  ④ 若 `CommitReady=false`，直接返回 ErrNotReady，不再从 Checkpoint 猜 committed frontier
  ⑤ handler/fetch.go 直接按运行时 CommitHW 裁剪 store.Read 结果，不再通过 `LoadCheckpoint()` 推导 committed frontier
  ⑥ 解码 DurableMessage → Message → handler/codec.go:decodeMessageRecord
  ⑦ 返回 Messages + NextSeq + CommittedSeq
```

### 5.3 副本复制（Replication）

发起: `runtime/replicator.go` | 接收: `replica/replication.go:ApplyFetch`

```
正常 Fetch 复制:
  ① 新数据写入 → Runtime 检测需要复制 → 调度器排入 Follower 任务
  ② 构建 FetchRequest{ChannelKey, Epoch, FetchOffset, MaxBytes}
  ③ Transport 对 Fetch 做 200µs 定时合并；队列达到 8 条或 32 KiB 时立即刷批，批量失败/缺项时先按 4 条分块重试，再退化到单条 RPC → transport/session.go → 发送到 Follower

Follower 侧:
  ④ replica/fetch.go:Fetch → 从日志读取记录，检测分歧(Epoch History)
  ⑤ 返回 FetchResponse{Records, LeaderHW, TruncateTo}；其中 `LeaderHW` 使用 `visibleCommittedHW`，
     `CommitReady=false` 时只暴露保守 frontier（通常不超过 `CheckpointHW`）

Leader 处理响应:
  ⑥ replica/replication.go:ApplyFetch → 更新 Progress Map
  ⑦ replica/progress.go:advanceHW → 取 ISR 中第 MinISR 高的 MatchOffset
  ⑧ 推进运行时 CommitHW，通知 Waiter → 消息提交完成
```

```
Reconcile Probe（启动 / leader transfer 后的 provisional 收敛）:
  ① leader promotion 或 recover 后若 `CommitReady=false`，runtime 触发 peer probe
  ② transport/session.go 发送 ReconcileProbe RPC，收集 follower 的 {OffsetEpoch, LogEndOffset, CheckpointHW}
  ③ replica/reconcile.go 基于 epoch lineage + quorum proofs 计算 quorum-safe prefix
  ④ 保留 quorum-safe tail，必要时截断 minority-only suffix
  ⑤ 持久化新的 Checkpoint，推进 `CheckpointHW`，最后把 `CommitReady` 置为 true
```

### 5.4 角色转换

入口: `replica/replica.go`

```
BecomeLeader (replica.go:153):
  ① 验证 Meta.Leader 是本节点
  ② 追加 EpochPoint → history.go:appendEpochPointLocked
  ③ 初始化 Progress: 本节点=LEO, 其他ISR=当前安全 committed frontier → progress.go:seedLeaderProgressLocked
  ④ 若本地恢复结果仍是 provisional，则允许完成 leader promotion，但保持 `CommitReady=false`
  ⑤ Runtime 触发 reconcile；只有 `CommitReady=true` 后 leader 才接受新的 Append
  ⑥ 检查 Lease → 过期则 FencedLeader

BecomeFollower (replica.go:209):
  ① 应用新 Meta（支持同 channel epoch 下、更高 LeaderEpoch 的 leader transfer）
  ② 失败所有待处理 Append (failOutstandingAppendWorkLocked)

Tombstone (replica.go:230):
  ① 标记 Tombstoned → 拒绝所有后续操作
```

### 5.5 故障恢复

入口: `replica/recovery.go:11 recoverFromStores`

```
  ① 加载 Checkpoint{Epoch, LogStartOffset, HW} → store/checkpoint.go
  ② 加载 Epoch History → store/history.go
  ③ 校验一致性 (CheckpointHW ≤ LEO, Epoch 匹配)
  ④ 启动时不再立即把 `LEO > CheckpointHW` 的尾巴截断掉；先保留 local tail，发布 `CommitReady=false`
  ⑤ 若后续 probe 证明尾巴是 quorum-safe，就保留并提升 CommitHW；若只存在于 minority/stale leader，则在 reconcile 中截断
  ⑥ reconcile 成功后持久化 fresh checkpoint，推进 `CheckpointHW`，再标记 recovered / `CommitReady=true`
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
- **双水位不要混用**: `HW` 是运行时 CommitHW，驱动 sendack / committed reads / fetch；`CheckpointHW` 是冷恢复安全下界；`CommitReady` 决定 leader 是否已经完成 quorum-safe 收敛。live read path 不能再把 `LoadCheckpoint()` 当作 committed source of truth。
- **HW 推进不可回退**: 运行时 `HW(CommitHW)` 只能前进不能后退。`progress.go:advanceHW` 会检查 newHW ≥ currentHW。
- **Lease 过期自动降级**: Leader Lease 过期后自动变为 FencedLeader，拒绝所有写入但不影响读取。见 `replica/append.go:appendableLocked`。
- **Cross-channel durable batching**: `store/commit.go` 使用 200µs 窗口跨频道合并 Pebble durable 写入；Leader 的 synced Append 和 Follower 的 ApplyFetch 都走同一个 coordinator。单频道的 `writeMu` 仍然串行，且 sync 完成前不会发布新的 LEO。
- **Checkpoint 不再阻塞 sendack**: leader 在 quorum commit 后先完成 Append waiter，Checkpoint 持久化走后台 coalescing；若 checkpoint 写盘长期失败，当前实现还缺少显式 health / metrics 暴露。
- **Transport RPC 分片**: Fetch / ReconcileProbe 都按 FNV-64a(ChannelKey) 路由到固定 RPC 流，保证同一频道有序，并复用 warmed pool slot。见 `transport/session.go`。
- **Fetch 批量刷批策略**: 默认保留 200µs 定时窗口；队列到 8 条或累计编码体到 32 KiB 时立即刷批；每轮刷批都带 generation，旧 timer / eager callback 不会误冲掉后续新队列；批量 RPC 失败或返回缺项时，先按 4 条分块再试，只有最终 singleton 或 chunk RPC 本身失败后才退到单条 `Fetch`。
