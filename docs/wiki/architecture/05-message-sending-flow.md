# 发送消息完整流程

> 本文档描述一条消息从客户端发送到所有接收者的完整链路，涉及三层分布式架构的协作方式，并说明频道寻址和脑裂防护机制。

## 1. 流程总览

```
客户端 SendPacket
  │
  ▼
┌─────────────────────────────────────────────────────────────┐
│  接入层（Gateway）                                          │
│  OnFrame → mapSendCommand → message.App.Send                │
└─────────────────────┬───────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────┐
│  业务层（UseCase · Message）                                │
│  Send → NormalizePersonChannel → sendDurable                │
│    → sendWithMetaRefreshRetry → channellog.Append           │
└─────────────────────┬───────────────────────────────────────┘
                      │ 频道寻址（两步定位）
                      ▼
┌─────────────────────────────────────────────────────────────┐
│  L2 Slot 层 — 元数据查询                                    │
│  SlotForKey(channelID) → CRC32 % SlotCount + 1             │
│  → 从对应 Slot Raft Leader 获取 ChannelRuntimeMeta         │
│  → 得到 ISR Leader / Replicas / Epoch 等信息               │
└─────────────────────┬───────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────┐
│  L3 Channel 层 — ISR 消息写入                               │
│  isr.Replica.Append → Group Commit → 本地日志写入          │
│  → FetchResponse 推送 Followers → ProgressAck              │
│  → HW 推进（MinISR 副本确认）→ 返回 MessageSeq             │
└─────────────────────┬───────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────┐
│  回包 + 异步投递                                            │
│  SendackPacket → 客户端                                     │
│  asyncCommittedDispatcher → 路由到 ISR Leader 节点          │
│  → 解析订阅者 → 解析在线端点 → Push RecvPacket 到接收者    │
└─────────────────────────────────────────────────────────────┘
```

## 2. 详细步骤

### 2.1 客户端发送（Gateway 接入）

**代码位置**：`internal/access/gateway/frame_router.go` · `internal/access/gateway/mapper.go`

1. 客户端通过 TCP 长连接发送 `SendPacket`（WuKong 二进制协议帧）。
2. Gateway 的 `OnFrame` 分发器根据帧类型路由到 `handleSend`。
3. `mapSendCommand()` 从 Session 中提取发送者 UID，并对**个人频道**做规范化处理——将双方 UID 按字典序拼接为固定的 `channelID`，确保 A→B 和 B→A 对话使用同一个频道。
4. 创建带超时的 `context`（默认 10s），调用 `message.App.Send()`。

```
SendPacket
  → OnFrame(ctx, frame)
  → handleSend(ctx, pkt)
  → mapSendCommand(ctx, pkt)       // 提取 FromUID、规范化 channelID
  → message.App.Send(ctx, cmd)
```

### 2.2 业务校验与持久化写入（UseCase · Message）

**代码位置**：`internal/usecase/message/send.go` · `internal/usecase/message/retry.go`

1. **校验发送者**：`FromUID` 不能为空，频道类型必须是 Person 或 Group。
2. **个人频道规范化**：如果是 Person 类型，调用 `NormalizePersonChannel()` 确保 channelID 唯一且确定。
3. **构造持久消息**：填充时间戳、消息帧元数据、载荷等字段。
4. **带元数据刷新的追加**：调用 `sendWithMetaRefreshRetry()` 尝试写入频道日志。
   - 首次尝试直接 `cluster.Append()`
   - 如果返回 `ErrStaleMeta` 或 `ErrNotLeader`，从 Slot 层重新获取最新 `ChannelRuntimeMeta`，更新本地缓存后重试一次
5. **异步投递**：写入成功后，调用 `dispatcher.SubmitCommitted()` 触发异步消息投递。
6. **返回结果**：将 `MessageID` 和 `MessageSeq` 封装为 `SendResult`。

```go
// retry.go — 元数据刷新重试核心逻辑
result, err := cluster.Append(ctx, req)
if shouldRefreshAndRetry(err) {
    meta := refresher.RefreshChannelMeta(ctx, channelKey)
    cluster.ApplyMeta(meta)
    result, err = cluster.Append(ctx, req)  // 重试
}
```

### 2.3 频道寻址（两步定位）

#### 第一步：Key → SlotID（哈希路由）

**代码位置**：`pkg/cluster/router.go`

```go
func SlotForKey(key string) SlotID {
    return SlotID(crc32.ChecksumIEEE([]byte(key)) % slotCount + 1)
}
```

- 将 `channelID` 字符串经 CRC32 哈希，对 SlotCount 取模后加 1（Slot 编号从 1 开始）。
- SlotCount 在集群初始化时确定，后续不可变。
- 所有节点使用相同算法，保证路由一致性。

#### 第二步：SlotID → ChannelRuntimeMeta → ISR Leader

**代码位置**：`pkg/slot/proxy/store.go` · `internal/app/channelmeta.go`

1. 根据 SlotID 找到该 Slot 的 Raft Leader 节点。
2. 从 Raft Leader 读取该频道的 `ChannelRuntimeMeta`，包含：

| 字段         | 说明                               |
| ------------ | ---------------------------------- |
| Leader       | ISR 组的 Leader 节点 ID            |
| Replicas     | 所有副本节点列表                   |
| ISR          | 同步副本集合                       |
| LeaderEpoch  | Leader 版本号                      |
| ChannelEpoch | 频道配置版本号                     |
| MinISR       | 最小同步副本数                     |
| LeaseUntil   | Leader 租约到期时间                |
| Status       | 频道状态（Creating/Active/...）    |

3. **元数据缓存**：`channelMetaSync` 组件周期性同步频道元数据到本地 ISR 运行时，避免每次写入都远程查询。

4. **Leader 追踪**：如果 Slot RPC 请求到达非 Leader 节点，通过 `callAuthoritativeRPC` 的 Leader 追踪循环重定向到正确的 Leader。

```
寻址流程：
channelID = "group_abc"
    → CRC32("group_abc") % 256 + 1 = SlotID(137)
    → SlotID(137) 的 Raft Leader 在 Node-2
    → Node-2 返回 ChannelRuntimeMeta{Leader: Node-3, ISR: [3,1,2]}
    → 消息写入路由到 Node-3
```

### 2.4 ISR 消息写入（Channel 层）

**代码位置**：`pkg/channel/log/send.go` · `pkg/channel/isr/append.go` · `pkg/channel/isr/progress.go`

#### 2.4.1 入口校验

```
channellog.Append(req)
  → 频道是否存在？状态是否 Active？
  → ChannelEpoch 和 LeaderEpoch 是否匹配？（防止 stale 写入）
  → 幂等检查：(FromUID, ClientMsgNo) 是否已写入？
      ├─ PayloadHash 一致 → 返回缓存结果（真重复）
      └─ PayloadHash 不一致 → 返回冲突错误
```

#### 2.4.2 Leader Append（Group Commit）

```
isr.Replica.Append(records)
  → 角色校验：必须是 Leader（非 FencedLeader / Follower）
  → 租约校验：now < LeaseUntil（否则降级为 FencedLeader）
  → ISR 数量校验：len(ISR) >= MinISR
  → 进入 Group Commit 收集器：
      等待 1ms / 凑满 64 条 / 凑满 64KB（先到先触发）
  → 合并多个并发请求为一批
  → 写入本地日志 + fsync
  → LEO（Log End Offset）前进
```

#### 2.4.3 ISR 复制（推模型）

```
Leader                                  Follower
  │ FetchResponse ─────────────────────▸ │
  │  (records, leaderHW, epoch)          │
  │                                      │ ApplyFetch: 写入本地日志
  │ ◀───────────── ProgressAck ─────────│
  │  (matchOffset)                       │
```

- Leader 将新记录通过 `FetchResponse` 主动推送给所有 ISR Follower。
- Follower 收到后写入本地日志，发送 `ProgressAck(matchOffset)` 确认。

#### 2.4.4 HW 推进

```go
// progress.go — HW 推进算法
matches := collect ISR members' matchOffset
sort descending: [100, 90, 80]
candidateHW = matches[MinISR - 1]  // 第 MinISR 大的值
// 例：3 副本 ISR, MinISR=2 → candidateHW = matches[1] = 90
// 含义：至少 2 个副本确认了 offset ≤ 90 的数据
```

当候选 HW > 当前 HW 时：
1. 推进 HW
2. 原子写入 Checkpoint（HW + Epoch）和幂等记录到 Pebble
3. 唤醒等待中的 Append waiter
4. 返回 `MessageSeq = HW + 1`（用户可见的 1-indexed 序号）

### 2.5 回包给发送者

**代码位置**：`internal/access/gateway/mapper.go`

写入成功后，Gateway 构造 `SendackPacket` 回传给客户端：

```go
ctx.WriteFrame(&frame.SendackPacket{
    MessageID:   result.MessageID,
    MessageSeq:  result.MessageSeq,
    ClientSeq:   pkt.ClientSeq,
    ClientMsgNo: pkt.ClientMsgNo,
    ReasonCode:  frame.ReasonSuccess,
})
```

### 2.6 异步消息投递

**代码位置**：`internal/app/deliveryrouting.go`

回包和投递是**解耦的**——回包后立即在新 goroutine 中执行投递。

#### 2.6.1 路由到 ISR Leader 节点

```
asyncCommittedDispatcher.SubmitCommitted(msg)
  → go routeCommitted(ctx, msg)
      → 单节点模式（preferLocal=true）→ 直接本地提交
      → 多节点模式：
          → channelLog.Status(channelKey) → 获取 ISR Leader
          → Leader 在本地 → submitLocal()
          → Leader 在远端 → nodeClient.SubmitCommitted(nodeID, msg) via RPC
          → 获取 Leader 失败 → 重试 3 次（20ms * attempt 退避）
          → 全部失败 → 降级为仅更新会话列表
```

#### 2.6.2 解析订阅者与在线端点

```
localDeliveryResolver.ResolvePage()
  → subscribers.NextPage()           // 从 Slot 层分页获取频道订阅者列表
  → authority.EndpointsByUIDs(uids)  // 查询每个 UID 的在线端点
  → 返回 RouteKey 列表：{UID, NodeID, BootID, SessionID}
```

#### 2.6.3 推送 RecvPacket

```
distributedDeliveryPush.Push(cmd)
  → buildRealtimeRecvPacket(msg, recipientUID)
  → 个人频道特殊处理：RecvPacket.ChannelID 替换为发送者 UID
  → 按 NodeID 分组路由：
      ├─ 本地路由 → localDeliveryPush.pushFrame()
      │              → online.Connection(sessionID)
      │              → conn.Session.WriteFrame(recvPacket)  // TCP 直写
      │
      └─ 远端路由 → client.PushBatch(nodeID, pushCmd)
                     → RPC 发送预编码帧到目标节点
                     → 目标节点 WriteFrame 到接收者连接
```

### 2.7 接收者收到消息

接收者的 TCP 连接收到 `RecvPacket`，包含：
- MessageID / MessageSeq
- FromUID、ChannelID、ChannelType
- Payload（消息正文）
- Timestamp、MsgKey 等元数据

对于**个人频道**，接收者看到的 `ChannelID` 是发送者的 UID（而非内部规范化后的频道 ID），便于客户端直接展示对话界面。

## 3. 频道寻址机制详解

### 3.1 两层路由设计

WuKongIM 的频道寻址采用**两层路由**设计，将"找到频道的元数据"和"找到频道的消息日志"分离到不同的层次：

```
                    ┌──────────────┐
  channelID ──────▸ │   CRC32 Hash  │ ──▸ SlotID
                    └──────────────┘
                           │
                           ▼
                    ┌──────────────┐
    SlotID ────────▸│  Slot Raft   │ ──▸ ChannelRuntimeMeta
                    │  Leader      │      {Leader, ISR, Epoch, ...}
                    └──────────────┘
                           │
                           ▼
                    ┌──────────────┐
    ISR Leader ────▸│  Channel ISR │ ──▸ 消息读写
                    │  Leader Node │
                    └──────────────┘
```

### 3.2 为什么不用一步直接定位

- **解耦元数据与数据**：频道的 ISR Leader 可能因故障/重平衡而迁移，但 `channelID → SlotID` 的映射是静态的。Slot 层负责维护最新的 Leader 信息，客户端不需要感知 Leader 变化。
- **元数据强一致**：通过 Raft 保证频道元数据的一致性，避免出现两个节点同时认为自己是 Leader 的情况。
- **水平扩展**：Slot 将元数据分散到多个 Raft 组，避免单点瓶颈。

### 3.3 元数据缓存与刷新

每个节点本地缓存频道的 `ChannelRuntimeMeta`：

- **主动同步**：`channelMetaSync` 周期性从 Slot 层拉取变更，应用到本地 ISR 运行时。
- **被动刷新**：当写入返回 `ErrStaleMeta` 或 `ErrNotLeader` 时，`sendWithMetaRefreshRetry` 从 Slot 层权威源重新获取最新元数据后重试。
- **Epoch 保护**：每次写入携带 `ExpectedChannelEpoch` 和 `ExpectedLeaderEpoch`，Channel 层校验后才接受写入，防止过时的元数据导致数据写入错误的 Leader。

## 4. 脑裂防护机制

三层架构在每一层都有针对性的脑裂防护措施。

### 4.1 Controller 层：Raft 共识

- 使用标准 Raft 协议（etcd/raft v3），Leader 选举需要多数派同意。
- **PreVote**（`controllerraft/service.go:154`）：候选人在正式选举前先发 PreVote，只有得到多数同意才开始真正选举，防止分区后的节点用高 Term 干扰集群。
- **两阶段超时**：节点状态 Alive → Suspect（3s）→ Dead（10s），避免短暂网络抖动触发不必要的副本迁移。

### 4.2 Slot 层：PreVote + CheckQuorum

**代码位置**：`pkg/slot/multiraft/slot.go`

```go
CheckQuorum: raftOpts.CheckQuorum,
PreVote:     raftOpts.PreVote,
```

| 机制         | 作用                                                                 |
| ------------ | -------------------------------------------------------------------- |
| PreVote      | 分区节点无法获得多数派 PreVote 支持，不会发起真正选举，不会扰乱集群 |
| CheckQuorum  | Leader 周期性检查是否仍能与多数派通信，否则主动让出 Leadership       |

**请求转发**：非 Leader 节点收到写请求时，通过 RPC 转发到 Leader。如果收到 `NotLeader` 响应，根据 `leaderID` 提示重定向。

### 4.3 Channel 层：Leader 租约 + FencedLeader + Epoch

这是三层中最关键的脑裂防护，因为 Channel 层使用的是 ISR 协议（非 Raft），Leader 选举由外部（Slot 层）指定，需要额外机制防止双写。

#### 4.3.1 Leader 租约（Lease）

**代码位置**：`pkg/channel/isr/append.go:28-31`

```go
if !r.now().Before(r.meta.LeaseUntil) {
    r.state.Role = RoleFencedLeader  // 降级为只读
    return CommitResult{}, ErrLeaseExpired
}
```

- 每个 Channel Leader 持有一个租约（`LeaseUntil` 时间戳）。
- 租约由 Slot 层在下发 `ChannelRuntimeMeta` 时设定和续期。
- **写入前必须校验租约有效**，过期则立即降级为 `FencedLeader`。
- FencedLeader 拒绝所有写入，但仍可响应 Fetch 请求（供 Follower 追数据）。

#### 4.3.2 FencedLeader 状态

```
               正常写入
  Leader ──────────────▸ 接受 Append
    │
    │ 租约过期
    ▼
  FencedLeader ────────▸ 拒绝 Append（ErrLeaseExpired）
    │                    仍可响应 Fetch
    │ 新元数据下发
    ▼
  Follower ────────────▸ 接受新 Leader 推送的 FetchResponse
```

#### 4.3.3 Epoch 保护

- **LeaderEpoch**：每次 Leader 切换时递增，写入请求必须携带匹配的 LeaderEpoch。
- **ChannelEpoch**：每次 ISR 成员变化时递增，防止过时的元数据导致错误路由。
- 新 Leader 就任时，**截断 LEO 到 HW**（`replica.go:132-183`），丢弃上一任 Leader 未提交的数据，确保不会出现幽灵写入。

#### 4.3.4 Epoch History 分歧检测

**代码位置**：`pkg/channel/isr/history.go` · `pkg/channel/isr/progress.go`

当发生 Leader 切换后，新旧 Leader 的日志可能出现分歧：

```
旧 Leader：[1, 2, 3, 4(epoch=1), 5(epoch=1)]
新 Leader：[1, 2, 3, 4(epoch=2), 5(epoch=2), 6(epoch=2)]

Follower(旧 Leader) FetchRequest(offset=6, epoch=1)
  → 新 Leader 查 EpochHistory：epoch=1 的 range = [1, 4)
  → 返回 truncateTo=4
  → Follower 截断到 offset=3，重新从 offset=4 拉取新数据
```

### 4.4 脑裂防护总结

```
┌──────────────────────────────────────────────────────────────────┐
│                    脑裂防护全景                                  │
├──────────┬───────────────────────────────────────────────────────┤
│ L1 层    │ Raft 多数派选举 + PreVote 防扰动                     │
│ Controller│ 两阶段超时（Suspect 3s → Dead 10s）防抖动           │
├──────────┼───────────────────────────────────────────────────────┤
│ L2 层    │ MultiRaft PreVote + CheckQuorum                      │
│ Slot     │ Leader 主动让位 + 请求转发 + Leader 追踪重定向       │
├──────────┼───────────────────────────────────────────────────────┤
│ L3 层    │ Leader 租约（Lease）→ 过期降级为 FencedLeader        │
│ Channel  │ Epoch 保护 → 旧 Leader 写入被拒绝                   │
│          │ 新 Leader 截断 LEO 到 HW → 丢弃未提交数据           │
│          │ EpochHistory → 精确定位分歧点 → Follower 截断恢复   │
└──────────┴───────────────────────────────────────────────────────┘
```

## 5. 关键设计决策

### 5.1 为什么回包和投递解耦

- 消息一旦写入 ISR 并达到 HW 即视为**已提交**，不会丢失。
- 回包延迟只取决于 ISR 写入延迟（通常 < 10ms），不受投递解析影响。
- 投递失败（如接收者离线）不影响消息持久化，后续可通过拉取接口获取。

### 5.2 为什么投递要路由到 ISR Leader

- ISR Leader 是频道的"所有者"，集中在 Leader 节点做投递可以：
  - 避免重复投递（多节点同时投递同一消息）
  - 统一管理投递状态（ACK 跟踪、离线通知）
  - 简化会话更新的一致性

### 5.3 个人频道的特殊处理

- **发送端**：channelID 规范化为 `min(A,B) + max(A,B)` 的确定性形式，确保双向对话共用同一个频道。
- **接收端**：RecvPacket 的 channelID 替换为发送者 UID，方便客户端直接展示"来自谁的消息"。

## 6. 错误处理与容错

| 错误场景                | 处理方式                                                |
| ----------------------- | ------------------------------------------------------- |
| Slot Leader 不可达      | RPC Leader 追踪循环，尝试所有 Peers 找到新 Leader       |
| Channel 元数据过期      | `sendWithMetaRefreshRetry` 从 Slot 权威源刷新后重试     |
| ISR Leader 租约过期     | 降级为 FencedLeader，返回 `ErrLeaseExpired`，触发重试   |
| ISR 副本不足            | `len(ISR) < MinISR` 时拒绝写入，返回错误                |
| 投递 Leader 查找失败    | 重试 3 次后降级为仅更新会话列表（不实时推送）           |
| 远端节点推送失败        | 标记为 Retryable，由投递运行时调度重试                  |
| 消息重复发送            | 幂等检查 (FromUID, ClientMsgNo) + PayloadHash，返回缓存 |
