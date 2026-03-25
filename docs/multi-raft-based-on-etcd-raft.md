# Cockroach 是如何基于 etcd/raft 实现 multi-raft 的

本文整理 `learn_project/cockroach` 中与 Raft 相关的核心代码，说明
Cockroach 是如何基于 `etcd-io/raft` 的 `RawNode` 模型实现 multi-raft
的。

需要先说明一点：这里的 Cockroach 并不是通过一个独立的 `MultiRaft`
抽象来管理所有 raft group。相反，它早就把旧的 `multiraft` 抽象拆掉
了，把“单 group 的共识内核”和“多 group 的组织与调度”分别落到了
`Replica` 和 `Store` 中。

对应的历史 RFC 在：

- `docs/RFCS/20151213_dismantle_multiraft.md`

其中最关键的设计结论是：

- “每个 range 的每个副本”对应一个 `Replica`
- 每个 `Replica` 直接内嵌一个 `raft.RawNode`
- 每个 `Store` 统一负责这些 raft group 的消息路由、tick 调度、Ready
  处理、心跳合并和快照发送

所以，Cockroach 的 multi-raft 可以概括成一句话：

`单 group 用 RawNode，多 group 用 Store 统一调度`

## 1. 先看 etcd/raft 提供了什么

Cockroach 使用的 Raft 接口模型，本质上仍然是 etcd/raft 的
`RawNode` 模型。对应代码在：

- `pkg/raft/rawnode.go`

几个关键点：

1. `NewRawNode(config)` 创建一个单独的 raft group
2. `Tick()` 推进该 group 的逻辑时钟
3. `Step(msg)` 向该 group 投递网络消息
4. `Ready()` 取出当前这一个 group 需要应用层处理的工作

也就是说，`RawNode` 只关心“一个 raft group 如何推进”，它并不负责：

- 多个 raft group 的统一管理
- 消息按 range 路由
- 定时给很多 group 调 `Tick()`
- 调度 `Ready()` 的执行顺序
- 与数据库存储层深度耦合的快照、日志持久化和应用状态机

这些事情都需要应用层自己完成。Cockroach 正是在这一层实现了
multi-raft。

## 2. Cockroach 的基本映射关系

在 Cockroach 里，raft group 与存储对象的对应关系是：

- 一个 `Range` 有多个副本
- 每个副本在本地表现为一个 `Replica`
- 每个 `Replica` 内部持有一个 `raft.RawNode`

对应代码：

- `pkg/kv/kvserver/replica.go`
- `pkg/kv/kvserver/replica_init.go`

`Replica` 内部保存 raft group 的字段是：

- `r.mu.internalRaftGroup *raft.RawNode`

初始化逻辑在 `initRaftGroupRaftMuLockedReplicaMuLocked()`：

- `pkg/kv/kvserver/replica_init.go`

核心代码路径是：

```go
rg, err := raft.NewRawNode(newRaftConfig(...))
r.mu.internalRaftGroup = rg
```

这说明 Cockroach 的“单个 raft group”并不是额外再包装一层，而是直接
把 `RawNode` 嵌进 `Replica` 中。

## 3. `newRaftConfig()` 如何把 Cockroach 的需求接到 raft 上

创建 `RawNode` 时，Cockroach 会构造自己的 `raft.Config`，代码在：

- `pkg/kv/kvserver/store.go`

这里可以看到它传给 raft 的内容包括：

- `ID`
- `Applied`
- `ElectionTick`
- `ElectionJitterTick`
- `HeartbeatTick`
- `MaxUncommittedEntriesSize`
- `MaxCommittedSizePerReady`
- `MaxSizePerMsg`
- `MaxInflightMsgs`
- `MaxInflightBytes`
- `Storage`
- `Logger`
- `StoreLiveness`
- `PreVote`
- `CheckQuorum`

这说明 Cockroach 并没有把 raft 当成“黑盒网络库”来用，而是把自己对选
举、日志大小、流控、liveness、兼容版本的要求都压到了 `raft.Config`
中。

尤其值得注意的是：

- `Storage` 不是内存实现，而是 Cockroach 自己的 `replicaRaftStorage`
- `PreVote` 默认开启
- `CheckQuorum` 由 `StoreConfig` 控制

## 4. multi-raft 的“multi”是由 `Store` 来做的

历史 RFC 已经说明，Cockroach 拆掉旧 `multiraft` 后，把“multi”的职责
下沉到了 `Store`。

在今天的代码里，这个设计体现在：

- `Store` 持有全局的 `replicasByRangeID`
- `Store` 持有 `raftScheduler`
- `Store` 持有 `RaftTransport`
- `Store` 负责 tick loop、coalesced heartbeat loop、snapshot queue

关键代码：

- `pkg/kv/kvserver/store_raft.go`
- `pkg/kv/kvserver/scheduler.go`

这意味着：

- `Replica` 负责一个 raft group 的本地状态推进
- `Store` 负责在节点内组织成千上万个 raft group 一起工作

这是 Cockroach multi-raft 的核心分层。

## 5. `raftScheduler`：节点级统一调度器

`raftScheduler` 是 multi-raft 最重要的基础设施之一，代码在：

- `pkg/kv/kvserver/scheduler.go`

从接口可以看出，调度器关心的工作类型主要有三类：

1. `processRequestQueue(rangeID)`
2. `processTick(rangeID)`
3. `processReady(rangeID)`

它的 worker 执行顺序也非常关键：

1. 先处理该 range 收到的 raft 请求
2. 再处理 tick
3. 最后处理 Ready

代码中甚至专门强调了：

- 不要把 request/tick 的处理顺序挪到 `processReady()` 后面

这个顺序的意义是：

- 先把新消息 `Step()` 进 `RawNode`
- 再推进逻辑时钟
- 最后统一处理由这些操作产生的 `Ready`

这样，一个 `Store` 上很多 raft group 的推进就被统一收敛到了同一个
调度器模型中。

所以从实现角度看，Cockroach 的 multi-raft 并不是：

- 一个 `MultiNode` 同时管理所有 group

而是：

- 一个 `Store` 维护很多 `Replica.RawNode`
- 一个 `raftScheduler` 驱动这些 group 的执行

## 6. raft 消息是如何路由到对应 group 的

网络入口虽然叫 `MultiRaft` 服务，但它只是 RPC 服务名：

- `pkg/kv/kvserver/storage_services.proto`

真正的消息路由是在 `Store.HandleRaftRequest()` 中完成的：

- `pkg/kv/kvserver/store_raft.go`

处理流程如下：

1. 收到一条 `RaftMessageRequest`
2. 如果是 coalesced heartbeat，先拆成普通 heartbeat
3. 按 `RangeID` 放入该 range 对应的接收队列
4. 把该 `RangeID` 放入 `raftScheduler`
5. 调度器后续会调用 `processRequestQueue(rangeID)`

`processRequestQueue()` 会：

1. 把该 range 队列中的请求全部 drain 出来
2. 找到或懒创建目标 `Replica`
3. 在 `Replica.raftMu` 保护下执行 `processRaftRequestWithReplica()`
4. 后者最终调用 `stepRaftGroupRaftMuLocked()`

而 `stepRaftGroupRaftMuLocked()` 最终做的事情就是：

```go
err := raftGroup.Step(req.Message)
```

也就是把消息正式送入对应 range 的 `RawNode`。

因此，“按 range 分发消息到正确的 raft group”这件事，是 Cockroach 的
`Store + Scheduler + Replica` 三者配合完成的，而不是 etcd/raft 自带
的能力。

## 7. `Replica.stepRaftGroup...()` 在 `Step()` 前后做了什么

Cockroach 对 `RawNode.Step()` 的使用并不是裸调用，而是包了一层很厚的
外围逻辑，主要在：

- `pkg/kv/kvserver/replica_raft.go`

做的事情包括：

- 如有需要，先把 quiesced range 唤醒
- 如果 follower 在休眠状态收到来自非 leader 的消息，必要时同时唤醒
  leader，避免错误选举
- 更新 `lastUpdateTimes`
- 特判 `MsgPreVote` / `MsgVote`
- 特判 `MsgApp`
- 特判 `MsgAppResp`
- 再调用 `raftGroup.Step(req.Message)`

这说明 Cockroach 的 raft 集成是“深度系统化”的：

- etcd/raft 负责一致性推进
- Cockroach 在外层维护 quiesce、lease、liveness、flow control、追踪
  和观测指标

## 8. `replicaRaftStorage`：如何把数据库存储层接成 raft.Storage

Cockroach 通过 `replicaRaftStorage` 实现了 raft 的 `Storage` 接口：

- `pkg/kv/kvserver/replica_raftstorage.go`

这部分非常关键，因为它决定了 `RawNode` 看到的 raft 日志和状态来自哪
里。

它提供的接口包括：

- `InitialState()`
- `Entries(lo, hi, maxSize)`
- `Term(index)`
- `LastIndex()`
- `Compacted()`
- `LogSnapshot()`
- `Snapshot()`

其中：

- `InitialState()` 从 Cockroach 的 raft state loader 读取 `HardState`
  和 `ConfState`
- `Entries()` / `Term()` / `LastIndex()` 等从 Cockroach 的 raft log
  存储读取
- `Snapshot()` 返回的只是一个占位 snapshot metadata

这里最值得注意的一点是：

Cockroach 明确说明，真正的 range snapshot 发送，并不是完全沿着
etcd/raft 默认的 snapshot 发送语义走的。`Snapshot()` 只是给 raft 一个
占位结果，真正的数据快照由 Cockroach 自己的 snapshot queue 和
streaming snapshot 路径去生成和发送。

所以它对 etcd/raft 的使用方式是：

- 共识状态机和日志复制规则遵循 `RawNode`
- 快照传输和存储交互由数据库自己接管

## 9. `handleRaftReady()`：Cockroach 版 Ready 循环

在 etcd/raft 模型里，`Ready` 是应用层和共识内核之间的交接点。

Cockroach 的应用层实现基本都收敛在：

- `pkg/kv/kvserver/replica_raft.go`
- `handleRaftReady()`
- `handleRaftReadyRaftMuLocked()`

整体流程可以概括为：

1. 锁住 `raftMu`
2. 给 `RawNode` 投递本地消息和存储确认
3. flush proposal buffer
4. 检查 `HasReady()`
5. 如果有 Ready，取出 `ready := raftGroup.Ready()`
6. 先发送 `ready.Messages`
7. 再从日志快照中装载 committed entries
8. 处理 `ready.StorageAppend`
9. 应用 committed entries 到 replicated state machine
10. 调 `raftGroup.AckApplied(toApply)`
11. 如果还有新的 Ready，再把该 range 重新入队

其中有几个关键设计：

### 9.1 先发消息，再做部分存储读取

代码里专门先 `sendRaftMessages()`，再从 log snapshot 读 committed
entries。

目的很明确：

- 避免发送路径被本地存储读取阻塞
- 降低 Ready 处理尾延迟

### 9.2 `StorageAppend` 与 apply 分阶段处理

`Ready` 中新的日志写入和 committed entry 应用并不是混在一起做的，而
是明显分阶段：

- 先处理 `ready.StorageAppend`
- 再 apply committed entries

这保证了 raft log 持久化状态和状态机应用的顺序。

### 9.3 应用后显式确认给 `RawNode`

在 apply 完 committed entries 之后，Cockroach 会调用：

```go
raftGroup.AckApplied(toApply)
```

这对应的是把应用层已经完成的部分反馈回 raft 内核，让 `RawNode`
推进自身的稳定状态。

### 9.4 如果还有 Ready，则重新入队

处理完一轮后，如果 `raftGroup.HasReady()` 仍然为真，就再次调用：

```go
r.store.enqueueRaftUpdateCheck(r.RangeID)
```

这体现出一种典型的 multi-raft 调度模式：

- 不在一个 goroutine 里死循环跑某个 group
- 而是“处理一轮 Ready 后，必要时重新交回调度器”

这样可以让大量 raft group 更公平地共享 Store 上的执行资源。

## 10. tick 是如何统一驱动很多 raft group 的

tick 不是每个 `Replica` 自己开一个 goroutine，而是 `Store` 统一驱动。

相关代码在：

- `pkg/kv/kvserver/store_raft.go`
- `pkg/kv/kvserver/replica_raft.go`

流程如下：

1. `Store.processRaft()` 启动统一的 `raftTickLoop()`
2. tick loop 周期性把需要 tick 的 range 批量放进 scheduler
3. scheduler 调用 `processTick(rangeID)`
4. `processTick()` 调 `Replica.tick()`
5. `Replica.tick()` 最终调用 `r.mu.internalRaftGroup.Tick()`

这就是 Cockroach 统一推进所有 raft group 时间轮的方式。

因此，etcd/raft 只负责：

- 某个 group tick 一次会发生什么

而 Cockroach 负责：

- 哪些 group 需要 tick
- tick 频率如何控制
- tick 工作如何和 request/ready 共享执行资源

## 11. 消息发送是怎样接到 transport 层的

`Ready.Messages` 最终会通过：

- `sendRaftMessages()`
- `sendRaftMessage()`

发出去，对应代码在：

- `pkg/kv/kvserver/replica_raft.go`

发送逻辑有几个关键分支：

### 11.1 心跳会被合并

如果是 heartbeat / heartbeat response，优先走 heartbeat coalescing。

发送循环在：

- `pkg/kv/kvserver/store_raft.go`
- `coalescedHeartbeatsLoop()`

这说明 Cockroach 针对 multi-raft 做了重要优化：

- 大量 range 的心跳不逐条发送
- 而是按目标节点或 store 聚合后批量发

### 11.2 `MsgSnap` 不直接按普通消息发送

如果是 `MsgSnap`，不会直接通过普通消息队列发，而是交给：

- `raftSnapshotQueue`

这与前面 `replicaRaftStorage.Snapshot()` 的占位设计是一致的，说明
Cockroach 对快照完全走的是自己的一套外层机制。

### 11.3 普通消息通过 `RaftTransport.SendAsync()` 发出

普通 raft 消息会被包装成 `RaftMessageRequest`，然后交给：

- `pkg/kv/kvserver/raft_transport.go`
- `RaftTransport.SendAsync()`

`RaftTransport` 负责：

- 到目标节点的连接管理
- per-node/per-class 发送队列
- 异步收发双向流
- 把响应重新交回 `Store.HandleRaftResponse()`

这意味着 transport 也是 multi-raft 的一部分：

- `RawNode` 只产出 message
- `Replica` 把 message 补足 range/replica 元信息
- `RaftTransport` 才真正负责跨节点发送

## 12. 本地消息与存储确认也被纳入同一套调度

Cockroach 还有一个容易忽略但很重要的点：

`RawNode` 的推进并不只来自网络消息，还来自本地存储确认和本地消息。

对应代码：

- `sendStorageAck()`
- `deliverLocalRaftMsgsRaftMuLockedReplicaMuLocked()`

当存储 append 完成后，会生成本地 ack，再通过：

- `raftGroup.AckAppend(m)`

交还给 `RawNode`。

如果这是第一条待投递的本地消息，还会触发：

- `enqueueRaftUpdateCheck(rangeID)`

也就是说，Cockroach 把：

- 网络消息
- tick
- 本地存储确认
- flow control 相关本地事件

都统一收敛到同一个按 range 调度的执行模型里。

这是它 multi-raft 能稳定工作的关键之一。

## 13. 为什么 Cockroach 要拆掉旧的 multiraft 抽象

从 RFC 来看，拆掉旧 `multiraft` 的根本原因是：

1. 旧抽象试图与存储层解耦，但实际上两者高度耦合
2. active raft group 等信息在多个地方重复维护，容易并发失配
3. channel/goroutine 风格让与 `Replica` 现有锁体系协调非常困难
4. 换到 `RawNode` 后，可以直接把 raft 操作和 `Replica` 状态放到同一
   套锁语义下处理

这就是为什么今天看到的结构是：

- 不是“独立 multiraft 包 + storage 包”
- 而是“`Replica` 直接持有 `RawNode`，`Store` 统一协调多个
  `Replica`”

## 14. 一个简化的调用链

从外部消息进入，到 raft 状态推进，再到状态机应用，主路径可以简化成：

```text
RaftTransport / MultiRaft RPC
  -> Store.HandleRaftRequest
  -> per-range raft receive queue
  -> raftScheduler
  -> Store.processRequestQueue(rangeID)
  -> Replica.stepRaftGroupRaftMuLocked
  -> RawNode.Step(msg)
  -> raftScheduler / Store.processReady(rangeID)
  -> Replica.handleRaftReady
  -> RawNode.Ready()
  -> append raft log / apply snapshot / apply committed entries
  -> RawNode.AckApplied(...)
  -> if HasReady() => re-enqueue
```

而 tick 路径则是：

```text
Store.raftTickLoop
  -> raftScheduler.EnqueueRaftTicks
  -> Store.processTick(rangeID)
  -> Replica.tick()
  -> RawNode.Tick()
  -> if work produced => enqueue Ready
```

## 15. 最终总结

Cockroach 基于 etcd/raft 实现 multi-raft 的方式，不是维护一个中心化的
“多 raft 内核”，而是把职责明确拆成两层：

### 单 group 层

由 `Replica + raft.RawNode + replicaRaftStorage` 负责：

- 单个 raft group 的状态机推进
- 单 group 的日志状态
- 单 group 的 `Step / Tick / Ready / AckApplied`

### 多 group 层

由 `Store + raftScheduler + RaftTransport + snapshot queue` 负责：

- 消息按 `RangeID` 路由
- 多个 raft group 的统一 tick
- Ready 调度
- coalesced heartbeat
- transport 复用
- 快照发送与接收

因此，最准确的理解方式是：

- Cockroach 使用 etcd/raft 的 `RawNode` 作为“单 raft group 共识内核”
- Cockroach 自己实现了“数据库语义下的 multi-raft 运行时”

如果后续继续阅读源码，推荐按下面的顺序看：

1. `pkg/kv/kvserver/replica_init.go`
2. `pkg/kv/kvserver/store.go`
3. `pkg/kv/kvserver/replica_raftstorage.go`
4. `pkg/kv/kvserver/store_raft.go`
5. `pkg/kv/kvserver/scheduler.go`
6. `pkg/kv/kvserver/replica_raft.go`
7. `pkg/kv/kvserver/raft_transport.go`
8. `docs/RFCS/20151213_dismantle_multiraft.md`

读完这几处代码后，Cockroach 的 multi-raft 主体设计就会比较清楚。
