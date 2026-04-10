# WuKongIM 分布式架构总览

WuKongIM v3.1 采用 **三层分布式架构**，通过职责清晰的分层、差异化的一致性算法和统一的集群网络层，兼顾了元数据强一致、消息高吞吐和运维可观测性。

## 1. 设计目标

- **元数据强一致**：频道、订阅、用户、路由等元数据必须在全集群保持强一致。
- **消息高吞吐**：消息写入以 Channel 为粒度进行复制，支持水平扩展到百万级频道。
- **控制面自治**：集群新增、节点故障、副本迁移等运维动作无需人工介入，由系统自动编排。
- **一份网络**：所有层共享同一套节点间通信基础设施，降低运维复杂度。

## 2. 分层结构

```
┌──────────────────────────────────────────────────────────────────┐
│                  L1 · Controller 控制层                          │
│                一致性算法：Raft（独立 Quorum）                   │
│                                                                  │
│   职责：维护/调度 Group 层的数据                                 │
│   - 集群节点状态（Alive / Suspect / Dead / Draining）            │
│   - 每个 Group 的期望副本分布（GroupAssignment）                 │
│   - 副本迁移/重平衡任务（ReconcileTask）                         │
│   - 全局可观测性的权威视图（GroupRuntimeView）                   │
│                                                                  │
│   代码位置：pkg/controller/raft/                          │
│             pkg/controller/plane/                         │
│             pkg/controller/meta/                          │
└──────────────────────────────────────────────────────────────────┘
                               │ 调度决策/下发任务
                               ▼
┌──────────────────────────────────────────────────────────────────┐
│                  L2 · Group 元数据层                             │
│           一致性算法：MultiRaft（N 个 RaftGroup 复用进程）       │
│                                                                  │
│   职责：存储系统元数据                                           │
│   - 频道信息（ChannelMeta）                                      │
│   - 频道 ISR 分布（Replicas / ISR / Leader / Epoch）             │
│   - 订阅者与成员关系                                             │
│   - 用户信息与路由表                                             │
│                                                                  │
│   代码位置：pkg/replication/multiraft/                           │
│             pkg/cluster/                             │
└──────────────────────────────────────────────────────────────────┘
                               │ 元数据订阅/下发（ChannelMeta）
                               ▼
┌──────────────────────────────────────────────────────────────────┐
│                  L3 · Channel 消息层                             │
│              一致性算法：ISR（每个频道一个 ISR 组）              │
│                                                                  │
│   职责：消息数据的存储与查询                                     │
│   - 消息追加（Append / 幂等去重）                                │
│   - 消息拉取（按 seq 范围）                                      │
│   - HW（High Water Mark）推进与 Epoch 切换                       │
│   - 基于 Pebble 的日志落盘                                       │
│                                                                  │
│   代码位置：pkg/channel/isr/                                 │
│             pkg/channel/node/                             │
│             pkg/channel/log/                              │
└──────────────────────────────────────────────────────────────────┘
                               ▲
                               │  横向贯穿三层的集群网络
┌──────────────────────────────┴───────────────────────────────────┐
│                 Transport · 集群网络层                           │
│                                                                  │
│   - TCP 长连接 + 连接池 + 固定帧协议                             │
│   - 按 msgType 做一层分发，RPC 再按 serviceID 多路复用           │
│   - 三层（Controller / Group / Channel）全部走同一套 Transport   │
│                                                                  │
│   代码位置：pkg/transport/                         │
└──────────────────────────────────────────────────────────────────┘
```

### 2.1 为什么选这三种一致性算法

| 层级       | 算法        | 选择理由                                                                 |
| ---------- | ----------- | ------------------------------------------------------------------------ |
| Controller | Raft        | 集群规模小（通常 3/5 个副本），变更频率低，需要绝对的线性一致和可审计性 |
| Group      | MultiRaft   | 元数据量大但可分片，MultiRaft 在一个进程复用 N 个 Raft Group，成本最优  |
| Channel    | ISR         | 频道数量多（百万级）、写入吞吐敏感，ISR 相比 Raft 写入路径更短、更灵活 |

### 2.2 为什么 Controller 必须独立于 Group

- Controller 需要在 Group 发生故障时仍能做出决策，所以它**不能**依赖任何一个 Group 的可用性。
- 生产部署中，Controller Raft 只在 `ControllerReplicaN` 个节点上起 Raft 进程，其他节点只作为 Agent 上报状态和执行任务（参考 `cluster.go:106-155`）。

## 3. 三层之间的协作

### 3.1 请求写入一条消息的完整链路

```
业务客户端
   │ 1. SendPacket(channelKey, payload)
   ▼
接入节点 (any node)
   │ 2. 通过本地缓存找到该 channel 所在 Group（MultiRaft 的某个 GroupID）
   │ 3. 通过本地缓存或 Group 元数据找到 Channel 的 ISR Leader 节点
   ▼
Channel ISR Leader 节点
   │ 4. channellog.Append  →  isr.Replica.Append
   │ 5. 写本地日志 + 构造 FetchResponse 推送给 Followers
   │ 6. Followers ApplyFetch 完成后 ProgressAck
   │ 7. Leader 推进 HW（达到 MinISR 即可回包）
   ▼
    客户端收到 MessageSeq / MessageID
```

> 注意：步骤 2~3 用到的元数据来自 **Group 层**（MultiRaft），步骤 4~7 的日志复制发生在 **Channel 层**（ISR），Controller 层完全不参与热路径。

### 3.2 一个节点宕机的完整修复链路

```
t0  某节点失联 → 心跳丢失
t1  Controller Leader 通过 EvaluateTimeouts 标记节点为 Suspect，随后 Dead
t2  Planner 发现该节点出现在某些 Group 的 DesiredPeers 中 → 生成 RepairTask
t3  Controller 将 RepairTask 下发给目标节点 Agent
t4  目标节点 Agent 在本地 MultiRaft Runtime 上执行：
      AddLearner → CatchUp → Promote → TransferLeader → RemoveOld
t5  Agent 将 TaskResult 回传 Controller；Controller 更新 RuntimeView
t6  Channel 层感知 ChannelMeta 变化后，触发 ISR 成员调整
```

## 4. 文档索引

- [Controller 控制层详解](./01-controller-layer.md)
- [Group 元数据层详解](./02-group-layer.md)
- [Channel 消息层详解](./03-channel-layer.md)
- [Transport 集群网络层详解](./04-transport-layer.md)

## 5. 术语表

| 术语                 | 含义                                                                        |
| -------------------- | --------------------------------------------------------------------------- |
| NodeID               | 集群节点的全局唯一 ID（uint64）                                             |
| GroupID              | MultiRaft 中一个 Raft 组的编号（uint32）                                    |
| GroupKey             | ISR 中一个 ISR 组的字符串标识，对 Channel 层形如 `channel/<type>/<base64>` |
| Assignment           | Controller 下发的「某个 Group 的期望副本集合」                              |
| RuntimeView          | 节点上报的「某个 Group 的实时状态」，Controller 聚合后可供查询              |
| ReconcileTask        | Controller 下发给节点 Agent 的修复/重平衡任务                               |
| ISR                  | In-Sync Replicas，与 Leader 日志保持同步的副本集合                          |
| HW                   | High Water Mark，ISR 内达成共识、对外可见的已提交 offset                    |
| Epoch / LeaderEpoch  | 配置版本号，防止脑裂与旧 Leader 写入                                        |
| MinISR               | 最小 ISR 数量，写入提交至少需要这么多副本同步                               |
| Lease                | Leader 的租约时间，过期则降级为 FencedLeader（只读不写）                    |
