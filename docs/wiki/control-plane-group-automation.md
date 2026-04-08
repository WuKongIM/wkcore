# 控制面 Group 自动管理设计

## 背景

当前项目的控制面 `Group` 由静态配置驱动：

- `internal/app.Config.Cluster.Groups` 显式声明每个 `GroupID` 的 `Peers`
- `raftcluster.Cluster.Start()` 在启动时遍历这份配置并逐个 `OpenGroup` 或 `BootstrapGroup`
- 业务路由通过 `SlotForKey(key)` 把业务 key 映射到固定的 `GroupID`

这种方式的优点是简单直接，但它把以下职责都压给了人工运维：

- 新集群首次分配每个 group 的副本节点
- 节点故障后的副本补齐
- 节点恢复或新增后的负载重平衡
- 节点摘流时的平滑迁移

目标不是改变业务 key 到 `GroupID` 的映射，而是把控制面 `Group` 的 peer 管理从静态配置改成自动管理。

## 已确认的约束

- `GroupCount` 固定，业务 `SlotForKey -> GroupID` 映射保持不变
- 集群节点成员仍然通过 `WK_CLUSTER_NODES` 静态配置
- 每个 group 的目标副本数固定为 `ReplicaN`
- 只要多数副本在线，group 应继续运行，不因为单节点故障停机
- 节点恢复或新增后，系统应自动补齐副本并自动均衡
- 如果 group 已失去 quorum，v1 不做自动救援式重建

## 目标

- 去掉业务 groups 的静态 `Peers` 配置
- 增加统一的控制器，自动决定每个 group 的目标副本集
- 节点故障时自动修复缺副本 group
- 节点恢复或新增后自动重平衡
- 保持现有业务路由和 `GroupID` 语义不变
- 把重配流程做成安全、串行、幂等的状态机推进

## 非目标

- 不改成动态 `GroupCount`
- 不把 `WK_CLUSTER_NODES` 改成自动发现
- 不引入第二套外部控制平面，例如 etcd/operator
- 不在失去 quorum 后做自动数据重建
- 不推翻 `multiraft` 执行层和现有业务 `SlotForKey` 语义

## 决策概要

推荐设计是在现有控制面之上增加一个专门的 `GroupController`。

系统拆成四个角色：

- `Bootstrap Controller Group`
  - 少量静态 bootstrap group
  - 只负责承载 controller 的元数据和决策状态机
- `GroupController`
  - 运行在 controller leader 上
  - 决定每个业务 `GroupID` 的目标 peers
- `GroupAgent`
  - 运行在每个节点本地
  - 执行 controller 下发的目标，不做全局决策
- `Managed Groups`
  - 业务控制面 groups
  - 动态打开、关闭和重配

核心原则是：

- 路由和分片编号继续保持静态
- 副本 placement 和 membership change 改由单一控制器统一管理

## 为什么不能做成去中心化自动重配

如果每个节点都根据本地看到的在线节点和负载自行重算 peers，会出现三个问题：

- 节点在线视图不一致时，多个节点可能同时对同一 group 发起不同变更
- 节点抖动时容易产生修复和回迁振荡
- 无法稳定表达“期望状态”和“当前执行到哪一步”

因此必须有一个唯一可信的决策者。这个角色最自然的实现就是一个用 Raft 承载的控制器 group。

## 总体架构

### 1. 路由层保持不变

`SlotForKey(key)` 仍然通过固定 `GroupCount` 把业务 key 映射到 `1..GroupCount` 范围内的 `GroupID`。

这意味着：

- 业务语义不变
- `metastore` 的分片读取语义不变
- `channel/user/subscriber` 等元数据仍然知道自己归属哪个 `GroupID`

自动化只接管“这个 `GroupID` 由哪些节点承载”。

### 2. controller 统一保存期望分配

controller 是唯一有权决定下列内容的组件：

- 一个 group 当前目标副本集是什么
- 某个节点故障后应把副本补到哪个节点
- 节点恢复后应不应该做 rebalance
- 某次迁移当前执行到哪个步骤

业务节点本地只执行，不自行推导全局 placement。

### 3. managed groups 动态生命周期

普通业务 groups 不再在 `Cluster.Start()` 时通过静态配置一次性全部打开。

改造后变成两阶段：

1. 启动 bootstrap controller group
2. 启动 `GroupAgent`
3. 从 controller 拉取本节点应承载的 groups
4. 本地动态打开缺失 group
5. 本地关闭不再归属当前节点的 group

因此，上层 `raftcluster` 需要从“静态全量 group 配置”演进到“静态 bootstrap + 动态 managed groups”。

## 新增组件

### GroupController

`GroupController` 是控制面控制器，建议新建独立包，例如：

- `pkg/cluster/groupcontroller`

职责：

- 节点心跳处理
- 节点状态判定
- 期望副本集计算
- repair/rebalance 任务生成
- membership change 安全推进
- assignment/version 持久化

### GroupAgent

`GroupAgent` 跑在每个节点本地，建议放在 `pkg/cluster/raftcluster` 下或专门的新包中。

职责：

- 定期向 controller 上报节点心跳
- 上报本地承载的 group 运行视图
- 接收或拉取 assignment
- 调用本地 `raftcluster` / `multiraft` 接口执行开组、关组、变更
- 把执行结果反馈给 controller

约束：

- agent 不自行做 peer 选择
- agent 不直接决定 rebalance
- agent 只执行 controller leader 已提交的目标

## 元数据模型

控制器需要独立的元数据表，不应继续复用 `ChannelRuntimeMeta`。

原因：

- 管理对象不同
- 生命周期不同
- controller 元数据是控制面 placement/membership 元数据，不是业务 channel 元数据

建议的核心模型如下。

### ClusterNode

- `NodeID`
- `Addr`
- `Status`
  - `Alive`
  - `Suspect`
  - `Dead`
  - `Draining`
- `LastHeartbeatAt`
- `CapacityWeight`

说明：

- `CapacityWeight` v1 可默认 `1`
- `Draining` 用于主动摘流

### GroupAssignment

- `GroupID`
- `DesiredPeers []NodeID`
- `ConfigEpoch`
- `BalanceVersion`

说明：

- 这是期望状态
- `ConfigEpoch` 用于 fence 旧命令和旧观察结果

### GroupRuntimeView

- `GroupID`
- `CurrentPeers []NodeID`
- `LeaderID`
- `HealthyVoters`
- `HasQuorum`
- `ObservedConfigEpoch`
- `LastReportAt`

说明：

- 这是观测状态
- 来源于 agent 周期性上报

### ReconcileTask

- `GroupID`
- `Kind`
  - `Bootstrap`
  - `Repair`
  - `Rebalance`
- `Step`
  - `AddLearner`
  - `CatchUp`
  - `Promote`
  - `TransferLeader`
  - `RemoveOld`
- `SourceNode`
- `TargetNode`
- `Attempt`
- `LastError`

说明：

- controller 不直接把大动作一次做完
- 所有迁移都拆成显式的单步任务
- `Bootstrap` 只用于业务 managed group 的首次创建

## controller 状态机输入

controller FSM 只接受两类输入：

### 1. 节点上报

- 节点心跳
- group runtime view
- 执行结果

### 2. controller leader 产生的命令

- 更新 assignment
- 推进 reconcile task
- 标记节点 `Alive/Dead/Draining`

这样可以保证：

- 期望状态与实际状态分离
- leader 切换后，新 leader 可以从持久化状态继续推进任务
- 重配流程具备幂等恢复能力

## 节点状态模型

节点状态建议采用：

- `Alive`
- `Suspect`
- `Dead`
- `Draining`

判定规则：

- 心跳正常：`Alive`
- 短时超时：`Suspect`
- 超过故障阈值：`Dead`
- 运维显式摘流：`Draining`

状态语义：

- `Suspect` 不立刻触发迁移
- `Dead` 才进入自动 repair
- `Draining` 不再承载新 group，并逐步迁出已有 groups

v1 不引入单独的 `Recovered` 状态：

- 故障节点恢复心跳后，重新回到 `Alive`
- `Draining` 节点只通过显式 `ResumeNode(nodeID)` 回到 `Alive`

## placement 与均衡策略

v1 采用固定副本数、固定节点权重的简单策略。

### 基本规则

- 每个 group 的目标副本数固定为 `ReplicaN`
- 选择副本时过滤掉 `Dead` 和 `Draining` 节点
- 先满足 repair，再考虑 rebalance
- rebalance 以“每节点承载的 group 数偏差”作为主要目标

### 选择原则

对某个需要补副本或迁移的 group：

1. 先排除当前 peers
2. 再排除 `Dead` 和 `Draining`
3. 在剩余 `Alive` 节点中选 group 数最少的节点
4. 如有并列，再按稳定排序规则打散

### 安全优先

controller 的调度优先级必须固定：

1. quorum 仍在的 repair
2. 节点 draining 导致的迁移
3. 节点恢复后的 rebalance

不能为了均衡去打断仍在修复的 group。

## 启动流程

### 当前流程

当前 `raftcluster.Cluster.Start()` 的关键语义是：

1. 创建 transport/server
2. 创建 `multiraft.Runtime`
3. 遍历静态 `cfg.Groups`
4. 逐个 `OpenGroup` 或 `BootstrapGroup`

### 目标流程

改造后建议变成：

1. 启动 transport/server
2. 启动 `multiraft.Runtime`
3. 若本地节点属于 controller bootstrap peers，则打开 controller group `0`
4. 若本地节点属于 controller bootstrap peers，则启动本地 controller replica service
5. 只有 controller leader 运行真正的 `GroupController` 决策逻辑
6. 每个节点都启动本地 `GroupAgent`
7. `GroupAgent` 向 controller leader 注册并上报心跳
8. `GroupAgent` 拉取 assignment
9. 本地动态打开需要承载的 managed groups

v1 的节点角色边界需要写死：

- controller bootstrap peers 才承载 controller Raft 副本，并有资格成为 controller leader
- 只有 controller leader 运行 placement / reconcile 决策逻辑
- 非 controller 节点只运行 `GroupAgent` 和轻量 controller client，不承载 controller 状态

## brand-new managed group 的首次形成流程

brand-new managed group 不能被 repair/rebalance 流程隐式代替，v1 需要单独的 bootstrap 语义。

当某个业务 `GroupID` 还没有任何 runtime view，也没有历史 assignment 时：

1. controller leader 先为该 group 计算第一版 `DesiredPeers`
2. controller leader 创建 `ReconcileTask{Kind: Bootstrap}`
3. 目标 peers 按 v1 placement 规则选出
4. 被选中的 peer agent 先确保本地 storage/state machine 句柄存在
5. 每个目标 peer 若本地没有该 group 的持久化 Raft 状态，则执行本地 `BootstrapGroup(groupID, voters)`
6. controller 观察到该 group 已形成并选出 leader 后，才把 bootstrap task 标记完成

职责边界是：

- controller leader 决定初始 peer 集和 bootstrap epoch
- controller leader 是首次 bootstrap 的唯一授权者，只有在它提交 `Bootstrap` task 后，peer agent 才能执行本地 bootstrap
- 目标 peer agent 在“本地状态为空”时执行 bootstrap
- 后续同一 group 的生命周期变化只再走 `Repair` / `Rebalance`，不再走 `Bootstrap`

## 安全重配流程

v1 只支持一条显式、安全的迁移序列。

### Repair 流程

场景：

- 目标副本数为 `3`
- 当前 group 仍有 quorum
- 其中一个 voter 节点已 `Dead`

执行序列：

1. controller 选择新的目标节点
2. `AddLearner(target)`
3. 等 learner catch up
4. `PromoteLearner(target)` 或 `AddVoter(target)`
5. 如 leader 恰好在待移除源节点上，则先 `TransferLeadership`
6. `RemoveVoter(source)`
7. 更新 `DesiredPeers`
8. 标记 task 完成

### Rebalance 流程

rebalance 复用完全相同的执行序列，只是触发原因不同：

- repair：缺副本或 dead node
- rebalance：负载偏差超阈值

### 硬约束

- 不做一次性大换血
- 每次任务只替换一个 peer
- 迁移过程中始终保证 group 可解释
- 在未完成 catch-up 前不移除旧 voter

## 失去 quorum 的语义

若 group 已经失去 quorum：

- controller 将其标记为 `Degraded`
- 停止自动 membership change
- 暴露告警与人工修复入口

v1 明确不做自动救援式重建。

原因：

- 失去多数后，没有安全依据证明哪一份日志是最新真相
- 自动替换剩余节点会引入分叉风险

因此，`多数在线继续运行` 和 `失去多数后自动恢复` 必须被视为两个独立问题。

## agent 与 controller 的职责边界

### controller 做什么

- 决定期望 peers
- 决定何时修复
- 决定何时均衡
- 记录任务进度
- 决定是否允许继续下一步

### agent 做什么

- 报告节点和 group 的本地事实
- 执行 controller 已批准的本地动作
- 返回结果和错误

### agent 不做什么

- 不自行选择目标节点
- 不自行做全局平衡
- 不自行在多个候选方案中做全局决策

## 运维接口

建议至少提供下列运维接口：

- `MarkNodeDraining(nodeID)`
- `ResumeNode(nodeID)`
- `ListGroupAssignments()`
- `ForceReconcile(groupID)`
- `TransferGroupLeader(groupID, nodeID)`
- `RecoverGroup(groupID, strategy)`

其中：

- `RecoverGroup` 只用于失去 quorum 的人工恢复路径
- 不进入 v1 自动流程

## 失败语义

### controller 不可用

- 已打开的业务 groups 继续按现状运行
- 禁止新的自动重配决策
- `GroupAgent` 继续上报，但不自行改配置

### 节点短暂抖动

- 先标记 `Suspect`
- 不立即触发迁移
- 超过阈值后再进入 `Dead`

### task 执行失败

- task 保留在 FSM 中
- 指数退避重试
- 超过重试阈值后进入人工关注状态

### leader 切换

- 新 controller leader 从持久化任务状态继续推进
- 旧 leader 未完成的单步动作必须幂等重放

## 配置演进

最终建议删除业务 groups 的静态 peer 配置，改成：

- `WK_CLUSTER_GROUP_COUNT`
- `WK_CLUSTER_NODES`
- `WK_CLUSTER_CONTROLLER_BOOTSTRAP`
- `WK_CLUSTER_GROUP_REPLICA_N`

其中：

- `WK_CLUSTER_NODES` 仍然静态声明集群成员
- `WK_CLUSTER_CONTROLLER_BOOTSTRAP` 只用于启动 controller group
- 普通业务 groups 不再手写 peers

### bootstrap controller 配置

v1 只有一个 bootstrap controller group，并保留一个保留 group id：

- controller bootstrap group id 固定为 `0`
- 业务 managed groups 仍然是 `1..GroupCount`

`WK_CLUSTER_CONTROLLER_BOOTSTRAP` 的配置形态建议为：

```json
{"group_id":0,"peers":[1,2,3]}
```

规则：

- `group_id` 必须固定为 `0`
- `peers` 不能为空
- `peers` 中的节点必须全部存在于 `WK_CLUSTER_NODES`
- `WK_CLUSTER_GROUP_COUNT` 只描述业务 managed groups
- 本地节点不要求一定属于 bootstrap controller peers；非 controller 节点也可以作为 managed-group worker 加入集群

### 启动校验替换规则

现有围绕静态 `cluster.groups` 的校验应替换为：

- `WK_CLUSTER_NODES` 必须存在且包含本地节点
- `WK_CLUSTER_GROUP_COUNT > 0`
- `WK_CLUSTER_GROUP_REPLICA_N > 0`
- `WK_CLUSTER_CONTROLLER_BOOTSTRAP` 必须存在且合法
- 不再要求静态业务 groups peer 列表

### 首次启动与重启语义

controller bootstrap group 的语义：

- 若本地已存在 group `0` 的持久化 Raft 状态，则直接打开
- 否则按 `WK_CLUSTER_CONTROLLER_BOOTSTRAP` 做 bootstrap

业务 managed groups 的语义：

- 不再从静态配置 bootstrap
- 等待 controller 下发 assignment 后，再按 controller 指令打开或 bootstrap

## 代码结构建议

建议新增：

- `pkg/cluster/groupcontroller`
- `pkg/storage/controllermeta`

建议改造：

- `pkg/cluster/raftcluster`
- `internal/app/config.go`
- `internal/app/build.go`

尽量少动：

- `pkg/replication/multiraft`
- 业务 `SlotForKey` 语义
- `metastore` 的业务分片逻辑

## 迁移路径

推荐按四阶段推进。

### 阶段 1：引入 controller，但不接管

- 保留现有静态 `WK_CLUSTER_GROUPS`
- 启动 bootstrap controller group
- controller 只收集心跳和运行视图

### 阶段 2：只读建议模式

- controller 计算建议 assignment
- 不下发真实变更
- 验证 placement 计算和观测一致性

### 阶段 3：repair-only

- 允许 controller 在节点 `Dead` 时做补副本
- 禁止自动 rebalance

### 阶段 4：开启 rebalance

- 节点恢复或新增后自动平衡
- 最终删除业务 groups 的静态 peers 配置

## 测试策略

### 单元测试

- placement 计算
- repair/rebalance 触发条件
- 节点状态转换
- task 幂等推进

### FSM 测试

- 心跳超时
- `Suspect -> Dead -> Alive`
- `Draining -> Alive` through `ResumeNode`
- assignment 版本推进

### 多节点集成测试

- 3 节点 3 副本，挂 1 节点，多数仍可用
- 故障后自动补齐到新节点
- 节点恢复后自动均衡
- draining 时平滑迁出

### 故障测试

- controller leader 切换中 task 不丢
- agent 执行中节点重启
- 迁移过程中 leader 位于待迁出节点

## v1 明确不做的事情

- 失去 quorum 后自动恢复
- 动态调整 `GroupCount`
- 自动发现和自动移除 cluster nodes
- 基于磁盘/CPU/流量的复杂容量模型
- 跨多个 groups 的批量联动迁移事务

## 结论

推荐把控制面 `Group` 自动管理实现为：

- 固定 `GroupCount`
- 固定 `ReplicaN`
- 静态节点成员
- 单一 `GroupController`
- 本地 `GroupAgent` 执行
- 安全的单步 membership change
- repair 优先，rebalance 次之
- quorum-first，degraded 时人工恢复

这条路线能最大限度复用现有 `multiraft` 和 `raftcluster` 执行能力，同时把真正缺失的 placement controller 补上，而不是推翻整个集群层。
