# Conversation Sync Design

## Overview

为当前 `WuKongIM v3` 主仓设计一套高性能、可扩展的最近会话同步方案，并实现 `/conversation/sync`。方案需要满足以下约束：

- 保持消息发送热路径极简，不在每条消息发送时同步写 per-user conversation 状态
- 会话同步 correctness 不能依赖内存态或单一加速索引
- 兼容旧版会话项字段语义，尤其是 `channel_id`、`channel_type`、`unread`、`last_msg_seq`、`last_client_msg_no`、`readed_to_msg_seq`、`recents`
- 放弃旧版 `page/page_size`，改用更适合动态会话列表的 cursor 分页
- 支持“冷会话突然有新消息”的情况，不允许因此丢会话

该设计遵循当前仓库分层约定：

- `internal/access/api` 只做 HTTP 兼容和协议映射
- `internal/usecase/conversation` 负责同步编排
- `internal/app` 负责装配 projector、cache、usecase、API
- 持久化状态落在 `pkg/storage/metadb` / `pkg/storage/metastore`
- 消息事实源继续由 `pkg/storage/channellog` 提供

## Goals

- 提供正式的 `conversation` usecase，而不是继续把逻辑堆在 API handler
- 为 `/conversation/sync` 建立可靠的数据边界和性能边界
- 默认同步只关注用户当前工作集，而不是历史全量会话全集
- 在不污染消息热路径的前提下，解决冷会话重新活跃后的发现问题
- 让未来的 `clearUnread`、`delete`、会话搜索、归档同步都能复用同一数据模型

## Non-Goals

- 本次不实现会话搜索、归档会话浏览、置顶、免打扰、草稿等扩展能力
- 本次不实现旧版 `page/page_size` 兼容
- 本次不把 `sync` 做成严格事务快照浏览；cursor 只保证一次同步过程内的稳定 seek 语义
- 本次不在消息提交热路径同步写 `metastore` conversation 索引
- 本次不把 `/conversation/sync` 设计成“历史全量会话目录枚举”接口；它是工作集同步接口

## Existing Constraints

- 当前主仓还没有正式的 `conversation` usecase
- 当前 `message` 用例在消息提交后已经有异步 committed dispatcher 挂点，可用于 conversation projector
- `channellog` 已能提供 committed seq、最后消息、最近消息窗口
- `metastore` 已具备 authoritative slot 路由、RPC 透传、subscriber 快照能力
- 用户角度的会话同步必须以“用户 owner 节点”为聚合入口
- 单节点部署统一视为“单节点集群”

## Problem Framing

最近会话同步同时要解决三个不同问题：

1. `catalog correctness`
   用户有哪些会话候选频道，不能丢会话
2. `message facts`
   某个频道当前的最后消息、`last_msg_seq`、`recents`、未读如何计算
3. `working set performance`
   默认 sync 不应每次都扫描用户全部历史目录，也不应扫描全局全部频道更新

如果只依赖 `channellog`，会丢“用户有哪些频道”的目录信息。  
如果只依赖用户目录全量扫描，大用户 sync 成本会失控。  
如果每条消息都写 per-user conversation index，又会破坏消息热路径吞吐。

因此需要把 correctness 和 acceleration 分离：

- 真相层：`UserConversationState + channellog`
- 加速层：`UserConversationState.updated_at + ChannelUpdateLog`

## Recommended Approach

采用两类核心数据：

1. `UserConversationState`
   用户全量目录和低频状态真相，同时承载最近会话候选索引列 `updated_at`
2. `ChannelUpdateLog`
   频道级更新索引，分为 owner 节点内存 hot index 和 metastore cold index

消息发送热路径只做：

- `channellog.Append`
- 异步提交 committed message 给 projector

不在发送同步链路中写 `metastore` 会话状态。

## Architecture

### 1. API Adapter

`internal/access/api/conversation_sync.go`

职责：

- 解析 `/conversation/sync` 请求
- 兼容旧版会话项字段语义
- 把 cursor 协议映射为 usecase query
- 输出新的顶层响应结构

### 2. Conversation Usecase

`internal/usecase/conversation`

职责：

- 聚合用户目录、最近激活状态、更新索引、消息事实源
- 完成候选集合裁剪
- 计算 unread、recent messages、cursor
- 对单聊 internal channel ID 与外部 `channel_id` 做双向映射

### 3. Async Projector

projector 运行在 committed message 异步路径上。

职责：

- 更新 owner 节点内存 hot `ChannelUpdateLog`
- 周期性 flush hot index 到 cold `ChannelUpdateLog`
- 判断频道是否发生 `cold -> hot` 切换
- 对冷会话触发一次异步 `UserConversationState.updated_at` 激活 fanout

它是加速器和冷唤醒器，不是 correctness 真相源。

### 4. Storage

#### UserConversationState

按 `uid` authoritative 存储：

- `uid`
- `channel_id`
- `channel_type`
- `read_seq`
- `deleted_to_seq`
- `updated_at`

职责：

- 记录这个用户“知道哪些会话”
- 记录低频状态：已读位置、逻辑删除位置
- 为默认 sync 提供“最近会话”候选索引

状态字段更新时机：

- 单聊首次建档
- 群订阅加入 / 移除
- clear unread / set unread / read
- delete conversation

`updated_at` 推进时机：

- 用户打开会话
- 用户显式已读 / clear unread
- 冷会话重新活跃
- 单聊首次建档
- 可选的显式兴趣动作（本次不做）

本次不做“sync 命中后自动续期”。

读取方式：

- 按 `(uid, updated_at desc)` 读取最近会话
- 默认只取前若干条作为候选，不扫描用户全部历史目录
- `updated_at` 只表示“最近一次影响默认 sync 候选集的业务时间”，不承担最终会话排序语义

注意：

- `UserConversationState.updated_at` 是业务字段，不是泛化的“行最后修改时间”
- `delete conversation`、纯后台修复等状态修改不应自动推进 `updated_at`
- 可选的后台冷降级任务只允许把 `updated_at` 置为 `0` 或降到更早的业务时间，用于移出默认 working set；不得删除 `UserConversationState` 行
- 冷降级必须异步执行，不得放在 `/conversation/sync` 主查询链路里逐条 join 后同步回写
- `UserConversationState` 仍是 correctness 主表，不能为了 working set 裁剪而删除旧会话行
- 性能边界依赖索引 seek 读取，而不是独立 working set 表的垃圾回收

#### ChannelUpdateLog

频道 owner 侧的更新索引，分两层：

- hot index：内存覆盖表
- cold index：持久化 metastore 表

字段：

- `channel_id`
- `channel_type`
- `updated_at`
- `last_msg_seq`
- `last_client_msg_no`
- `last_msg_at`

职责：

- 为 `version` 驱动的增量 sync 提供最近变更频道候选
- 为默认排序提供轻量索引

不是消息事实源，也不是 correctness 必需唯一来源。

## Data Ownership

- `UserConversationState`
  由 `uid` 所属 owner 节点 authoritative
- `ChannelUpdateLog`
  由 `channel_id` 所属 owner 节点 authoritative
- `channellog`
  仍由频道 owner 提供消息事实

`/conversation/sync` 必须先路由到 `uid owner`，再由 usecase 按需向不同频道 owner 拉取 hot/cold update info 和消息事实。

## Working Set Strategy

默认 sync 不面向“用户全部历史目录”，而面向工作集。

工作集候选由两类数据组成：

1. `recently activated working set`
   来自 `UserConversationState.updated_at`，按 `updated_at desc` 读取前若干条
2. `client known channels`
   来自客户端 `last_msg_seqs`

再叠加一类增量候选：

3. `recent channel updates`
   先由 `version` 命中 hot/cold `ChannelUpdateLog`，再与用户 membership 交叉

最终候选集：

`candidate = recently activated working set ∪ client known ∪ recent updates for this user`

工作集读取边界：

- `UserConversationState` 必须基于 `(uid, updated_at desc)` 索引 seek 读取
- 读取时必须过滤 `updated_at > 0`
- 单次 sync 默认只读取前 `working_set_scan_limit` 条最近激活记录
- `working_set_scan_limit` 应明显大于请求 `limit`，用于吸收 `exclude_channel_types` 和 `only_unread` 过滤后的损耗
- 推荐默认值：`min(max(limit * 4, 128), 512)`

这样：

- 几个月 / 几年无消息、用户也没显式兴趣的会话，不默认进入 sync
- 冷会话突然活跃，会通过异步更新 `UserConversationState.updated_at` 被重新纳入下一次 sync
- 首次新设备或客户端重装后，客户端显式已知频道仍可补集回来

### Bootstrap Contract

当客户端是首次同步，或本地状态丢失时，可能出现：

- `version == 0`
- `last_msg_seqs` 为空
- `cursor` 为空

本接口在这种情况下的契约是：

- `/conversation/sync` 仍然只返回“服务端工作集 + 最近变化”视角下的会话
- 它不会退化成用户全量历史目录扫描接口
- 冷历史会话如果既不在 working set、也不在最近变化窗口内，则本次不会返回

这不是错误，而是设计选择。原因是：

- 该接口优先服务最近会话首页和增量同步
- 全量历史目录枚举会显著破坏默认 sync 的查询边界

为了保证 brand-new device 仍能拿到有意义的数据，设计依赖三点：

- `UserConversationState.updated_at` 是持久化的跨设备最近会话提示，而不是客户端本地缓存
- brand-new direct chat 通过首次建档推进 `updated_at`
- 冷会话重新活跃时，通过异步 bump `UserConversationState.updated_at` 重新进入候选集合

后续如果需要“浏览全部历史会话目录”，应通过单独的 archive/search 接口解决，而不是扩展本接口语义。

## Cold Wakeup Strategy

当频道 owner projector 处理 committed message 时：

1. 更新 hot `ChannelUpdateLog`
2. 判断该频道是否从冷状态切到热状态

建议冷状态判定：

- `ChannelUpdateLog` 不存在，或其 `last_msg_at <= now - cold_threshold`
- 这条消息是重新活跃后的第一条 committed message

如果只是普通热频道：

- 不做任何 per-user fanout

如果是 `cold -> hot`：

- 单聊：异步为双方 ensure `UserConversationState` 存在，并把 `updated_at` 推进到 `message_time`
- 群聊：异步按 subscriber 快照分页展开成员，再批量推进 `UserConversationState.updated_at=message_time`

该 fanout 是 rare path，且只在冷转热发生一次，不在后续连续热消息中重复。

## ChannelUpdateLog Update Strategy

消息发送同步热路径不写 `ChannelUpdateLog` 持久层。

projector 的更新策略：

- committed message 到达 owner 节点后，hot index 覆盖最新 entry
- flush 触发条件：
  - 周期定时，例如 `100ms ~ 500ms`
  - 脏键数量阈值，例如 `1024`
  - 优雅停机 best-effort flush

同一频道连续 100 条消息，只在 flush 时落一次 cold `ChannelUpdateLog`。

查询顺序：

1. 先查 owner 内存 hot index
2. hot 未命中再查 cold `ChannelUpdateLog`
3. 最终消息事实仍以 `channellog` 为准

hot/cold index 缺失或滞后只会让 sync 变慢，不得导致错误结果或丢会话。

### Hot Window Cleanup

`ChannelUpdateLog` 只保留 hot channel，不承担历史全量索引职责。

规则：

- 当 `last_msg_at <= now - cold_threshold` 时，该 entry 视为冷数据
- 冷数据在读取时按未命中处理
- 冷数据由后台 janitor 或 flush 附带清理异步删除，不在消息热路径和 sync 读路径同步删除

因此：

- 当 `version > 0` 时，增量候选只看 `version` 之后且仍处于 hot window 的频道更新
- 当 `version == 0` 时，brand-new sync 只看当前 hot window 内的频道更新

超过该窗口的冷历史会话不会通过 `ChannelUpdateLog` 命中，只能依赖：

- `UserConversationState.updated_at`
- 客户端显式 `last_msg_seqs`

## Sync API

### Request

保留旧版主要语义字段，去掉 `page/page_size`，改为 cursor：

- `uid`
- `version`
- `sync_version`
- `last_msg_seqs`
- `msg_count`
- `only_unread`
- `exclude_channel_types`
- `limit`
- `cursor`

说明：

- `version`
  表示客户端已持久化的上一次完整 sync 下界，只看此后变化
- `sync_version`
  表示当前这一轮 seek 分页的服务端上界；首次请求不传，后续翻页必须原样带回
- `last_msg_seqs`
  保持旧版字符串协议：`channel_id:channel_type:last_msg_seq|channel_id:channel_type:last_msg_seq`
- `last_msg_seqs`
  用于补客户端已知频道集合，并帮助快速跳过未变化频道；空字符串表示客户端无已知频道
- `msg_count`
  表示每个会话 `recents` 的窗口大小；`msg_count <= 0` 时不加载 `recents`
- `cursor`
  为不透明字符串，建议实现为 base64url(JSON)

`cursor` 至少需要编码以下字段：

- `sort_updated_at`
  上一条返回记录的 `effective_updated_at`
- `channel_type`
  上一条返回记录的 `channel_type`
- `channel_id`
  上一条返回记录的 `channel_id`
- `filter_hash`
  当前请求中 `only_unread + exclude_channel_types + limit + version` 的稳定摘要
- `sync_version`
  当前分页轮次对应的服务端上界

### Response

顶层返回：

- `conversations`
- `next_cursor`
- `has_more`
- `sync_version`

`sync_version` 的精确定义：

- 由 `uid owner` 在一次新的 sync 开始时生成的服务端时间上界，单位为 UnixNano
- 表示这次 sync 的候选与排序只观察 `updated_at <= sync_version` 的数据窗口
- 它不是请求 `version` 的 echo，也不是消息 `last_msg_seq`

客户端语义：

- 同一轮继续翻页时：把同一个 `sync_version` 原样带回
- 一次新的 sync 结束后：把最终得到的 `sync_version` 持久化，并在下一次新的 sync 中作为 `version` 发送

会话项字段保持旧版语义：

- `channel_id`
- `channel_type`
- `unread`
- `timestamp`
- `last_msg_seq`
- `last_client_msg_no`
- `offset_msg_seq`
- `readed_to_msg_seq`
- `version`
- `recents`

`offset_msg_seq` 本次固定返回 `0`。

### Error Contract

延续当前 API 的简单错误风格，参数或分页状态非法时返回：

- HTTP `400`
- 响应体：`{"error":"..."}`

固定错误语义：

- `{"error":"invalid cursor"}`
  cursor 解析失败、缺字段、签名/编码不合法
- `{"error":"sync_version required"}`
  `cursor` 非空但请求未携带 `sync_version`
- `{"error":"sync_version mismatch"}`
  请求 `sync_version` 与 cursor 内编码值不一致
- `{"error":"cursor filter mismatch"}`
  翻页请求修改了 `only_unread`、`exclude_channel_types`、`limit` 或 `version`
- `{"error":"invalid sync request"}`
  `cursor` 为空但携带 `sync_version`，或其他明显非法组合

## Sync Algorithm

### New Sync

当 `cursor` 为空时：

1. 路由到 `uid owner`
2. 若请求未携带 `sync_version`，服务端生成本次 `sync_version = now_unix_nano`
3. 若请求携带 `sync_version` 但 `cursor` 为空，则视为非法组合
4. 解析 `last_msg_seqs`
5. 拉取：
   - `UserConversationState(updated_at > 0 order by updated_at desc limit working_set_scan_limit)`
   - client known channels
6. 若 `version > 0`：
   - 通过 hot/cold `ChannelUpdateLog` 查询最近变化频道
   - 再用用户 membership 交叉筛选
7. 若 `version == 0` 且 `last_msg_seqs` 为空：
   - 不做用户全量目录扫描
   - 仅以 `working set + recent updates within hot window` 作为候选来源
8. 合并候选集合并去重
9. 先按 `exclude_channel_types` 过滤候选集合
10. 对过滤后的候选集合读取最小消息事实，至少包括：
   - `read_seq`
   - `last_msg_seq`
   - last message metadata
11. 基于消息事实计算 `unread`
12. 在完成 unread 计算后应用 `only_unread`
13. 对剩余候选按排序键计算 seek 顺序，且只观察 `updated_at <= sync_version` 的记录
14. 取前 `limit` 个频道作为当前页
15. 只对本页频道读取：
   - `channellog.Status`
   - last message
   - recent messages
   - 用户目录状态
16. `msg_count` 仅决定每个会话 `recents` 的窗口大小，不影响候选选择和排序
17. 组装响应，并回传本次 `sync_version`
18. 生成 `next_cursor`

### Continue Sync

当 `cursor` 非空时：

1. 校验 cursor
2. 请求必须携带首次响应返回的 `sync_version`
3. 在相同 filter 条件下继续 seek 下一个窗口，且只观察 `updated_at <= sync_version` 的数据
4. 先按 `exclude_channel_types` 过滤 cursor 之后的候选
5. 读取最小消息事实并计算 unread
6. 应用 `only_unread`
7. 取满 `limit`
8. 只读取当前页所需频道的 `recents`
9. 返回下一页和新的 `next_cursor`，并原样回传同一个 `sync_version`

本次不实现服务端 snapshot view 缓存；cursor 基于稳定排序键 seek。

### Optional Cold Demotion

为了控制 `UserConversationState.updated_at` 候选集规模，可以实现后台冷降级任务：

- 输入：按批扫描 `updated_at > 0` 的用户会话
- 判断：若关联 `ChannelUpdateLog.last_msg_at <= now - cold_threshold`
- 动作：只把 `UserConversationState.updated_at` 清零或降级
- 禁止：删除 `UserConversationState` 行，或在 `sync` 主链路里同步执行降级

## Ordering

`UserConversationState.updated_at` 仅用于：

- 高性能读取最近会话候选集
- 控制默认 sync 的候选读取边界

它不直接决定 `/conversation/sync` 返回结果的最终顺序；最终顺序仍按频道消息更新时间决定。

排序键固定为：

- `effective_updated_at desc`
- `channel_type asc`
- `channel_id asc`

`effective_updated_at` 优先级：

1. hot `ChannelUpdateLog.updated_at`
2. cold `ChannelUpdateLog.updated_at`
3. last message timestamp

这能保证 seek 稳定，不依赖 offset 分页。

## Unread Calculation

基础计算：

- `unread = max(0, last_msg_seq - read_seq)`

其中：

- `read_seq` 来自 `UserConversationState.read_seq`
- `deleted_to_seq` 会抬高 recent message 窗口的下界

单聊兼容语义：

- 返回 `channel_id` 时使用对端 uid，而不是内部归一化 person channel ID

“自己发送的最新消息不算未读”的兼容规则在查询侧处理：

- 如果最后一条 recent message 的 `from_uid == uid`，则把
  - `readed_to_msg_seq = last_msg_seq`
  - `unread = 0`

本次不在消息热路径持久化 sender ack position。

## Filter and Window Order

为避免 planning 和实现时出现歧义，过滤与窗口顺序固定为：

1. 构造原始候选集合
2. 应用 `exclude_channel_types`
3. 读取最小消息事实并计算 `unread`
4. 应用 `only_unread`
5. 按排序键排序
6. 应用 `cursor`
7. 截取 `limit`
8. 仅对当前页加载 `recents`

`msg_count` 的语义固定为：

- 每个会话返回的 `recents` 条数上限
- 不参与候选集合裁剪
- 不参与排序
- 不参与 cursor 计算

## Failure and Fallback Behavior

- hot `ChannelUpdateLog` 查询失败：回退 cold index
- cold index 未命中：回退到 `channellog` 读取事实
- working set 缺失：回退 client known + 用户目录精确读取
- 冷会话激活 fanout 延迟：最多导致冷会话重新出现稍慢，不得丢失已存在目录项

整体原则：

- correctness 总能回退到 `UserConversationState + channellog`
- hot/cold index 只是性能优化

## Proposed Package Layout

- `internal/access/api/conversation_sync.go`
- `internal/access/api/conversation_legacy_model.go`
- `internal/usecase/conversation/app.go`
- `internal/usecase/conversation/deps.go`
- `internal/usecase/conversation/types.go`
- `internal/usecase/conversation/sync.go`
- `internal/usecase/conversation/cursor.go`
- `internal/usecase/conversation/projector.go`
- `pkg/storage/metadb/user_conversation_state.go`
- `pkg/storage/metadb/channel_update_log.go`
- `pkg/storage/metastore/user_conversation_state_rpc.go`
- `pkg/storage/metastore/channel_update_log_rpc.go`

## Integration Points

### Message Send

在 committed dispatcher 路径上接 conversation projector。

### Subscriber Changes

群订阅加入/移除时，维护：

- `UserConversationState`
- 加入时可按需要推进 `updated_at`

### Direct Message Bootstrap

单聊首次出现时，为双方确保：

- `UserConversationState`（包含初始 `updated_at`）

该操作是 pair 首次建档，不是每条消息写。

## Testing Strategy

### Storage Tests

- `UserConversationState` 幂等建档、读写、逻辑删除、read_seq 更新、`updated_at` 排序读取、authoritative remote slot 读取
- `ChannelUpdateLog` hot 覆盖、flush 合并、cold 读取

### Usecase Tests

- working set 命中时不扫描全量目录
- cold wakeup 会通过更新 `UserConversationState.updated_at` 把冷会话带回下次 sync
- 客户端 `last_msg_seqs` 能补齐 server 目录之外的已知频道
- `only_unread` 过滤正确
- 单聊 `channel_id` 映射正确
- cursor seek 稳定且不重复
- hot/cold update index 缺失时能回退

### API Tests

- 请求参数兼容旧版主要字段
- 顶层响应为新结构，单会话字段语义与旧版对齐
- 单节点集群和多节点集群都能路由到 `uid owner`

## Open Decisions Resolved

- 不在消息热路径同步写 per-user conversation 索引
- `page/page_size` 不兼容，改用 cursor
- 不在 `UserConversationState` 主表中加入 `last_interest_at`
- 不单独引入 `UserConversationWorkingSet` 表，最近会话索引直接落在 `UserConversationState.updated_at`
- 不做“sync 命中自动续期 working set”
- `ChannelUpdateLog` 采用 hot memory + cold persistence 结构
- `UserConversationState.updated_at` 承担最近会话业务时间语义，而不是泛化行更新时间
- 冷会话重新活跃通过异步更新 `UserConversationState.updated_at` 进入候选集合

## Implementation Guidance

实现顺序建议：

1. 扩展 `UserConversationState` 并落 `ChannelUpdateLog`
2. 落 `internal/usecase/conversation` 的 sync 编排与 cursor
3. 接入 API `/conversation/sync`
4. 接入 message committed projector 和 hot/cold `ChannelUpdateLog`
5. 接入冷会话激活 fanout
6. 补 integration tests

先实现 correctness 路径，再叠加 hot/cold index 优化；任何阶段都必须保留回退到 `UserConversationState + channellog` 的能力。
