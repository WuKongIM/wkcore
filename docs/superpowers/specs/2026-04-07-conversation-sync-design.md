# Conversation Sync Design

## Goal

为当前 WuKongIM v3 主仓设计并落地高性能会话管理，同时实现兼容旧版 `/conversation/sync` 语义的会话同步能力。设计目标必须满足：

- 消息发送热路径不引入 per-user 会话持久化写放大
- 默认会话同步只处理用户真实关心的工作集
- 冷会话重新活跃时不能丢会话
- 当前 HTTP 接口兼容旧版请求参数与单条会话返回字段语义
- 保持当前仓库的分层约束：`access -> usecase/runtime`，组合根只在 `internal/app`

## Problem

会话同步同时面临三个约束：

1. 如果每条消息都同步写用户会话，会把发送链路拖回 O(N) fanout。
2. 如果完全只在查询时计算，又会遇到两类问题：
   - 用户会话目录过大，`sync` 扫描成本不可控
   - 冷会话突然来消息时，若没有额外机制，用户可能收不到该会话
3. 最近会话按时间排序，传统 `page/page_size` 在实时列表里不稳定。

因此系统必须把“正确性真相”“热路径加速”“冷会话唤醒”拆开处理。

## Design Summary

### 1. UserConversationState

这是唯一的用户侧正式目录与低频状态表，按用户 authoritative slot 存储。

字段：

- `uid`
- `channel_id`
- `channel_type`
- `read_seq`
- `deleted_to_seq`
- `last_interest_at`
- `updated_at`

职责：

- 表示用户可见的会话目录，不存最后消息、不存未读快照
- 记录已读位点与逻辑删除位点
- 提供“工作集”索引：用户最近仍然关心的会话

写入时机：

- 单聊 pair 首次建立目录
- 群成员加入/退出
- 用户主动已读
- 用户删除会话
- 会话在 sync 中命中返回时更新 `last_interest_at`

### 2. ChannelUpdateLog

这是频道侧更新加速索引，不是正确性的唯一来源。

字段：

- `channel_id`
- `channel_type`
- `updated_at`
- `last_msg_seq`
- `last_client_msg_no`
- `last_msg_at`

存储结构分两层：

- `hot index`：频道 owner 节点内存中的最新覆盖表
- `cold index`：定时 flush 到 metastore 的持久化索引

更新原则：

- 消息提交成功后，仅异步更新内存 `hot index`
- 周期性或阈值触发 flush，把每个频道当前最新状态写入 `cold index`
- 连续 100 条消息只需要 1 次落库

正确性原则：

- `ChannelUpdateLog` 只用于加速筛选
- 缺失、延迟、重建都不能导致错误结果或丢会话

### 3. UserConversationWakeup

这是冷会话重新活跃时的唤醒表，用于解决“冷会话突然来消息”的问题。

字段：

- `uid`
- `channel_id`
- `channel_type`
- `wakeup_at`
- `last_msg_seq`
- `expire_at`

职责：

- 只在频道从冷状态切到热状态时异步写入
- 把冷频道重新推入相关用户的工作集
- 同一频道后续持续活跃时不重复 fanout

### 4. channellog

`channellog` 仍然是频道消息事实源：

- `committed seq`
- 最后一条消息
- recent messages

最终返回给客户端的：

- `last_msg_seq`
- `last_client_msg_no`
- `timestamp`
- `recents`
- `unread`

都必须最终以 `channellog` 为准。

## Work Set Strategy

默认 `/conversation/sync` 不面向用户全量历史会话全集工作，而面向用户当前工作集工作。

候选会话集合由以下三部分并集组成：

1. `active working set`
   来自 `UserConversationState` 中最近 `last_interest_at` 窗口内的目录项
2. `pending wakeups`
   来自 `UserConversationWakeup`
3. `client known channels`
   来自客户端 `last_msg_seqs`

对于增量同步，再由 `ChannelUpdateLog` 做候选裁剪：

- `hot index` 命中优先
- 再查 `cold index`
- 都未命中则回退到目录项 + `channellog` 读时计算

这样可以做到：

- 默认 sync 不扫用户历史全集
- 冷频道被重新激活时可快速进入候选集
- 热频道增量同步仍然很快

## Cold Wakeup

冷频道突然来消息时，不能要求默认 sync 扫全量目录去发现它。

解决方案：

1. 消息 append 成功后，通过已有 committed dispatcher 异步通知 conversation projector
2. projector 在频道 owner 节点更新 `hot ChannelUpdateLog`
3. 若判断该频道发生 `cold -> hot` 切换，则异步创建 wakeup 任务
4. 后台 worker 展开成员并批量写 `UserConversationWakeup`

判断 `cold -> hot` 的标准由 `cold_threshold` 控制，例如 30 天。

特性：

- 普通热频道消息不做 per-user fanout
- 只有冷频道重新活跃时才做一次异步 O(N) fanout
- 这是 rare path，不污染热路径

## API Contract

### Request

`/conversation/sync` 兼容旧版主要字段：

- `uid`
- `version`
- `last_msg_seqs`
- `msg_count`
- `only_unread`
- `exclude_channel_types`

分页协议升级为更稳定的 cursor 方案：

- `limit`
- `cursor`
- `sync_version`

不再兼容 `page/page_size`。

### Response

顶层返回对象：

- `conversations`
- `next_cursor`
- `has_more`
- `sync_version`

单条会话字段与旧版尽量对齐：

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

说明：

- `offset_msg_seq` 固定返回 `0`
- `version` 表示该会话记录的服务端同步版本

### Pagination

使用 bounded keyset cursor：

- `sync_version` 作为本次同步视图的上界
- `cursor` 编码上一次返回最后一条记录的排序位置
- 排序键固定为：
  - `effective_updated_at desc`
  - `channel_type asc`
  - `channel_id asc`

不维护 server-side snapshot view，降低复杂度。

## Sync Algorithm

首次请求：

1. 路由到 `uid` owner 节点
2. 生成 `sync_version`
3. 从三类来源构建候选频道并集：
   - active working set
   - pending wakeups
   - client known channels
4. 用 `ChannelUpdateLog` 对增量请求做变化筛选
5. 对候选频道读取：
   - `channellog.Status`
   - last message
   - recents
6. 过滤：
   - `exclude_channel_types`
   - `only_unread`
   - `deleted_to_seq`
7. 排序并返回 `limit` 条
8. 返回 `next_cursor` 与 `sync_version`

后续翻页：

1. 客户端带回相同 `sync_version`
2. 继续按 `cursor` 之后的位置 seek
3. 不让新消息插入当前翻页过程

## Direct Chat Bootstrap

单聊 brand-new pair 如果不建立目录，会导致离线接收者丢会话。

因此需要一个最小化补偿：

- person channel 首条消息建立时
- 为双方各执行一次 `EnsureUserConversation`
- 每个 pair 最多发生一次，不是每条消息都写

这部分不属于 per-message O(N) fanout，成本可接受。

## Runtime Integration

会话 projector 接入点使用现有 committed dispatcher 链路：

- 消息 durable append 成功
- committed dispatcher 异步通知
- conversation projector 更新：
  - `hot ChannelUpdateLog`
  - `cold -> hot` 检测
  - wakeup 任务

这样不会改变发送热路径的同步持久化成本。

## Package Layout

- `internal/access/api/conversation_sync.go`
- `internal/access/api/conversation_legacy_model.go`
- `internal/access/node/conversation_hot_rpc.go`
- `internal/usecase/conversation/app.go`
- `internal/usecase/conversation/deps.go`
- `internal/usecase/conversation/types.go`
- `internal/usecase/conversation/sync.go`
- `internal/usecase/conversation/projector.go`
- `internal/usecase/conversation/wakeup.go`
- `internal/usecase/conversation/direct_bootstrap.go`
- `pkg/storage/metadb/user_conversation.go`
- `pkg/storage/metadb/channel_update_log.go`
- `pkg/storage/metadb/user_conversation_wakeup.go`
- `pkg/storage/metastore/user_conversation_rpc.go`
- `pkg/storage/metastore/channel_update_log_rpc.go`
- `pkg/storage/metastore/user_conversation_wakeup_rpc.go`

## Testing Strategy

### Storage

- `UserConversationState` 幂等 ensure
- `read_seq` 与 `deleted_to_seq` 单调更新
- `ChannelUpdateLog` 覆盖更新与范围读取正确
- `UserConversationWakeup` upsert + TTL 语义正确

### Usecase

- 增量 sync 只返回变更会话
- `only_unread` 过滤正确
- 单聊 real/fake channel id 转换正确
- 冷会话 wakeup 后可被 sync 命中
- `ChannelUpdateLog` 缺失时回退到 `channellog` 仍正确

### API

- 请求参数兼容旧版核心字段
- 顶层返回新 cursor 协议
- 单条会话字段与旧版对齐
- person channel brand-new pair 不丢会话

## Tradeoffs

优点：

- 热路径无 per-user conversation 同步写
- 热会话 sync 快
- 冷会话重活跃不丢
- 分层清晰，和当前仓库结构一致

代价：

- 设计比纯读时计算更复杂
- 需要额外 projector / wakeup worker
- correctness 与 acceleration 明确拆层，测试面更广

这是为了同时满足性能、正确性和可维护性所需的复杂度。
