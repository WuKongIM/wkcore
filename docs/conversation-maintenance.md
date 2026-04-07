# WuKongIM v3 最近会话维护设计

## 一、问题本质

读扩散 IM 中，消息存储在频道维度（一条消息只存一份）。但"最近会话"是用户维度——每个用户需要知道哪些频道有新消息、未读多少条。

### 写放大问题

```
  万人群发 1 条消息:

  消息存储:   1 次写入 (存到 channel 存储)     ← 很轻
  会话更新: 10000 次写入 (每个成员的会话列表)   ← 灾难

  如果群内每秒 10 条消息:
    消息写入:  10 次/秒
    会话写入:  10 × 10000 = 100,000 次/秒      ← 单个群就把存储打爆

  100 个万人群:
    会话写入:  10,000,000 次/秒                 ← 不可能
```

### 三种方案的本质区别

```
  方案 A: 朴素方案
    每条消息 → 同步写 N 个用户的会话记录
    写入量: O(N) per message，在热路径上
    延迟: 高，直接影响消息发送速度

  方案 B: CoalesceBuffer 合并方案
    每条消息 → 标记 dirty → 异步批量刷盘
    写入量: O(N) per flush，但合并了时间窗口内的多条消息
    万人群 50 条/秒 → 依然 10000 次写入/flush
    问题: 本质还是 O(N)，只是减小了常数

  方案 C: 零写入方案 ← 本文最终方案
    每条消息 → 只写 channel.maxSeq（1 次）
    用户的会话状态完全在查询时计算
    写入量: O(1) per message，与成员数无关
    万人群、十万人群、百万人频道 → 都是 1 次写入
```

---

## 二、Telegram 怎么做的

Telegram 对不同规模的频道使用不同策略：

### 私聊 / 小群 (< 200 人)

- 使用 per-user 更新流（pts 模型）
- 每条消息为每个成员创建一条更新记录
- 写放大 = 成员数，但小群可以接受

### Supergroup / Channel (大群、频道)

- **服务端不写任何 per-user 状态**
- 频道自身维护一个 `channel_pts`（消息序号）
- 客户端自己跟踪每个频道读到了哪里
- 同步时客户端主动请求：`getChannelDifference(channel, my_pts)`
- 未读数由客户端计算

### 核心思想

```
  Telegram 的关键洞察:

  ┌─────────────────────────────────────────────────────┐
  │                                                     │
  │  消息到达时:                                         │
  │    服务端只更新频道状态（1 次写入）                    │
  │    不触碰任何用户的数据                               │
  │                                                     │
  │  用户需要会话列表时:                                  │
  │    由客户端主动查询                                   │
  │    服务端按需计算、按需返回                           │
  │                                                     │
  │  "不写" 才是最快的写                                  │
  │                                                     │
  └─────────────────────────────────────────────────────┘
```

---

## 三、零写入方案设计

### 数据模型

服务端只维护三张"源数据"表，它们各自独立更新，互不干扰：

```
  ┌──────────────────────────────────────────────────────────┐
  │  表 1: ChannelState                                      │
  │  写入时机: 每条消息到达时（1 次）                           │
  │                                                          │
  │  channelKey → {maxSeq, lastMsgID, lastMsgAt}             │
  │                                                          │
  │  group1  →  {maxSeq: 501, lastMsgAt: 2025-01-01 12:00}  │
  │  group2  →  {maxSeq: 120, lastMsgAt: 2025-01-01 11:50}  │
  │  user_c  →  {maxSeq: 30,  lastMsgAt: 2025-01-01 10:00}  │
  ├──────────────────────────────────────────────────────────┤
  │  表 2: UserReadPosition                                  │
  │  写入时机: 用户标记已读时（用户主动触发，频率低）            │
  │                                                          │
  │  (uid, channelKey) → readSeq                             │
  │                                                          │
  │  (alice, group1)  →  readSeq: 495                        │
  │  (alice, group2)  →  readSeq: 120                        │
  │  (alice, user_c)  →  readSeq: 25                         │
  ├──────────────────────────────────────────────────────────┤
  │  表 3: ChannelUpdateLog                                  │
  │  写入时机: 频道有新消息时（1 次 upsert per channel）       │
  │                                                          │
  │  channelKey → updatedAt                                  │
  │                                                          │
  │  group1  →  2025-01-01 12:00:00                          │
  │  group2  →  2025-01-01 11:50:30                          │
  │  user_c  →  2025-01-01 10:00:15                          │
  └──────────────────────────────────────────────────────────┘

  注意: 没有 per-user 的 ConversationIndex 表
        没有 per-user 的 version 计数器
        没有 CoalesceBuffer
```

### 消息到达时的写入量

```
  万人群 group1 收到一条消息:

  1. 消息存储         → 1 次写入
  2. ChannelState     → 1 次 upsert (maxSeq: 500→501)
  3. ChannelUpdateLog → 1 次 upsert (group1 → now)

  总计: 3 次写入
  与成员数完全无关
  万人群 = 3 次，十万人群 = 3 次，百万人频道 = 3 次
```

---

## 四、会话同步协议

### 核心问题

没有 per-user ConversationIndex，用户上线后怎么知道哪些频道有新消息？

### 回答：客户端驱动 + 服务端交叉比对

```
  客户端本地存储:
    lastSyncAt:  1709000000                    // 上次同步时间
    positions:   {group1: 495, group2: 120}    // 各频道已知的最新 seq
```

### 同步流程

说明：正式 v3 HTTP API 使用 `/*`；裸路径 `/conversation/*`、`/conversations/*` 是 v2 兼容层；旧的 `api` 前缀别名已移除。

```
 Client (离线 2 小时后上线)                    Server
 ─────────────────────────                   ──────
    │                                            │
    │  POST /conversation/sync                │
    │  {                                         │
    │    "uid": "alice",                         │
    │    "lastSyncAt": 1709000000,               │
    │    "msg_count": 20                         │
    │  }                                         │
    │───────────────────────────────────────────▶│
    │                                            │
    │                              ┌─────────────▼──────────────────────┐
    │                              │                                    │
    │                              │  Step 1: 查用户的订阅列表          │
    │                              │  subscriptions = ChannelRepo       │
    │                              │    .GetSubscriptions("alice")      │
    │                              │  → [group1, group2, group5,        │
    │                              │     group88, user_c, user_d]       │
    │                              │                                    │
    │                              │  Step 2: 查哪些频道在离线期间有更新  │
    │                              │  updatedChannels = ChannelUpdateLog │
    │                              │    .GetUpdatedSince(1709000000)    │
    │                              │  → [group1, group5, group99,       │
    │                              │     user_c, user_x, ...]          │
    │                              │                                    │
    │                              │  Step 3: 交叉 → 只关心用户订阅的   │
    │                              │  relevant = subscriptions          │
    │                              │            ∩ updatedChannels       │
    │                              │  → [group1, group5, user_c]        │
    │                              │                                    │
    │                              │  Step 4: 批量查 maxSeq             │
    │                              │  group1.maxSeq  = 501              │
    │                              │  group5.maxSeq  = 80               │
    │                              │  user_c.maxSeq  = 30               │
    │                              │                                    │
    │                              │  Step 5: 批量查 readSeq            │
    │                              │  alice@group1   = 495              │
    │                              │  alice@group5   = 80               │
    │                              │  alice@user_c   = 25               │
    │                              │                                    │
    │                              │  Step 6: 计算 unread               │
    │                              │  group1: 501 - 495 = 6             │
    │                              │  group5: 80 - 80   = 0             │
    │                              │  user_c: 30 - 25   = 5             │
    │                              │                                    │
    │                              │  Step 7: 查最后一条消息摘要         │
    │                              │  (批量查, 可选)                     │
    │                              │                                    │
    │                              └─────────────┬──────────────────────┘
    │                                            │
    │  HTTP 200                                  │
    │  {                                         │
    │    conversations: [                        │
    │      {channel:"group1", unread:6,          │
    │       lastMsg:{from:"bob", text:"..."}},   │
    │      {channel:"group5", unread:0},         │
    │      {channel:"user_c", unread:5,          │
    │       lastMsg:{from:"user_c", text:"..."}} │
    │    ],                                      │
    │    syncAt: 1709007200                      │
    │  }                                         │
    │◀──────────────────────────────────────────│
    │                                            │
    │  客户端保存 lastSyncAt = 1709007200        │
```

### 同步步骤的代价分析

```
  Step 1: GetSubscriptions("alice")
          用户订阅的频道列表，通常几十到几百个
          可以强缓存（变化频率很低，通过订阅/退订事件失效）

  Step 2: GetUpdatedSince(timestamp)
          ChannelUpdateLog 按 updatedAt 索引
          扫描时间范围内有更新的频道
          离线 2 小时 → 可能几百到几千个频道有更新

  Step 3: 交叉取交集
          内存操作，纳秒级
          结果通常远小于两个集合（用户只关心自己的频道）

  Step 4-7: 批量查询
          结果集通常几十个频道，批量查询很快

  总结: 整个同步过程只有 READ，没有 WRITE
        代价与"离线期间用户订阅频道的更新数"成正比
        而不是与"所有频道的总成员数"成正比
```

---

## 五、ChannelUpdateLog 设计

这是整个方案的关键数据结构。它替代了 per-user ConversationIndex，将 O(N) 的用户维度写入降为 O(1) 的频道维度写入。

### 数据结构

```go
// ChannelUpdateLog 频道更新日志
// 每个频道只保留最新一条记录（upsert，不是 append）
type ChannelUpdateEntry struct {
    ChannelKey  ChannelKey
    UpdatedAt   time.Time     // 最后更新时间（用于范围查询）
    MaxSeq      MessageSeq    // 冗余存储，避免二次查询
    LastMsgID   MessageID     // 冗余存储
}
```

### 存储引擎中的索引

```
  Pebble Key 设计:

  主键:   "cul:{channelKey}"         → ChannelUpdateEntry
  索引:   "cul_t:{updatedAt}:{channelKey}" → nil (空值，仅用于范围扫描)

  查询 "最近 2 小时内更新的频道":
    Scan("cul_t:{2小时前}", "cul_t:{now}")
    → 返回所有 channelKey

  写入时（原子操作）:
    1. Put("cul:{channelKey}", entry)
    2. Delete("cul_t:{oldUpdatedAt}:{channelKey}")   // 删除旧时间索引
    3. Put("cul_t:{newUpdatedAt}:{channelKey}", nil)  // 插入新时间索引
    用 WriteBatch 保证原子性
```

### 在 Pipeline 中的位置

```go
// StoreStage 中更新 ChannelUpdateLog
func (s *StoreStage) Process(ctx context.Context, mc *MessageContext) error {
    // 1. 保存消息
    seq, err := s.msgRepo.Save(ctx, mc.Message)
    if err != nil {
        return err
    }
    mc.Seq = seq

    // 2. 更新 ChannelUpdateLog（1 次 upsert）
    err = s.channelUpdateLog.Upsert(ctx, ChannelUpdateEntry{
        ChannelKey: mc.Channel.Key,
        UpdatedAt:  mc.Message.Timestamp,
        MaxSeq:     seq,
        LastMsgID:  mc.Message.ID,
    })
    return err
}
```

### 清理策略

ChannelUpdateLog 会随时间增长，需要定期清理：

```
  策略: 保留最近 7 天的记录

  - 用户离线超过 7 天 → 上线时走全量同步（遍历订阅列表）
  - 用户离线 7 天内 → 走 ChannelUpdateLog 增量同步
  - 99.9% 的同步请求是增量同步

  清理: 后台定时任务删除 updatedAt < 7天前 的记录
```

---

## 六、在线用户优化

在线用户已经通过 RecvPacket 实时收到了消息，客户端可以本地维护会话列表。

```
  ┌──────────────────────────────────────────────────────────────┐
  │  在线用户: 本地维护会话，零服务端请求                           │
  │                                                              │
  │  Client 收到 RecvPacket:                                     │
  │  {channelID:"group1", seq:501, fromUID:"bob", payload:...}  │
  │                                                              │
  │  Client 本地操作:                                             │
  │    conversations["group1"].lastMsg = {from:"bob", ...}       │
  │    conversations["group1"].lastMsgAt = now                   │
  │    conversations["group1"].localMaxSeq = 501                 │
  │    conversations["group1"].unread++                          │
  │    排序会话列表                                               │
  │                                                              │
  │  服务端: 什么都不做                                           │
  └──────────────────────────────────────────────────────────────┘

  ┌──────────────────────────────────────────────────────────────┐
  │  离线 → 上线: 一次增量同步补齐                                │
  │                                                              │
  │  1. 连接认证成功                                              │
  │  2. POST /conversation/sync {lastSyncAt: ...}               │
  │  3. 服务端: 订阅列表 ∩ ChannelUpdateLog → 变化的频道          │
  │  4. 返回变化的频道 + unread + lastMsg                        │
  │  5. 客户端合并到本地会话列表                                   │
  │  6. 切换到在线模式，RecvPacket 驱动                           │
  └──────────────────────────────────────────────────────────────┘
```

---

## 七、标记已读

```
 Client                                     Server
 ──────                                    ──────
    │                                          │
    │  POST /conversation/read              │
    │  {channelID:"group1",                    │
    │   channelType:2,                         │
    │   readSeq: 501}                          │
    │─────────────────────────────────────────▶│
    │                                          │
    │                                ┌─────────▼──────────┐
    │                                │ UserReadPosition:   │
    │                                │ (alice, group1)     │
    │                                │  .readSeq = 501     │
    │                                │                    │
    │                                │ 仅 1 次写入         │
    │                                └─────────┬──────────┘
    │                                          │
    │  HTTP 200 OK                             │
    │◀─────────────────────────────────────────│

  下次查询 unread:
    channel.maxSeq = 501
    user.readSeq   = 501
    unread = 501 - 501 = 0 ✅

  无并发竞争:
    新消息 → 只更新 channel.maxSeq
    标记已读 → 只更新 user.readSeq
    两个操作写不同的表，不需要事务或锁
```

---

## 八、订阅变更处理

用户加入/退出频道是低频操作，直接写 per-user 记录没有性能问题。

### 用户被加入万人群

```
  admin 把 alice 加入 group1:

  1. ChannelRepo: group1.subscribers += "alice"       ← 频道维度写入
  2. SubscriptionRepo: alice.subscriptions += group1  ← 用户维度写入（低频，1 次）
  3. 通知 alice 客户端刷新订阅列表                      ← 推送或下次同步
```

### 同步时如何发现新加入的频道

```
  SubscriptionRepo 记录变更时间:
    (uid, channelKey, joinedAt)

  同步时:
    1. ChannelUpdateLog → 已有频道的消息更新
    2. SubscriptionRepo → lastSyncAt 之后新加入的频道
    两者合并返回
```

```
 Client                                     Server
 ──────                                    ──────
    │                                          │
    │  POST /conversation/sync              │
    │  {lastSyncAt: T1}                        │
    │─────────────────────────────────────────▶│
    │                                          │
    │                              ┌───────────▼─────────────────────┐
    │                              │                                 │
    │                              │  A: 已有频道的消息变化           │
    │                              │     subscriptions(alice)        │
    │                              │     ∩ ChannelUpdateLog(>T1)     │
    │                              │     → [group5, user_c]          │
    │                              │                                 │
    │                              │  B: 新加入的频道                 │
    │                              │     SubscriptionRepo            │
    │                              │     .JoinedAfter(alice, T1)     │
    │                              │     → [group1]   (刚被加入)     │
    │                              │                                 │
    │                              │  合并 A + B → 完整变化列表       │
    │                              │                                 │
    │                              └───────────┬─────────────────────┘
    │                                          │
    │  HTTP 200                                │
    │  {conversations: [                       │
    │    {group5, unread:3, ...},              │
    │    {user_c, unread:1, ...},              │
    │    {group1, unread:501, isNew:true, ...} │  ← 新频道，未读 = maxSeq
    │  ]}                                      │
    │◀─────────────────────────────────────────│
```

---

## 九、完整写入量对比

### 万人群每秒 10 条消息

| 操作 | 方案 A (朴素) | 方案 B (CoalesceBuffer) | 方案 C (零写入) |
|------|:---:|:---:|:---:|
| 消息存储 | 10 | 10 | 10 |
| Channel 状态更新 | 10 | 10 | 10 |
| ChannelUpdateLog | - | - | 10 (upsert 幂等) |
| 会话 Index 写入 | **100,000** | **10,000** (合并后) | **0** |
| **每秒总写入** | **100,020** | **10,020** | **30** |

### 100 个万人群各 10 条/秒

| | 方案 A | 方案 B | 方案 C |
|---|:---:|:---:|:---:|
| **每秒总写入** | **10,002,000** | **1,002,000** | **3,000** |

方案 C 的写入量与成员数**完全无关**。

---

## 十、查询代价分析

零写入方案把代价从写入侧转移到了查询侧。需要确保查询代价可控：

### 会话同步的查询步骤

```
  Step 1: GetSubscriptions(uid)
    ┌──────────────────────────────────────────────┐
    │ 频率: 每次同步 1 次                            │
    │ 代价: 1 次 DB 读取                             │
    │ 优化: 强缓存（订阅变更频率极低，通过事件失效）    │
    │ 缓存命中后: 0 次 DB 读取                        │
    └──────────────────────────────────────────────┘

  Step 2: GetUpdatedChannelsSince(timestamp)
    ┌──────────────────────────────────────────────┐
    │ 频率: 每次同步 1 次                            │
    │ 代价: 按 updatedAt 索引范围扫描                │
    │ 离线 2 小时: 扫描约数百到数千条                  │
    │ 离线 1 天:  扫描约数千到数万条                   │
    │ 这是全局数据，不是 per-user，可以缓存           │
    └──────────────────────────────────────────────┘

  Step 3: 交集计算
    ┌──────────────────────────────────────────────┐
    │ 纯内存 set 交集                                │
    │ 用户订阅 500 个频道，更新了 5000 个频道          │
    │ → 交集可能几十到几百个                          │
    │ 代价: 微秒级                                   │
    └──────────────────────────────────────────────┘

  Step 4-6: 批量查 maxSeq + readSeq + lastMsg
    ┌──────────────────────────────────────────────┐
    │ 交集结果通常几十到几百个频道                     │
    │ 3 次批量查询 (BatchGet)                        │
    │ 代价: 毫秒级                                   │
    └──────────────────────────────────────────────┘

  总计: 1~4 次 DB 查询（缓存命中时更少），0 次写入
```

### GetUpdatedChannelsSince 的热点优化

高并发场景下，大量用户同时上线同步，都会查询 ChannelUpdateLog。

```
  优化: 内存中维护一个最近更新频道的滑动窗口

  ┌───────────────────────────────────────────────────┐
  │  RecentUpdateCache (内存)                         │
  │                                                   │
  │  - 维护最近 N 小时内有更新的 channelKey 集合        │
  │  - 按时间分桶: [12:00~12:05], [12:05~12:10], ...  │
  │  - 每条消息到达时: 更新对应桶（O(1) 内存操作）      │
  │  - 查询时: 合并所需时间范围的桶                     │
  │  - 完全在内存中完成，不查 DB                        │
  │                                                   │
  │  内存占用: 10 万个频道 × 32B ≈ 3MB                 │
  └───────────────────────────────────────────────────┘

  Pipeline 中:
    StoreStage:
      msg 持久化
      channel.maxSeq++
      ChannelUpdateLog.Upsert()           ← 持久化（兜底）
      RecentUpdateCache.Touch(channelKey)  ← 内存（热路径查询用）

  同步时:
    优先查 RecentUpdateCache（内存，微秒级）
    cache miss 或离线超过 N 小时 → fallback 到 ChannelUpdateLog（DB）
```

---

## 十一、全流程时序图

```
  ① 消息写入 (O(1) 写入，与成员数无关)
  ──────────────────────────────────────────────────────────────

  alice                    Pipeline                         Storage
    │  SendPacket             │                                │
    │────────────────────────▶│                                │
    │                         │                                │
    │                 Store Stage:                              │
    │                   1. msgRepo.Save(msg)                   │
    │                   ──────────────────────────────────────▶│ 1 write
    │                   2. channelUpdateLog.Upsert(group1,now) │
    │                   ──────────────────────────────────────▶│ 1 upsert
    │                   3. recentUpdateCache.Touch(group1)     │
    │                      (内存操作)                           │
    │                         │                                │
    │                 Deliver Stage:                            │
    │                   推送 RecvPacket 给在线用户              │
    │                         │                                │
    │  SendAck ✅              │                                │
    │◀────────────────────────│                                │
    │                         │                                │
    │                                                          │
    │  无论 1 人群还是 10 万人群，存储写入量完全相同             │


  ② 在线用户收到消息 (0 次服务端写入)
  ──────────────────────────────────────────────────────────────

  bob (在线)                           bob 的客户端
    │                                     │
    │  RecvPacket                          │
    │  {group1, seq:501, from:alice, ...}  │
    │────────────────────────────────────▶│
    │                                     │
    │                           本地更新会话列表:
    │                           conversations["group1"]
    │                             .lastMsg = this
    │                             .unread++
    │                             .lastMsgAt = now
    │                           重新排序会话列表
    │                                     │
    │                           零服务端请求 ✅


  ③ 离线用户上线同步 (只有 READ，没有 WRITE)
  ──────────────────────────────────────────────────────────────

  carol (离线 2 小时)                  Server
    │                                     │
    │  POST /conversation/sync            │
    │  {lastSyncAt: T1}                   │
    │────────────────────────────────────▶│
    │                                     │
    │                           1. subscriptions(carol) = [g1,g2,g5,u_c]     (缓存命中)
    │                           2. recentUpdateCache.Since(T1) = [g1,g5,g99] (内存查询)
    │                           3. 交集 = [g1, g5]                           (内存计算)
    │                           4. BatchGetMaxSeq([g1,g5])                   (1 次 DB 读)
    │                           5. BatchGetReadSeq(carol, [g1,g5])           (1 次 DB 读)
    │                           6. unread: g1=6, g5=3                        (内存计算)
    │                                     │
    │  HTTP 200                           │
    │  [{g1, unread:6}, {g5, unread:3}]   │
    │◀────────────────────────────────────│
    │                                     │
    │  总计: 2 次 DB 读取，0 次写入 ✅     │


  ④ 用户标记已读 (唯一的 per-user 写入，但由用户主动触发)
  ──────────────────────────────────────────────────────────────

  carol                                Server
    │                                     │
    │  POST /conversation/read            │
    │  {group1, readSeq: 501}             │
    │────────────────────────────────────▶│
    │                                     │
    │                           UserReadPosition:
    │                           (carol, group1).readSeq = 501
    │                           1 次写入
    │                                     │
    │  HTTP 200 OK                        │
    │◀────────────────────────────────────│
```

---

## 十二、边界情况处理

### 12.1 用户离线超过 7 天（ChannelUpdateLog 已清理）

```
  降级为全量同步:

  1. 获取用户订阅列表: [g1, g2, g3, ..., g500]
  2. 批量查 maxSeq: 每个频道的 maxSeq
  3. 批量查 readSeq: 用户在每个频道的 readSeq
  4. 计算 unread，过滤 unread > 0 的频道返回

  代价: 需要检查所有 500 个订阅频道
  但这是极少数场景（7 天不上线的用户占比很低）
  可以分页处理，不会有性能问题
```

### 12.2 用户订阅了 1000 个频道

```
  增量同步仍然高效:

  ChannelUpdateLog 返回 "最近 2 小时内更新的频道" = 5000 个
  用户订阅 1000 个频道
  交集 = 可能 100 个频道有更新

  瓶颈是交集计算，但 set 交集是 O(min(M,N)) 的内存操作
  1000 ∩ 5000 → 微秒级
```

### 12.3 多设备同步

```
  alice 有 APP 和 PC 两个设备:

  APP 在线: 通过 RecvPacket 实时更新本地会话列表
  PC 离线 → 上线: POST /conversation/sync {lastSyncAt: ...}

  readSeq 是跨设备共享的（存在服务端）:
    APP 标记已读 → readSeq = 501
    PC 上线同步 → 查到 readSeq = 501 → unread 正确

  会话列表是各设备独立维护的（基于 RecvPacket + sync）:
    APP 和 PC 各自独立同步，互不影响
```

### 12.4 新成员加入万人群后的历史会话

```
  alice 刚被加入 group1（group1 已有 50000 条消息）:

  1. SubscriptionRepo 记录: (alice, group1, joinedAt=now)
  2. alice 的 readSeq 不存在 → 默认为 0
  3. unread = maxSeq(50000) - readSeq(0) = 50000？

  处理: 加入频道时初始化 readSeq = 当前 maxSeq
    → unread = 0，新成员不看历史未读
    → 如需查看历史消息，客户端主动向前翻页拉取
```

---

## 十三、Port 接口

```go
// ChannelUpdateLog 频道更新日志
type ChannelUpdateLog interface {
    // Upsert 更新频道的最后活跃时间（每条消息 1 次调用）
    Upsert(ctx context.Context, entry ChannelUpdateEntry) error

    // GetUpdatedSince 查询指定时间之后有更新的频道
    GetUpdatedSince(ctx context.Context, since time.Time, limit int) ([]ChannelUpdateEntry, error)

    // Cleanup 清理指定时间之前的记录
    Cleanup(ctx context.Context, before time.Time) error
}

// ReadPositionRepository 已读位置
type ReadPositionRepository interface {
    Save(ctx context.Context, uid string, channelKey ChannelKey, readSeq MessageSeq) error
    Get(ctx context.Context, uid string, channelKey ChannelKey) (MessageSeq, error)
    BatchGet(ctx context.Context, uid string, keys []ChannelKey) (map[ChannelKey]MessageSeq, error)
}

// SubscriptionRepository 用户订阅关系
type SubscriptionRepository interface {
    // GetSubscriptions 获取用户订阅的所有频道
    GetSubscriptions(ctx context.Context, uid string) ([]ChannelKey, error)
    // JoinedAfter 查询用户在指定时间之后新加入的频道
    JoinedAfter(ctx context.Context, uid string, since time.Time) ([]ChannelKey, error)
    // Add/Remove
    Add(ctx context.Context, uid string, channelKey ChannelKey) error
    Remove(ctx context.Context, uid string, channelKey ChannelKey) error
}

// ConversationService 会话同步服务（Application 层）
type ConversationService struct {
    channelUpdateLog  ChannelUpdateLog
    readPositionRepo  ReadPositionRepository
    subscriptionRepo  SubscriptionRepository
    channelStateRepo  ChannelStateRepository
    messageRepo       MessageRepository
    recentUpdateCache *RecentUpdateCache  // 内存缓存
}

func (s *ConversationService) Sync(ctx context.Context, uid string, lastSyncAt time.Time) ([]ConversationDTO, error) {
    // 1. 获取用户订阅列表（缓存）
    subs, _ := s.subscriptionRepo.GetSubscriptions(ctx, uid)
    subsSet := toSet(subs)

    // 2. 查哪些频道有更新（优先走内存缓存）
    updated := s.recentUpdateCache.Since(lastSyncAt)
    if updated == nil {
        // 缓存未覆盖（离线太久），fallback 到 DB
        entries, _ := s.channelUpdateLog.GetUpdatedSince(ctx, lastSyncAt, 10000)
        updated = toKeySet(entries)
    }

    // 3. 交集
    relevant := intersect(subsSet, updated)

    // 4. 新加入的频道
    newSubs, _ := s.subscriptionRepo.JoinedAfter(ctx, uid, lastSyncAt)
    for _, ch := range newSubs {
        relevant[ch] = struct{}{}
    }

    // 5. 批量查 maxSeq + readSeq
    keys := setToSlice(relevant)
    maxSeqs, _ := s.channelStateRepo.BatchGetMaxSeq(ctx, keys)
    readSeqs, _ := s.readPositionRepo.BatchGet(ctx, uid, keys)

    // 6. 组装结果
    result := make([]ConversationDTO, 0, len(keys))
    for _, key := range keys {
        maxSeq := maxSeqs[key]
        readSeq := readSeqs[key]
        unread := uint64(0)
        if maxSeq > readSeq {
            unread = uint64(maxSeq - readSeq)
        }
        result = append(result, ConversationDTO{
            ChannelKey: key,
            Unread:     unread,
            MaxSeq:     maxSeq,
        })
    }

    return result, nil
}
```

---

## 十四、总结

### 设计哲学

```
  ┌─────────────────────────────────────────────────────────┐
  │                                                         │
  │  朴素思路: 消息到达 → 写 N 份用户数据 → 用户直接读       │
  │                        ▲ 灾难                           │
  │                                                         │
  │  零写入思路: 消息到达 → 只写频道数据 → 用户查询时计算     │
  │                         ▲ 常数级           ▲ 按需、可缓存 │
  │                                                         │
  │  本质: 把"写时材料化"变成"读时计算"                       │
  │       写是所有成员被动承受的，读是单个用户主动发起的      │
  │       用户数 >> 查询频率，所以读时计算远比写时更划算      │
  │                                                         │
  └─────────────────────────────────────────────────────────┘
```

### 三张表各自的写入触发条件

| 表 | 写入时机 | 每次写入量 | 触发者 |
|---|---|---|---|
| `ChannelState` | 每条消息 | 1 次 upsert | 消息发送 |
| `ChannelUpdateLog` | 每条消息 | 1 次 upsert | 消息发送 |
| `UserReadPosition` | 用户标记已读 | 1 次 upsert | 用户主动 |

**没有任何表的写入量与频道成员数相关。**
