# 基于 Pebble 的静态表规范与 user/channel 示例

本文先定义一套可扩展的静态表规范，再用 `user` 和 `channel` 两张表做实例说明。

这次的重点是：

- 表结构固定写在代码里
- 不做 `system.namespace` / `system.descriptor`
- 底层直接使用 Pebble
- 允许后续持续增加新表
- `value` 存储方式借鉴 Cockroach 的 SQL value-side encoding

这里的“借鉴 Cockroach”不是说完整复刻 Cockroach 的所有层，而是重点借鉴下面这几条：

1. 主键列主要放在 key 中
2. 行数据按 column family 组织
3. 一个 family 的 value 使用 tuple/value-side encoding
4. value 内部按 `columnID delta + type tag + payload` 编码
5. NULL 列通常不落盘，缺失表示 NULL
6. family value 外面再包一层 `4-byte-checksum + 1-byte-tag`

但本文仍然保持简化：

- 不实现 Cockroach 的完整 MVCC
- 不实现完整 `roachpb.Value` / `MVCCValue` 结构，但保留它最核心的 `checksum + tag + data` 外层格式
- 不实现运行时 schema metadata 表

也就是说，本文采用的是：

> 借鉴 Cockroach SQL 层的行 value 编码思想，
> 但不照搬它完整的 KV/MVCC 存储栈。

## 1. 规范目标

这套规范解决 5 个问题：

1. 新表如何分配稳定的 `tableID / columnID / indexID / familyID`
2. 不同表如何共用同一套 key/value 编码规则
3. 新增列时如何保持旧数据可读
4. 新增索引时如何保持写入、更新、删除一致维护
5. 如何让 value 存储方式更接近 Cockroach，而不是自定义一套完全不同的 row blob

非目标：

- 不考虑分布式事务
- 不考虑 MVCC
- 不考虑 SQL 优化器
- 不考虑在线 DDL

## 2. 统一静态 catalog 规范

## 2.1 全局注册表

所有表都必须先在代码里的全局注册表登记。

建议有一个中心文件维护稳定 ID：

```go
const (
    TableIDUser    uint32 = 1
    TableIDChannel uint32 = 2
)
```

以后新增表，例如 `conversation`：

```go
const (
    TableIDUser         uint32 = 1
    TableIDChannel      uint32 = 2
    TableIDConversation uint32 = 3
)
```

规则：

- `tableID` 全局唯一
- 一旦分配，永久不复用
- 即使删除表，也不把它的 `tableID` 让给新表

## 2.2 列 ID 规范

每张表内部的列都要有稳定的 `columnID`。

规则：

- `columnID` 在表内唯一
- 一旦发布，不能随意改
- 删除列后不复用旧 `columnID`
- 新增列时只追加新的 `columnID`

例如 `user`：

```go
Columns: []ColumnDesc{
    {ID: 1, Name: "uid", Type: ColumnString},
    {ID: 2, Name: "token", Type: ColumnString},
    {ID: 3, Name: "device_flag", Type: ColumnInt64},
    {ID: 4, Name: "device_level", Type: ColumnInt64},
}
```

以后加 `name` 字段，应分配：

```go
{ID: 5, Name: "name", Type: ColumnString}
```

## 2.3 索引 ID 规范

每张表的索引都必须有稳定的 `indexID`。

建议规则：

- 主键索引固定 `indexID = 1`
- 二级索引从 `2` 开始递增
- 同一张表内 `indexID` 唯一
- 不复用老 `indexID`

## 2.4 Column Family 规范

为了借鉴 Cockroach 的 value 存储方式，每张表增加 `column family` 概念。

规则：

- 每张表至少有一个 `family 0`
- `family 0` 作为默认 family，必须存在
- 主键列通常不进 family value，而是主要体现在 key 里
- 非主键列放入某个 family 中
- 后续新增列时，可以决定是放进现有 family，还是新建 family

建议：

- 小表、简单表，先全部非主键列都放 `family 0`
- 大字段、低频字段、未来想单独更新的字段，可以放独立 family

例如以后 `user` 增加 `profile_blob` 这类大字段，可以放：

```go
Family 1: profile_blob
```

这样读主路径时不必每次都把大字段读出来。

## 2.5 表描述结构

统一描述结构建议如下：

```go
type ColumnType int

const (
    ColumnString ColumnType = iota + 1
    ColumnInt64
)

type ColumnDesc struct {
    ID       uint16
    Name     string
    Type     ColumnType
    Nullable bool
}

type ColumnFamilyDesc struct {
    ID              uint16
    Name            string
    ColumnIDs       []uint16
    DefaultColumnID uint16
}

type IndexDesc struct {
    ID        uint16
    Name      string
    Unique    bool
    ColumnIDs []uint16
    Primary   bool
}

type TableDesc struct {
    ID               uint32
    Name             string
    Columns          []ColumnDesc
    Families         []ColumnFamilyDesc
    PrimaryIndex     IndexDesc
    SecondaryIndexes []IndexDesc
}
```

这里：

- `columnID` 和 `familyID` 使用 `uint16`
- 对未来新增列、新增 family 足够用
- 仍然比较紧凑

## 3. 统一二进制编码规范

## 3.1 key 类型

统一约定三类记录前缀：

- `0x00`：元信息
- `0x01`：主记录
- `0x02`：二级索引记录

## 3.2 主记录 key

借鉴 Cockroach，主记录不是“一行一个 key”，而是“一行的每个 family 一个 key”。

主记录 key 结构：

```text
[kind:1B][tableID:4B][indexID:2B][encoded primary key columns...][familyID:uvarint]
```

说明：

- `kind=0x01`
- `indexID` 对主记录固定为主键索引 ID，一般是 `1`
- 同一行不同 family 的区别体现在最后的 `familyID`

这样一个 row 可能对应多条 Pebble KV：

```text
row(pk, family0) -> value0
row(pk, family1) -> value1
row(pk, family2) -> value2
```

这就是 Cockroach 的核心思想之一：  
一行不是一个“大 value”，而是一组按 family 拆开的 KV。

## 3.3 二级索引 key

二级索引 key 保持统一规范：

```text
[kind:1B][tableID:4B][indexID:2B][encoded index columns...][encoded primary key suffix...]
```

其中：

- `kind=0x02`
- `primary key suffix` 用于保证索引键唯一，并支持从索引还原主键

本文默认：

- 二级索引 value 为空
- 先不实现“索引存储覆盖列”

## 3.4 主键列与 value 的关系

借鉴 Cockroach 的建议做法：

- 主键列主要编码在 key 里
- 主记录 family value 中通常不重复保存主键列
- value 主要存非主键列

这意味着：

- 通过 key 就能知道这行是谁
- value 只负责补充这行的非主键属性

这比“把整行所有字段再完整塞进 value”更接近 Cockroach 的设计。

## 3.5 字段编码规则

### key 中 string 编码

```text
[len:2B][raw bytes]
```

例如：

- `"u1001"` -> `00 05 75 31 30 30 31`
- `"group-001"` -> `00 09 67 72 6f 75 70 2d 30 30 31`

### key 中 int64 编码

为了保证 Pebble 中的字典序和数值序一致，`int64` 在 key 中使用 ordered encoding：

```go
u := uint64(v) ^ 0x8000000000000000
binary.BigEndian.PutUint64(buf[:], u)
```

例如：

- `0` -> `80 00 00 00 00 00 00 00`
- `1` -> `80 00 00 00 00 00 00 01`
- `2` -> `80 00 00 00 00 00 00 02`

## 3.6 value 存储：借鉴 Cockroach 的 tuple/value-side encoding

本文的核心调整在这里。

不再使用旧版文档里的：

```text
[version][columnCount][column entries...]
```

而改成更接近 Cockroach 的 value-side encoding：

```text
[encoded column 1][encoded column 2][encoded column 3]...
```

每个列值的结构为：

```text
[valueTag(colIDDelta, type)][payload]
```

这里的 `valueTag` 借鉴 Cockroach 的 `EncodeValueTag()`：

- 用 `columnID delta`，而不是重复写完整 `columnID`
- 用一个 type tag 表示值类型
- 小的 `colIDDelta + type` 会压缩到 1 个字节

源码参考：

- `valueside.Encode()`：`pkg/sql/rowenc/valueside/encode.go`
- `EncodeValueTag()`：`pkg/util/encoding/encoding.go`

### 为什么用 `columnID delta`

因为同一个 family 内的列总是按 `columnID` 升序写入。

例如列顺序是：

- `2`
- `3`
- `4`

那它们写入时保存的是：

- `delta=2`
- `delta=1`
- `delta=1`

这样比每次都写完整列号更省空间。

### 为什么 NULL 不写

借鉴 Cockroach：

- NULL 列通常不落盘
- 读取时，如果某列在该 family 的 value 中缺失，就认为它是 NULL

好处：

- 节省空间
- 新增 nullable 列时，旧数据天然兼容

## 3.7 当前建议的 value 外层包装

Cockroach 的 family payload 在更外层还会再包：

- `roachpb.Value`
- checksum
- value tag
- MVCCValue / MVCCValueHeader

本文现在采用一个简化但实用的版本：

```text
PebbleValue = [4-byte-checksum][1-byte-tag][family payload]
```

和当前仓库里的 `wkdb/` 实现对齐说明：

- `wkdb/codec.go` 已经实现了这层外层包装：`wrapFamilyValue` 负责写入 `checksum + tag + payload`，`decodeWrappedValue` 负责按 key 重新计算并校验 checksum
- `user/channel` 的主记录 CRUD 已经在写路径调用 `encodeUserFamilyValue` / `encodeChannelFamilyValue`，在读路径调用 `decodeUserFamilyValue` / `decodeChannelFamilyValue`
- 当前没有包这层 checksum 的，是 value 为空的二级索引记录；这不等于“整个 wkdb 没有 checksum”
- `wkdb/snapshot_codec.go` 里的 slot snapshot payload 也有独立 CRC32 校验，但不属于本文讨论的主记录 family value 编码范围

其中：

- `family payload` 仍然是前面定义的 tuple/value-side encoding 结果
- `tag` 借鉴 Cockroach `roachpb.Value` 的 tag 语义
- `checksum` 借鉴 Cockroach 的 key-aware CRC 校验思路

建议约定：

- `tag = 0x0A`
- 含义：`TupleFamilyValue`
- 它表示当前这条 Pebble value 里装的是“一个 family 的 tuple/value-side payload”

也就是说，当前方案不是直接把 payload 裸写进 Pebble，而是：

```text
PebbleValue = checksum(4B) + tag(1B) + familyPayload
```

### checksum 计算规则

借鉴 Cockroach `roachpb.Value`：

```text
checksum = CRC-32-IEEE(key + tag + familyPayload)
```

说明：

- `key` 是这条 Pebble KV 的完整 key
- `tag + familyPayload` 是 value 去掉前 4 字节 checksum 后的剩余内容
- checksum 以大端 4 字节写入 value 开头
- 如果算出来是 `0`，建议折叠成 `1`
  这样可以和“未初始化 checksum”的语义分开，和 Cockroach 的做法一致

### 写入流程

对于一条主记录 family KV：

1. 先按 `columnID delta + type tag + payload` 编出 `familyPayload`
2. 再在前面放 1 字节 `tag`
3. 用 `key + tag + familyPayload` 计算 CRC-32-IEEE
4. 把 4 字节 checksum 写到最前面

得到最终落盘 value：

```text
[checksum:4B][tag:1B][familyPayload...]
```

补充约定：

- 这里的外层包装默认只用于“主记录 family value”
- 如果二级索引 value 为空，则仍然保持空 value，不额外写 checksum 和 tag
- 如果未来二级索引也开始存非空 value，再复用同一套包装规则

### 读取流程

读取时：

- 先取出前 4 字节 checksum
- 再取出后面的 `tag + familyPayload`
- 用当前 key 重新计算 CRC-32-IEEE
- 校验通过后，再按 family payload 解码列值

### 仍然保留的简化点

虽然加上了 `4-byte-checksum`，但本文仍然没有照搬完整 Cockroach Value 栈：

- 不引入 timestamp
- 不引入 MVCC 版本链
- 不引入 `MVCCValueHeader`
- 不引入 extended encoding
- 不引入完整 `roachpb.Value` API

## 3.8 元信息 key 规范

即使不做 `system.namespace/system.descriptor`，也建议保留极少量全局元信息。

例如：

```text
[00]["schema_version"] -> 1
```

这样以后 row/value 编码升级时，至少有一个总版本入口。

## 4. 统一 CRUD 语义规范

所有表都按同一套语义执行 CRUD。

## 4.1 INSERT

统一流程：

1. 根据表名拿到 `TableDesc`
2. 校验主键列和必要字段
3. 编码主记录各 family 的 key
4. 为每个 family 生成 tuple/value-side payload
5. 给每个 family payload 包上 `checksum + tag`
6. 编码所有二级索引 key
7. 检查主键冲突
8. 检查唯一索引冲突
9. 用一个 batch 同时写入所有 family KV 和所有索引 KV

结论：

> 插入一行，不再是“1 条主记录 + N 条索引”，而是
> “M 条 family 主记录 + N 条索引”。

其中：

- `M` = 该行非空 family 数量，且 `family 0` 必须存在

## 4.2 SELECT BY PRIMARY KEY

统一流程：

1. 根据主键列构造主键前缀
2. 读取该主键下的所有 family KV
3. 先校验每个 family value 的 checksum
4. 分别解码每个 family 的 value payload
5. 合并成完整 row

如果只需要部分列，理论上也可以只读需要的 family。

这正是 column family 的价值之一。

## 4.3 SELECT BY SECONDARY INDEX

统一流程：

1. 根据索引条件编码索引前缀
2. 用 Pebble iterator 做前缀扫描
3. 从索引 key 中还原主键
4. 再读取主记录 family KV
5. 校验 family value checksum
6. 解码并返回结果

这就是：

```text
索引扫描 -> 主键恢复 -> 读 family -> 合并 row
```

## 4.4 UPDATE

统一流程：

1. 按主键读出旧 row
2. 在内存中生成新 row
3. 比较旧 row 和新 row 哪些 family 发生变化
4. 重新编码变化 family 的 `payload + checksum + tag`
5. 如果索引列发生变化，同步删旧索引、写新索引

结论：

> UPDATE 不一定重写整行，
> 可以只重写受影响的 family。

这比“整行一个大 value”更接近 Cockroach 的思路。

## 4.5 DELETE

统一流程：

1. 先读出旧 row
2. 计算该 row 的所有 family key
3. 计算该 row 的所有索引 key
4. 在一个 batch 中删除全部 family 和全部索引

结论：

> DELETE 也不是删 1 条主记录，而是删整行对应的所有 family KV 和索引 KV。

## 5. 新表接入规范

以后增加任何新表，都按下面步骤做。

## 5.1 分配稳定 ID

新增表时必须先分配：

- `tableID`
- 各列 `columnID`
- 各 family `familyID`
- 主键索引 `indexID=1`
- 所有二级索引 `indexID`

## 5.2 定义表模块

建议每张表都至少有：

- `desc.go`
  只放 `TableDesc`
- `codec.go`
  放 key 编码、family value 编码/解码
- `store.go`
  放 `Insert/Get/Update/Delete`

## 5.3 明确官方查询路径

对于每张表，要先明确：

- 哪些是主键点查
- 哪些字段支持二级索引
- 哪些场景允许全表扫描
- 哪些大字段要拆独立 family

## 5.4 新增列规范

新增列时：

- 只追加新的 `columnID`
- 决定该列放入哪个 family
- 解码时，如果旧记录缺失该列，则按 NULL 或默认值处理

例如以后给 `user` 增加 `name`：

```go
{ID: 5, Name: "name", Type: ColumnString}
```

如果把它放进 `family 0`，老记录不需要改写也能正常读：

- 旧记录没有 `name`
- 解码时缺失即视为 NULL

## 5.5 新增索引规范

新增索引时要考虑：

- 历史数据如何回填
- 新写入数据如何同步维护
- 更新和删除如何同步维护

如果当前系统没有 backfill 机制，那么新增索引本质上就是一次离线重建。

## 6. user 表实例

## 6.1 表定义

`user` 表字段：

- `uid: string`
- `token: string`
- `device_flag: int64`
- `device_level: int64`

主键：

```text
PRIMARY KEY(uid)
```

采用一个 family：

- `family 0`：`token, device_flag, device_level`

静态描述：

```go
var UserTable = &TableDesc{
    ID:   TableIDUser,
    Name: "user",
    Columns: []ColumnDesc{
        {ID: 1, Name: "uid", Type: ColumnString},
        {ID: 2, Name: "token", Type: ColumnString},
        {ID: 3, Name: "device_flag", Type: ColumnInt64},
        {ID: 4, Name: "device_level", Type: ColumnInt64},
    },
    Families: []ColumnFamilyDesc{
        {
            ID:              0,
            Name:            "primary",
            ColumnIDs:       []uint16{2, 3, 4},
            DefaultColumnID: 2,
        },
    },
    PrimaryIndex: IndexDesc{
        ID:        1,
        Name:      "pk_user",
        Unique:    true,
        Primary:   true,
        ColumnIDs: []uint16{1},
    },
}
```

这里 `uid` 不进 value，只进主键 key。

## 6.2 user 主记录 key 示例

假设：

- `tableID = 1`
- `indexID = 1`
- `uid = "u1001"`
- `familyID = 0`

那么主记录 key：

```text
[01][00 00 00 01][00 01][00 05 75 31 30 30 31][00]
```

拼起来：

```text
01 00 00 00 01 00 01 00 05 75 31 30 30 31 00
```

## 6.3 user family 0 value 示例

假设记录：

```text
uid="u1001", token="tk_abc", device_flag=1, device_level=2
```

由于 `uid` 已经在 key 里，所以 family value 只编码：

- `token`，列 ID = 2
- `device_flag`，列 ID = 3
- `device_level`，列 ID = 4

使用 Cockroach 风格 value-side encoding：

### `token="tk_abc"`

- `colIDDelta = 2`
- `type = Bytes = 6`
- `tag = (2 << 4) | 6 = 0x26`
- 长度 `06`
- 内容 `74 6b 5f 61 62 63`

编码结果：

```text
26 06 74 6b 5f 61 62 63
```

### `device_flag=1`

- `colIDDelta = 1`
- `type = Int = 3`
- `tag = (1 << 4) | 3 = 0x13`
- `1` 的 varint 记为 `02`

编码结果：

```text
13 02
```

### `device_level=2`

- `colIDDelta = 1`
- `type = Int = 3`
- `tag = 0x13`
- `2` 的 varint 记为 `04`

编码结果：

```text
13 04
```

因此 family 0 的最终 value payload 为：

```text
26 06 74 6b 5f 61 62 63 13 02 13 04
```

外层再包上：

- `tag = 0x0A`
- `checksum = CRC32-IEEE(key + 0A + payload)`

对上面的 user 主记录 key：

```text
01 00 00 00 01 00 01 00 05 75 31 30 30 31 00
```

算出来的 checksum 为：

```text
42 b6 f5 91
```

所以最终落盘 Pebble value 为：

```text
42 b6 f5 91 0a 26 06 74 6b 5f 61 62 63 13 02 13 04
```

这样就同时具备了：

- Cockroach 风格的 family payload 编码
- Cockroach 风格的 key-aware checksum 外层保护

## 6.4 user CRUD

### INSERT

```sql
INSERT INTO user(uid, token, device_flag, device_level)
VALUES ('u1001', 'tk_abc', 1, 2);
```

写入 1 条 family 0 KV：

```text
01 00 00 00 01 00 01 00 05 75 31 30 30 31 00
-> 42 b6 f5 91 0a 26 06 74 6b 5f 61 62 63 13 02 13 04
```

### SELECT

```sql
SELECT * FROM user WHERE uid = 'u1001';
```

直接点查该主键下的 `family 0`，先验 checksum，再解码回：

- `uid` 从 key 得到
- `token/device_flag/device_level` 从 value 得到

### UPDATE

```sql
UPDATE user SET token = 'tk_new' WHERE uid = 'u1001';
```

由于只有 `family 0`，直接重写这一个 family value，并重新计算 checksum 即可。

### DELETE

```sql
DELETE FROM user WHERE uid = 'u1001';
```

删除该主键下所有 family。当前只有 `family 0`，所以只删 1 条 KV。

## 7. channel 表实例

## 7.1 表定义

`channel` 表字段：

- `channel_id: string`
- `channel_type: int64`
- `ban: int64`

主键：

```text
PRIMARY KEY(channel_id, channel_type)
```

二级索引：

```text
INDEX idx_channel_id(channel_id)
```

采用一个 family：

- `family 0`：`ban`

静态描述：

```go
var ChannelTable = &TableDesc{
    ID:   TableIDChannel,
    Name: "channel",
    Columns: []ColumnDesc{
        {ID: 1, Name: "channel_id", Type: ColumnString},
        {ID: 2, Name: "channel_type", Type: ColumnInt64},
        {ID: 3, Name: "ban", Type: ColumnInt64},
    },
    Families: []ColumnFamilyDesc{
        {
            ID:              0,
            Name:            "primary",
            ColumnIDs:       []uint16{3},
            DefaultColumnID: 3,
        },
    },
    PrimaryIndex: IndexDesc{
        ID:        1,
        Name:      "pk_channel",
        Unique:    true,
        Primary:   true,
        ColumnIDs: []uint16{1, 2},
    },
    SecondaryIndexes: []IndexDesc{
        {
            ID:        2,
            Name:      "idx_channel_id",
            Unique:    false,
            ColumnIDs: []uint16{1},
        },
    },
}
```

## 7.2 channel 主记录 key 示例

假设：

- `tableID = 2`
- 主键索引 `indexID = 1`
- `channel_id = "group-001"`
- `channel_type = 1`
- `familyID = 0`

主记录 key：

```text
01 00 00 00 02 00 01 00 09 67 72 6f 75 70 2d 30 30 31 80 00 00 00 00 00 00 01 00
```

## 7.3 channel family 0 value 示例

记录：

```text
channel_id="group-001", channel_type=1, ban=0
```

由于主键列都在 key 中，所以 value 只写 `ban`。

`ban` 编码：

- `colIDDelta = 3`
- `type = Int = 3`
- `tag = (3 << 4) | 3 = 0x33`
- `0` 的 varint = `00`

所以 family 0 value：

```text
33 00
```

外层包装后：

- `tag = 0x0A`
- `checksum = CRC32-IEEE(key + 0A + 33 00)`

对该 channel 主记录 key：

```text
01 00 00 00 02 00 01 00 09 67 72 6f 75 70 2d 30 30 31 80 00 00 00 00 00 00 01 00
```

算出来的 checksum 为：

```text
6f 27 0b 83
```

所以最终落盘 Pebble value 为：

```text
6f 27 0b 83 0a 33 00
```

## 7.4 idx_channel_id 索引 key 示例

假设：

- `tableID = 2`
- `indexID = 2`
- `channel_id = "group-001"`
- 主键后缀 `channel_type = 1`

索引 key：

```text
02 00 00 00 02 00 02 00 09 67 72 6f 75 70 2d 30 30 31 80 00 00 00 00 00 00 01
```

索引 value 为空：

```text
<empty>
```

这里因为索引 value 本身为空，所以不包 `checksum + tag`。

## 7.5 channel CRUD

### INSERT

```sql
INSERT INTO channel(channel_id, channel_type, ban)
VALUES ('group-001', 1, 0);
```

写入两条 KV：

主记录：

```text
01 00 00 00 02 00 01 00 09 67 72 6f 75 70 2d 30 30 31 80 00 00 00 00 00 00 01 00
-> 6f 27 0b 83 0a 33 00
```

索引记录：

```text
02 00 00 00 02 00 02 00 09 67 72 6f 75 70 2d 30 30 31 80 00 00 00 00 00 00 01
-> <empty>
```

### SELECT BY PK

```sql
SELECT * FROM channel
WHERE channel_id = 'group-001' AND channel_type = 1;
```

直接点查主记录 family 0，先验 checksum：

- `channel_id/channel_type` 从 key 拿
- `ban` 从 value 解出来

### SELECT BY channel_id

```sql
SELECT * FROM channel WHERE channel_id = 'group-001';
```

走 `idx_channel_id`：

1. 扫索引前缀
2. 从索引 key 恢复 `channel_type`
3. 组合主键
4. 回主表读 family 0

### UPDATE ban

```sql
UPDATE channel
SET ban = 1
WHERE channel_id = 'group-001' AND channel_type = 1;
```

因为：

- 主键没变
- `idx_channel_id` 的索引列没变

所以只重写主记录 family 0 的 value，并重新计算它的 checksum。

### DELETE

```sql
DELETE FROM channel
WHERE channel_id = 'group-001' AND channel_type = 1;
```

删除：

- 主记录 family 0
- `idx_channel_id` 索引记录

## 8. 什么时候要拆新 family

以后新增列时，可以不是只考虑“要不要索引”，还要考虑“要不要拆 family”。

适合拆独立 family 的情况：

- 大字段
- 低频字段
- 不希望每次主路径查询都读出来
- 经常单独更新

例如以后 `user` 增加：

- `name`
- `avatar`
- `profile_blob`

可以这样设计：

- `family 0`: `token, device_flag, device_level, name`
- `family 1`: `avatar`
- `family 2`: `profile_blob`

这样：

- 查基础信息只读 `family 0`
- 读头像时才读 `family 1`
- 大对象单独放 `family 2`

这比“把所有列塞进一个大 value”更适合未来扩展。

## 9. 新增一张表时的检查清单

每增加一张新表，都检查下面这些项：

1. 是否分配了新的 `tableID`
2. 是否为每个列分配了稳定 `columnID`
3. 是否定义了 `family 0`
4. 是否决定了每个非主键列属于哪个 family
5. 是否定义了主键索引 `indexID = 1`
6. 是否把正式查询路径明确成主键或二级索引
7. 是否实现了主记录 key 编码
8. 是否实现了 family value 编码/解码
9. `INSERT` 是否会同步写所有 family 和所有索引
10. `UPDATE` 是否会只重写受影响 family，并在索引列变化时同步重建索引
11. `DELETE` 是否会同步删除所有 family 和所有索引
12. 是否考虑了旧数据兼容和 `schema_version`

## 10. 总结

为了支持未来持续新增表，真正需要规范化的不是 `user` 或 `channel` 本身，而是下面 5 件事：

- 稳定的 ID 分配规则
- 主记录按 family 拆分的 key 规则
- 借鉴 Cockroach 的 tuple/value-side encoding
- 统一的 CRUD 与索引维护语义
- 统一的新表接入流程

这份文档现在采用的是：

- 静态 schema
- Pebble 直写
- column family
- Cockroach 风格 family value 编码

这样以后继续加新表时，你不需要重新设计 value 存储模型，只需要：

1. 注册新表
2. 定义列、family、索引
3. 复用同一套 key 规则
4. 复用同一套 value-side encoding
5. 复用同一套 CRUD 语义

这才是静态表方案能够长期扩展的关键。
