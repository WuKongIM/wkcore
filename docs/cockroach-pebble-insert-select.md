# Cockroach 基于 Pebble 的表存储、INSERT 和 SELECT 链路整理

本文基于 `learn_project/cockroach` 源码，整理 CockroachDB 在 SQL 层如何
把“表”和“表数据”映射到底层 KV，再由 KV 映射到 Pebble 的过程，并重点说明：

- 表本身如何存储
- `INSERT` 是如何把一行数据写到底层 Pebble 的
- `SELECT` 是如何从 Pebble 读回一行数据的

需要先明确一个核心结论：

> Cockroach 并不是把一个 SQL 表直接映射成 Pebble 的一个“表”。
> Pebble 只负责存储有序的字节键值对（`[]byte -> []byte`）。
> “数据库、表、索引、行、事务、MVCC” 都是 Cockroach 在上层编码出来的。

## 1. 整体分层

从上到下，可以把链路大致分成 4 层：

1. SQL 层
   - 负责解析 SQL、生成执行计划、校验约束、组织行写入和行读取。
2. Row/KV 编码层
   - 负责把一行 SQL 数据编码成主索引 KV、二级索引 KV。
3. MVCC/KV 存储层
   - 负责事务、版本、锁、条件写入、扫描。
4. Pebble
   - 只负责保存最终的有序字节 key/value。

对应的关键代码位置：

- SQL 表元数据 key 编码：`pkg/keys/sql.go`
- 行与索引编码：`pkg/sql/rowenc/index_encoding.go`
- 行插入：`pkg/sql/row/inserter.go`
- 主表 value 拼装：`pkg/sql/row/writer.go`
- 行读取与解码：`pkg/sql/row/fetcher.go`
- MVCC 读写：`pkg/storage/mvcc.go`
- Pebble 封装：`pkg/storage/pebble.go`

## 2. 表本身不是 Pebble 表，而是系统表中的 KV

### 2.1 表名如何找到表 ID

Cockroach 用 `system.namespace` 保存名字到 descriptor ID 的映射。其 schema
定义在：

- `pkg/sql/catalog/systemschema/system.go`

关键片段：

```sql
CREATE TABLE system.namespace (
    "parentID" INT8,
    "parentSchemaID" INT8,
    name       STRING,
    id         INT8,
    CONSTRAINT "primary" PRIMARY KEY ("parentID", "parentSchemaID", name)
);
```

名字 key 的编码在：

- `pkg/sql/catalog/catalogkeys/keys.go`

对应函数：

- `EncodeNameKey()`

它会把：

```text
(parentID, parentSchemaID, name)
```

编码成类似下面的 key：

```text
/Table/<namespaceTableID>/<primaryIndexID>/<parentID>/<parentSchemaID>/<name>/<family>
```

写入逻辑在：

- `pkg/sql/catalog/descs/collection.go`

对应函数：

- `InsertNamespaceEntryToBatch()`
- `UpsertNamespaceEntryToBatch()`

因此，建表时“把表名注册进去”这一步，本质上就是往 `system.namespace` 写一条
KV。

### 2.2 表结构如何保存

Cockroach 用 `system.descriptor` 保存表结构本身。其 schema 定义在：

- `pkg/sql/catalog/systemschema/system.go`

关键片段：

```sql
CREATE TABLE system.descriptor (
    id         INT8,
    descriptor BYTES,
    CONSTRAINT "primary" PRIMARY KEY (id)
);
```

descriptor key 的编码在：

- `pkg/keys/sql.go`
- `pkg/sql/catalog/catalogkeys/keys.go`

关键函数：

- `DescMetadataKey()`
- `MakeDescMetadataKey()`

写 descriptor 的逻辑在：

- `pkg/sql/catalog/descs/collection.go`

对应函数：

- `WriteDescToBatch()`

它最终会把 descriptor protobuf 作为 value，按 descriptor ID 作为主键写入
`system.descriptor`。

### 2.3 查询表时如何解析名字

名字解析的典型流程是：

1. 先从内存 cache / uncommitted descriptors 查。
2. 如果没命中，再去 `system.namespace` 查名字对应的 ID。
3. 再根据 descriptor ID 去 `system.descriptor` 取出 descriptor。

相关代码：

- `pkg/sql/catalog/descs/collection.go`
  - `LookupObjectID()`
- `pkg/sql/catalog/internal/catkv/catalog_reader.go`
  - `GetByNames()`

所以：

> SQL 里的“表 t”最终会先被解析成一个 descriptor ID，再变成对应索引上的 key span。

## 3. 一个具体例子

下面用一个最小例子说明整条链路：

```sql
CREATE TABLE t(
  id   INT PRIMARY KEY,
  name STRING,
  age  INT,
  INDEX idx_name(name)
);

INSERT INTO t VALUES (1, 'a', 18);
```

为了方便描述，假设：

- 表 `t` 的 descriptor ID 是 `tID`
- 主索引 ID 是 `1`
- 二级索引 `idx_name` 的 index ID 是 `2`

那么：

- `system.namespace` 中会有一条记录把名字 `t` 映射到 `tID`
- `system.descriptor` 中会有一条记录把 `tID` 映射到表 descriptor protobuf

普通用户表数据则会写到：

- 主索引 key 空间：`/Table/tID/1/...`
- 二级索引 key 空间：`/Table/tID/2/...`

## 4. INSERT 详细链路

下面详细看：

```sql
INSERT INTO t VALUES (1, 'a', 18);
```

### 4.1 SQL 层先构造 insertNode

优化器在：

- `pkg/sql/opt_exec_factory.go`

中通过 `ConstructInsert()` 创建：

- `row.Inserter`
- `insertNode`
- `tableInserter`

对应关键调用：

```go
ri, err := row.MakeInserter(...)
ins := insertNode{ ... ti: tableInserter{ri: ri} ... }
```

这一步只是把“如何写一行”准备好，还没有真正写 KV。

### 4.2 insertNode 逐行处理输入

本地执行入口在：

- `pkg/sql/insert.go`

关键函数：

- `insertNode.startExec()`
- `insertNode.processBatch()`
- `insertRun.processSourceRow()`

`processSourceRow()` 对每一行会做：

1. 提取插入列值
2. `NOT NULL` 校验
3. `CHECK` 校验
4. partial index 判定
5. vector index 相关准备
6. 调用 `tableInserter.row()` 把当前行排入 KV batch

### 4.3 tableInserter 调用 row.Inserter.InsertRow

`tableInserter` 位于：

- `pkg/sql/tablewriter_insert.go`

真正执行插入的是：

```go
ti.ri.InsertRow(ctx, &ti.putter, values, pm, vh, oth, row.CPutOp, traceKV)
```

也就是说，SQL 层接下来就进入了 Row/KV 编码层。

### 4.4 InsertRow 先编码主索引和二级索引

代码位于：

- `pkg/sql/row/inserter.go`
- `pkg/sql/row/helper.go`
- `pkg/sql/rowenc/index_encoding.go`

关键路径：

1. `Inserter.InsertRow()`
2. `RowHelper.encodeIndexes()`
3. `encodePrimaryIndexKey()`
4. `EncodeSecondaryIndex()`

对例子里的 `(1, 'a', 18)`，逻辑上会生成：

#### 主索引行键

主索引前缀：

```text
/Table/tID/1
```

编码主键 `id=1` 之后：

```text
/Table/tID/1/1
```

再追加 family 0：

```text
/Table/tID/1/1/0
```

#### 二级索引键

二级索引前缀：

```text
/Table/tID/2
```

编码索引列 `name='a'` 后：

```text
/Table/tID/2/'a'
```

由于 `idx_name(name)` 是非唯一索引，Cockroach 还会把主键后缀追加到二级索引
key 上，以保证唯一性：

```text
/Table/tID/2/'a'/1
```

再追加 family 0：

```text
/Table/tID/2/'a'/1/0
```

### 4.5 主表 value 如何编码

主表 value 的构造在：

- `pkg/sql/row/writer.go`

关键函数：

- `prepareInsertOrUpdateBatch()`

对于这个例子：

- `id` 已经编码进主索引 key
- `name` 和 `age` 会编码进 value

因此主表 KV 逻辑上大致是：

```text
Key   = /Table/tID/1/1/0
Value = tuple(name='a', age=18)
```

如果表有多个 column family，那么一行会拆成多个 KV，每个 family 一个 KV。
如果某个 family 中所有列都是 `NULL`，该 family 对应的 KV 可以省略。

### 4.6 二级索引 value 如何编码

二级索引编码也在：

- `pkg/sql/rowenc/index_encoding.go`

对于这个例子，`idx_name` 没有 `STORING` 列，所以二级索引 KV 大致是：

```text
Key   = /Table/tID/2/'a'/1/0
Value = empty bytes
```

如果索引包含 `STORING` 列，或者存在特殊索引类型，则 value 中也可能带列值。

### 4.7 主键冲突为什么能检测到

插入时，主表通常走 `CPut` 语义，而不是普通 `Put`。

相关代码：

- `pkg/sql/row/inserter.go`

关键点：

- 主索引默认使用 `CPut`
- 唯一二级索引通常也会使用 `CPut`
- 非唯一二级索引一般使用 `Put`

所以这个例子更接近下面的逻辑：

```text
CPut /Table/tID/1/1/0 -> tuple(name='a', age=18)
Put  /Table/tID/2/'a'/1/0 -> empty
```

也正因为主索引使用条件写，重复主键会在 KV 层直接报冲突。

### 4.8 INSERT 不是每行直接落盘，而是先进 KV batch

批量写入的公共逻辑在：

- `pkg/sql/tablewriter.go`

关键函数：

- `tableWriterBase.init()`
- `flushAndStartNewBatch()`
- `finalize()`

`insertNode.processBatch()` 会不断把多行插入累积进当前 `kv.Batch`。

达到以下任一阈值就会 flush：

- `maxBatchSize`
- `maxBatchByteSize`

最终：

- 中间批次走 `txn.Run(batch)`
- 最后一批走 `finalize()`
- 在 auto commit 场景下，最后一批可能直接 `CommitInBatch()`

因此：

> SQL 层看到的是“逐行 INSERT”，但真正下沉到 KV 层时通常是“批量提交一批 KV 请求”。

### 4.9 KV 请求如何进入 MVCC 层

`kv.Batch` 提交给事务层后，会下沉到存储层。

由于主键写通常是 `CPut`，实际可能走：

- `MVCCConditionalPut()`

代码位置：

- `pkg/storage/mvcc.go`

相关函数：

- `MVCCConditionalPut()`
- `mvccConditionalPutUsingIter()`

而普通非条件写则可能走：

- `MVCCPut()`

无论走哪一条路径，最终都会转化为对某个 MVCC key 的写入。

### 4.10 最终如何落到 Pebble

Pebble 封装在：

- `pkg/storage/pebble.go`
- `pkg/storage/pebble_batch.go`

关键点：

- `Pebble` 本身只是 `*pebble.DB` 的包装
- 最终写入调用是 `db.Set(...)`

关键函数：

- `Pebble.put()`
- `writeBatch.put()`

其中最重要的一步是：

```go
EncodeMVCCKey(key)
```

也就是说，逻辑上的：

```text
/Table/tID/1/1/0
```

在真正进入 Pebble 前，会先变成：

```text
EncodeMVCCKey({
  Key: /Table/tID/1/1/0,
  Timestamp: ts,
})
```

所以，Pebble 里存的并不是“SQL 行”，而是“带 MVCC 时间戳版本的底层字节键”。

### 4.11 INSERT 链路总结

对这一条语句：

```sql
INSERT INTO t VALUES (1, 'a', 18);
```

可以总结成：

```text
SQL row
  -> insertNode / tableInserter
  -> row.Inserter.InsertRow
  -> 编码主索引 KV 和二级索引 KV
  -> 放入 kv.Batch
  -> Txn.Run / CommitInBatch
  -> MVCCConditionalPut / MVCCPut
  -> EncodeMVCCKey
  -> Pebble.Set
```

## 5. SELECT 详细链路

下面看两个典型查询：

```sql
SELECT * FROM t WHERE id = 1;
SELECT * FROM t WHERE name = 'a';
```

第一条通常是一跳主键查找，第二条通常是“二级索引 + 回表”两跳查找。

## 6. SELECT * FROM t WHERE id = 1

### 6.1 优化器先选主索引

扫描计划的构造在：

- `pkg/sql/opt_exec_factory.go`

关键函数：

- `ConstructScan()`
- `generateScanSpans()`

这里会把谓词 `id = 1` 转成主索引上的一个精确 span。承载这个信息的是：

- `scanNode`

其定义在：

- `pkg/sql/scan.go`

`scanNode` 本身只是逻辑/物理规划中的一个扫描节点描述，里面最关键的是：

- 要扫哪个表
- 要扫哪个索引
- 要扫哪些 spans
- 是否 reverse
- limit hint

### 6.2 执行时通常变成 tableReader

真正读取 KV 的处理器通常是：

- `pkg/sql/rowexec/tablereader.go`

关键函数：

- `tableReader.Start()`
- `tableReader.startScan()`
- `tableReader.Next()`

在 `startScan()` 中，`tableReader` 会调用：

- `Fetcher.StartScan()`

对应代码：

- `pkg/sql/row/fetcher.go`

### 6.3 Fetcher 发起 KV Scan

`Fetcher.StartScan()` 底下会使用 KV fetcher 构造批量读请求。

相关代码：

- `pkg/sql/row/kv_batch_fetcher.go`

其中底层发送最终走的是：

- `txn.Send(batchRequest)`

也就是说：

> SQL 层并不是直接用 Pebble iterator 扫，而是先构造 KV spans，再让 KV 层去执行 scan。

### 6.4 KV 层执行 MVCCScan

存储层入口在：

- `pkg/storage/mvcc.go`

关键函数：

- `MVCCScan()`

`MVCCScan()` 内部会创建 MVCC iterator，然后交给：

- `pebbleMVCCScanner`

对应实现位于：

- `pkg/storage/pebble_mvcc_scanner.go`

这个 scanner 做的事情包括：

1. 用 Pebble iterator seek 到起始位置
2. 按 MVCC 可见性规则挑选版本
3. 处理 intent / lock
4. 处理 resume span
5. 返回 scan 结果

因此，对 `SELECT * FROM t WHERE id = 1` 来说，存储层实际会扫到类似：

```text
/Table/tID/1/1/0@<visible-ts>
```

### 6.5 Fetcher 把 KV 解码回一行

返回到 SQL 层后，`row.Fetcher` 负责把 KV 还原成 SQL 行。

关键函数位于：

- `pkg/sql/row/fetcher.go`

典型路径：

- `nextKey()`
- `DecodeIndexKey()`
- `processKV()`
- `NextRow()`

对主键查询来说：

1. `DecodeIndexKey()` 从 key 中解出 `id=1`
2. `processKV()` 从 value 中解出 `name='a'`、`age=18`
3. `NextRow()` 组装成一行返回

因为这里只有一个 family，所以通常只需要一个 KV 就能组成整行。

### 6.6 主键查询总结

这条查询：

```sql
SELECT * FROM t WHERE id = 1;
```

本质上是一跳：

```text
生成主索引 span
  -> MVCCScan
  -> 从 Pebble 取到主索引 KV
  -> DecodeIndexKey + DecodeValue
  -> 返回整行
```

## 7. SELECT * FROM t WHERE name = 'a'

这一条更能体现 Cockroach 的“索引查询 + 回表”机制。

### 7.1 先走二级索引

优化器会优先考虑 `idx_name(name)`，于是第一阶段扫的不是主索引，而是：

```text
/Table/tID/2/'a'
```

扫描到的索引 KV 类似：

```text
Key   = /Table/tID/2/'a'/1/0
Value = empty
```

这里已经能拿到：

- 索引列：`name='a'`
- 主键后缀：`id=1`

### 7.2 为什么不能直接返回结果

因为查询是：

```sql
SELECT * FROM t WHERE name = 'a';
```

需要返回：

- `id`
- `name`
- `age`

而 `age` 并不在 `idx_name` 的 key 或 value 中，所以光扫二级索引还不够。

这时就需要第二阶段：回主索引取整行。

### 7.3 回表通常由 joinReader / index join 完成

相关代码在：

- `pkg/sql/rowexec/joinreader.go`

`joinReader` 注释里明确写了 lookup join 的实现方式：

1. 先读输入行
2. 把输入映射为 lookup key
3. 执行索引 lookup
4. 把 lookup 结果和输入行 join 起

对本例来说，第一阶段索引扫描的输出行会被转换成主表 lookup span，大致对应：

```text
/Table/tID/1/1
```

### 7.4 joinReader 再次调用 Fetcher.StartScan

在：

- `pkg/sql/rowexec/joinreader.go`

中，`joinReader.readInput()` 会：

1. 收集输入批次
2. 根据输入行构造 lookup spans
3. 调用 `fetcher.StartScan()` 对这些 spans 做第二次扫描

然后 `fetchLookupRow()` 会不断调用：

- `fetcher.NextRow()`

把 lookup 回来的主表行读出来。

### 7.5 回表阶段读到的是什么

第二阶段回表时，真正读到的是主索引上的 KV：

```text
/Table/tID/1/1/0
```

随后和前面一样：

1. `DecodeIndexKey()` 解出 `id=1`
2. value 解出 `name='a'`、`age=18`
3. joinReader 把这行作为 lookup 结果与索引侧输入关联起来

### 7.6 二级索引查询总结

所以：

```sql
SELECT * FROM t WHERE name = 'a';
```

典型上会走两跳：

```text
第一跳：扫二级索引
  /Table/tID/2/'a' -> 得到主键 id=1

第二跳：回主索引
  /Table/tID/1/1 -> 取完整行
```

如果查询变成：

```sql
SELECT id FROM t WHERE name = 'a';
```

那么很多时候不需要回表，因为：

- `id` 已经在二级索引 key 的主键后缀里

这时可以直接做 index-only scan。

## 8. 从 Pebble 视角看，实际存的是什么

无论是 INSERT 还是 SELECT，Pebble 看到的都不是 SQL 表，而是字节 key。

对例子中的这一行：

```sql
INSERT INTO t VALUES (1, 'a', 18);
```

逻辑上会有两条用户表 KV：

```text
/Table/tID/1/1/0      -> tuple(name='a', age=18)
/Table/tID/2/'a'/1/0  -> empty
```

但进入 Pebble 前都会再套一层 MVCC 编码：

```text
EncodeMVCCKey(/Table/tID/1/1/0, ts)      -> value
EncodeMVCCKey(/Table/tID/2/'a'/1/0, ts)  -> value
```

因此可以把 Cockroach 对 Pebble 的使用概括成：

```text
SQL 表/索引/行
  -> 编码成 KV key/value
  -> 再编码成 MVCC key/value
  -> 存进 Pebble
```

## 9. 总结

### 9.1 表如何通过 Pebble 实现

不是“一个 SQL 表对应 Pebble 一个表”，而是：

- 表名和表 ID 的关系存在 `system.namespace`
- 表结构存在 `system.descriptor`
- 普通行数据和索引数据都拆成 KV
- 这些 KV 最终以 MVCC 编码后的字节 key/value 存在 Pebble

### 9.2 INSERT 如何实现

`INSERT` 的核心链路是：

```text
insertNode
  -> tableInserter
  -> row.Inserter.InsertRow
  -> 编码主索引 KV / 二级索引 KV
  -> 放入 kv.Batch
  -> Txn.Run / CommitInBatch
  -> MVCCConditionalPut / MVCCPut
  -> Pebble.Set
```

### 9.3 SELECT 如何实现

`SELECT` 的核心链路是：

```text
scanNode / joinReader
  -> 生成索引 spans
  -> Fetcher.StartScan
  -> KV Scan / MVCCScan
  -> pebble iterator
  -> Fetcher.DecodeIndexKey + processKV
  -> 组装成 SQL row
```

### 9.4 两类查询的差异

- 主键查询：通常一跳，直接扫主索引
- 非覆盖二级索引查询：通常两跳，先扫二级索引，再回主索引

---

如果后续还要继续深入，建议下一步直接跟这几个函数读源码：

- `pkg/sql/row/inserter.go: InsertRow`
- `pkg/sql/row/writer.go: prepareInsertOrUpdateBatch`
- `pkg/sql/row/fetcher.go: NextRow`
- `pkg/storage/mvcc.go: MVCCScan / MVCCConditionalPut / MVCCPut`
- `pkg/storage/pebble_mvcc_scanner.go: pebbleMVCCScanner`
- `pkg/storage/pebble.go: put`
