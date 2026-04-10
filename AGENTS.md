# AGENTS.md

## 项目概览

本仓库是 `WuKongIM` 的 Go 单仓，当前核心方向包括：

- 网关接入与协议适配
- 消息相关用例编排
- 集群、Raft、存储运行时
- 最小 HTTP API 入口

代码组织采用“薄入口 + 可复用用例 + 本地运行时 + 单一组合根”原则，避免把业务继续堆回一个泛化的 `service` 层。

## 运行语义约束

- 本项目没有独立的单机语义；单节点部署统一视为“单节点集群”。
- 新增或修改功能时，不再引入绕过集群语义的业务分支；文档、注释、测试命名在描述部署形态时统一使用“单节点集群”，仅节点内行为保留“本地”表述。

## 常用命令

- 单元测试：`go test ./...`
- 集成测试：`go test -tags=integration ./...`
- 运行主程序：`go run ./cmd/wukongim`
- 显式指定配置文件：`go run ./cmd/wukongim -config ./wukongim.conf`
- 定向测试：`go test ./internal/... ./pkg/...`

## 配置约定

- 主配置文件使用 `wukongim.conf`，格式为 `KEY=value`。
- 文件键名与环境变量键名统一，均使用 `WK_` 前缀。
- 不传 `-config` 时，程序按 `./wukongim.conf`、`./conf/wukongim.conf`、`/etc/wukongim/wukongim.conf` 顺序查找。
- 环境变量优先级高于配置文件；列表字段使用 JSON 字符串整体覆盖。
- 仓库根目录提供 `wukongim.conf.example` 作为单节点集群示例。

## 目录结构

```text
cmd/
  wukongim/              程序入口，负责读取配置并启动应用

internal/
  app/                   组合根；负责 build、lifecycle、config
  access/                接入层，只做入口适配
    gateway/             网关 frame -> usecase 的适配
    api/                 Gin HTTP API 入口骨架
  usecase/               可复用业务用例，不依赖具体入口协议
    message/             消息发送、回执等用例
  runtime/               节点内运行时原语
    online/              在线会话注册与本地投递
    sequence/            序列号分配
  gateway/               通用网关基础设施：transport、protocol、session、core

pkg/
  replication/           复制与一致性运行时
    isr/                 单 group ISR 复制库
    isrnode/             多 ISR group 的节点内运行时
    multiraft/           Multi-Raft 基础库
  transport/             节点间传输抽象
    nodetransport/       节点间 transport / RPC
  protocol/              协议对象与编解码
    wkframe/             WuKong frame/object 模型
    wkcodec/             WuKong 二进制协议编解码
    wkjsonrpc/           JSON-RPC schema 与 frame bridge
  storage/               元数据、Raft 持久化与消息日志
    metadb/              业务元数据存储
    raftstorage/         Raft 存储实现
    metastore/           分布式元数据 facade
    metafsm/             元数据状态机与命令编解码
    channellog/          Channel 消息日志与元数据 facade
  cluster/               集群运行时
    raftcluster/         集群路由、转发与发现

docs/
  superpowers/           设计文档与实施计划

learn_project/           调研/实验代码，非主执行路径
```

## 分层约定

- `internal/access/*` 只做入口协议适配，不承载通用业务规则。
- `internal/usecase/*` 承载业务编排，输入输出应尽量入口无关。
- `internal/runtime/*` 放节点内可复用运行时能力，不放入口逻辑。
- `internal/app/*` 是唯一组合根；依赖装配只放这里。
- `internal/gateway/*` 放网关通用基础设施，不放面向具体业务的用例编排。

## 变更规则

- 新增 HTTP、RPC、任务入口时，优先落到 `internal/access/<entry>`。
- 新增可复用业务能力时，优先落到 `internal/usecase/<domain>`。
- 新增本地状态、在线路由、分配器等能力时，优先落到 `internal/runtime/<capability>`。
- 不再引入新的“大而全 service 包”或新的全局聚合服务对象。

## 提交前检查

- 至少运行与改动相关的测试。
- 若改动跨层，优先补 `internal/app` 装配测试或入口集成测试。
- 保持依赖方向清晰：`access -> usecase/runtime`，`usecase -> runtime/pkg`，`app -> all`。
