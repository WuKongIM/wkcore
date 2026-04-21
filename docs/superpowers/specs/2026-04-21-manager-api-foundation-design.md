# 管理 API 基础框架设计

- 日期：2026-04-21
- 范围：独立 manager API 服务、JWT 登录与权限、节点列表接口
- 关联目录：
  - `internal/access/api`
  - `internal/app`
  - `cmd/wukongim`
  - `pkg/cluster`
  - `learn_project/WuKongIM/internal/api/server_manager.go`
  - `learn_project/WuKongIM/pkg/auth`

## 1. 背景

当前 `internal/access/api` 暴露的 HTTP API 主要面向第三方内部业务服务，例如路由查询、token 更新、消息发送、会话同步。这类 API 的特点是：

- 入口开放，默认无 JWT 登录态
- 关注消息和接入侧业务动作
- 与后台管理系统的访问模型不同

本次需要为后台管理系统新增一套独立的 manager API 服务，满足以下前提：

- 与现有业务 API 不同端口
- 使用 JWT 登录态
- 支持资源级权限校验
- 第一版先只提供一个节点列表接口，用于把管理服务框架跑通

第一版的目标不是一次性补齐所有管理能力，而是先把“独立端口 + 登录 + 权限 + 管理接口聚合查询 + app 装配”这条链路建立起来，为后续扩展 `cluster / slot / channel runtime meta` 管理接口提供稳定骨架。

## 2. 目标与非目标

## 2.1 目标

本次设计目标如下：

1. 新增独立的 manager API 服务，不复用当前 `internal/access/api` 的服务实例与监听端口
2. 提供 `POST /manager/login`，基于配置中的静态用户签发 JWT
3. 提供 `GET /manager/nodes`，要求 JWT 与权限校验通过后才能访问
4. 节点列表返回适合后台展示的稳定 DTO，而不是直接透传内部控制面模型
5. 在 `internal/app` 中将 manager 服务作为独立入口装配进应用生命周期
6. 在 `cmd/wukongim/config.go` 和 `wukongim.conf.example` 中补齐 manager 配置

## 2.2 非目标

第一版明确不做以下内容：

- 不实现 slot、channel runtime meta、迁移任务等更多 manager 接口
- 不实现基于数据库或外部 IAM 的动态账号体系
- 不实现刷新 token、登出、黑名单、会话吊销
- 不实现细粒度的写操作权限校验链路
- 不兼容旧版 manager server 的全部功能，只借鉴其“登录 + JWT + 权限”模式

## 3. 设计原则

## 3.1 管理入口与业务入口分离

`internal/access/api` 继续服务第三方内部业务系统；manager API 专门服务后台管理系统。两者必须：

- 使用不同 HTTP server
- 使用不同监听地址
- 使用不同认证方式
- 保持各自职责边界清晰

## 3.2 继续遵守现有分层约定

遵循仓库现有约定：

- `internal/access/*` 只做入口协议适配
- `internal/usecase/*` 承载可复用业务编排
- `internal/app` 继续作为唯一组合根

因此 manager 的聚合查询逻辑不直接堆进 HTTP handler，而是下沉到新的 `internal/usecase/management`。

## 3.3 DTO 面向管理系统稳定，而不是面向内部模型稳定

后台管理系统需要的是稳定的展示与管理接口，而不是内部控制面结构的直接镜像。第一版虽然只有一个节点列表接口，也应定义独立 DTO，避免把 manager API 与 `pkg/controller/meta` 直接耦合。

## 3.4 第一版只搭骨架，不做过度抽象

第一版只做：

- 静态用户认证
- JWT 签发与解析
- 资源/动作权限校验
- 节点列表接口

不为了未来所有管理能力提前抽出过重的统一鉴权框架或通用插件体系。

## 4. 包结构设计

新增以下结构：

```text
internal/
  access/
    manager/            manager HTTP 入口、路由、中间件
  usecase/
    management/         manager 聚合查询用例
```

其中职责约束如下。

## 4.1 `internal/access/manager`

负责：

- 创建独立 gin server
- 注册 manager 路由
- 解析登录请求
- JWT 中间件
- 权限中间件
- 调用 management 用例并输出 JSON

不负责：

- 节点角色统计逻辑
- slot 统计逻辑
- controller 角色推导逻辑

## 4.2 `internal/usecase/management`

第一版新增一个很薄的节点聚合查询用例，例如：

- `ListNodes(ctx)` 或语义等价接口

负责：

- 调用 cluster 只读接口
- 聚合节点、slot runtime view、controller 角色信息
- 输出 manager 节点 DTO

这样后续继续扩展 `slot`、`channel runtime meta` 管理接口时，仍能延续同一个 usecase 包，而不会把查询聚合逻辑散落在 access 层。

## 5. 配置设计

## 5.1 新增配置项

第一版新增以下配置：

```conf
WK_MANAGER_LISTEN_ADDR=0.0.0.0:5301
WK_MANAGER_AUTH_ON=true
WK_MANAGER_JWT_SECRET=change-me
WK_MANAGER_JWT_ISSUER=wukongim-manager
WK_MANAGER_JWT_EXPIRE=24h
WK_MANAGER_USERS=[{"username":"admin","password":"admin123","permissions":[{"resource":"cluster.node","actions":["r"]}]}]
```

配置语义：

- `WK_MANAGER_LISTEN_ADDR`
  - manager 服务监听地址
  - 为空表示 manager 服务关闭
- `WK_MANAGER_AUTH_ON`
  - 是否启用 manager 鉴权
  - 第一版默认建议开启
- `WK_MANAGER_JWT_SECRET`
  - JWT 签名密钥
- `WK_MANAGER_JWT_ISSUER`
  - JWT `iss`
- `WK_MANAGER_JWT_EXPIRE`
  - JWT 过期时间
- `WK_MANAGER_USERS`
  - 静态用户列表，包含用户名、密码、权限

## 5.2 配置模型

建议在 `internal/app/config.go` 中新增：

- `ManagerConfig`
- `ManagerAuthConfig`
- `ManagerUserConfig`
- `ManagerPermissionConfig`

其中 `ManagerConfig` 至少包含：

- `ListenAddr string`
- `AuthOn bool`
- `JWTSecret string`
- `JWTIssuer string`
- `JWTExpire time.Duration`
- `Users []ManagerUserConfig`

`ManagerUserConfig` 至少包含：

- `Username string`
- `Password string`
- `Permissions []ManagerPermissionConfig`

`ManagerPermissionConfig` 至少包含：

- `Resource string`
- `Actions []string`

第一版不额外设计数据库模型或外部 provider。

## 5.3 启用与校验规则

建议规则：

- `WK_MANAGER_LISTEN_ADDR` 为空时：manager 服务关闭，不做 manager 配置强校验
- `WK_MANAGER_LISTEN_ADDR` 非空且 `WK_MANAGER_AUTH_ON=true` 时：
  - `JWTSecret` 必须非空
  - `Users` 必须非空
- `JWTExpire` 必须大于 0
- 用户名不能为空
- 密码不能为空
- 权限资源不能为空
- 权限动作必须属于 `r`、`w`、`*`

## 5.4 示例配置

`wukongim.conf.example` 需要新增 manager 示例段，且强调：

- manager 是独立服务
- 端口不同于 `WK_API_LISTEN_ADDR`
- 生产环境必须替换 JWT secret 与默认密码

## 6. 认证与权限模型

## 6.1 登录模式

第一版沿用旧项目的模式：

- manager 用户来自配置文件
- `POST /manager/login` 提交用户名/密码
- 服务端认证成功后签发 JWT

不采用“外部已签 JWT 直接透传”的模式，因为本次目标之一就是把 manager 服务自己的认证链路搭建完整。

## 6.2 JWT Claims

第一版 JWT claims 保持最小集：

- `iss`
- `iat`
- `exp`
- `username`

不在 JWT 中固化权限快照。

## 6.3 权限校验策略

第一版权限模型保留最小可扩展能力：

- 动作：`r`、`w`、`*`
- 资源：第一版只定义 `cluster.node`

接口与权限关系：

- `GET /manager/nodes` -> `cluster.node:r`

校验过程：

1. JWT 中间件验证签名与时效
2. 从 claims 提取 `username`
3. 权限中间件根据当前内存配置查询该用户权限
4. 判断是否具备目标资源的目标动作

注意：服务端不直接信任 token 内自带权限，避免权限配置更新后旧 token 仍长期保留旧权限快照。

## 6.4 资源与动作常量

建议在 manager 包内定义最小权限常量，例如：

- 资源：`cluster.node`
- 动作：`r`、`w`、`*`

第一版不抽出全仓库共享的 `pkg/auth`，避免在只有一个 manager 接口的阶段引入过重改造。待 manager 能力扩展后再评估是否上提为共用包。

## 7. HTTP 接口设计

## 7.1 `POST /manager/login`

请求：

```json
{
  "username": "admin",
  "password": "admin123"
}
```

成功响应：

```json
{
  "username": "admin",
  "token_type": "Bearer",
  "token": "<jwt>",
  "expires_at": "2026-04-22T10:00:00+08:00",
  "permissions": [
    {
      "resource": "cluster.node",
      "actions": ["r"]
    }
  ]
}
```

错误语义：

- 用户名或密码错误：`401`
- 请求体非法：`400`
- manager 鉴权未正确配置：`500`

## 7.2 `GET /manager/nodes`

请求头：

```text
Authorization: Bearer <jwt>
```

成功响应：

```json
{
  "items": [
    {
      "node_id": 1,
      "addr": "127.0.0.1:7000",
      "status": "alive",
      "last_heartbeat_at": "2026-04-21T10:00:00+08:00",
      "controller_role": "leader",
      "slot_count": 32,
      "leader_slot_count": 12,
      "is_local": true,
      "capacity_weight": 1
    }
  ]
}
```

错误语义：

- 未带 token / token 非法 / token 过期：`401`
- 权限不足：`403`
- 底层 cluster 查询失败：`500`

第一版不做分页、过滤、排序参数，避免在框架搭建阶段引入不必要复杂度。

## 8. 节点列表 DTO 设计

节点列表第一版返回如下字段：

- `node_id`
- `addr`
- `status`
- `last_heartbeat_at`
- `controller_role`
- `slot_count`
- `leader_slot_count`
- `is_local`
- `capacity_weight`

字段说明如下。

## 8.1 基础字段

- `node_id`
  - 节点 ID
- `addr`
  - 集群监听地址
- `status`
  - 节点状态，字符串化输出，建议值：
    - `unknown`
    - `alive`
    - `suspect`
    - `dead`
    - `draining`
- `last_heartbeat_at`
  - 最近心跳时间，RFC3339 格式输出
- `capacity_weight`
  - 节点容量权重

## 8.2 角色字段

不建议只返回一个笼统的 `role` 字段，因为节点在控制面与数据面上可能同时扮演多种角色。第一版用两个更明确的聚合视角：

- `controller_role`
  - `leader`
  - `follower`
  - `none`
- `leader_slot_count`
  - 当前节点作为 slot leader 的数量

这样后台列表既能看到控制面角色，也能看到数据面承载强度。

## 8.3 统计字段

- `slot_count`
  - 节点出现在多少个 slot 的 `CurrentPeers` 中
- `leader_slot_count`
  - 节点在多少个 slot runtime view 中是 `LeaderID`
- `is_local`
  - 是否为当前节点

## 9. 节点数据聚合设计

## 9.1 数据来源

`internal/usecase/management` 的节点聚合查询需要以下输入：

- `cluster.ListNodes(ctx)`
- `cluster.ListObservedRuntimeViews(ctx)`
- 当前本地节点 ID
- controller peer 列表
- controller leader ID

## 9.2 controller 角色推导

controller 角色按以下规则计算：

- 若节点 ID 等于 controller leader ID，则 `controller_role = leader`
- 否则若节点 ID 在 controller peer 列表中，则 `controller_role = follower`
- 否则为 `none`

第一版 controller peer 列表可直接基于 `cfg.Cluster.DerivedControllerNodes()` 注入 usecase，而不额外扩展更多 cluster 读接口。

## 9.3 slot 统计推导

对 `cluster.ListObservedRuntimeViews(ctx)` 返回的每个 view：

- 对 `CurrentPeers` 中的每个 peer，`slot_count++`
- 若 `LeaderID != 0`，则对应节点 `leader_slot_count++`

若某节点存在于 `ListNodes()` 中但未出现在任何 runtime view 中，则其 `slot_count` 和 `leader_slot_count` 为 0。

## 9.4 排序规则

节点列表按 `node_id` 升序返回，保证后台管理系统能得到稳定展示顺序。

## 10. Cluster API 边界调整

为避免 `management` 用例依赖具体 `*cluster.Cluster` 实现，建议给 `pkg/cluster.API` 新增一个很小的只读接口：

- `ControllerLeaderID() uint64`

原因：

- 第一版 manager 用例需要稳定拿到 controller leader
- 现有 `API` 已暴露多个只读运维接口，新增一个 leader 只读查询符合当前接口演进方向
- 可避免在 usecase 层做具体实现类型断言

controller peer 列表则不需要进 `pkg/cluster.API`，第一版直接从 `app.Config` 注入即可，减少改动面。

## 11. `internal/app` 装配设计

## 11.1 App 结构

建议在 `internal/app.App` 中新增：

- manager server 字段
- manager start/stop 生命周期钩子
- 对应运行状态标记

manager 服务与现有业务 API 服务并列存在，互不替代。

## 11.2 Build 装配

在 `internal/app/build.go` 中：

1. 构建 management usecase
2. 若 `cfg.Manager.ListenAddr != ""`，则构建 manager access server
3. 注入：
   - management usecase
   - manager 鉴权配置
   - logger

## 11.3 生命周期

在 `internal/app/lifecycle.go` 中：

- `Start()` 需要独立启动 manager 服务
- `Stop()` 需要独立关闭 manager 服务
- manager 服务失败不应影响业务 API 的边界定义，但在当前应用启动流程中，只要某个入口显式配置开启且启动失败，应整体启动失败

这与当前 API/gateway 的装配语义一致。

## 12. 错误处理策略

第一版保持简单明确，不做部分成功降级：

- 登录失败返回 `401`
- token 缺失或非法返回 `401`
- 权限不足返回 `403`
- cluster 查询失败返回 `500`

节点列表接口在底层数据源查询失败时整体失败，不返回“部分节点成功”的响应，避免把 `slot_count`、`leader_slot_count` 的零值误导为真实状态。

## 13. 测试设计

## 13.1 `internal/access/manager`

至少覆盖：

- `POST /manager/login` 成功
- `POST /manager/login` 用户名错误
- `POST /manager/login` 密码错误
- `GET /manager/nodes` 未带 token
- `GET /manager/nodes` token 非法
- `GET /manager/nodes` token 过期
- `GET /manager/nodes` 权限不足
- `GET /manager/nodes` 成功返回节点列表

## 13.2 `internal/usecase/management`

至少覆盖：

- controller role 计算正确
- `slot_count` 统计正确
- `leader_slot_count` 统计正确
- 结果按 `node_id` 排序
- 无 runtime view 时统计为 0

## 13.3 `internal/app`

至少覆盖：

- 配置 manager listen addr 时构建 manager server
- manager 生命周期纳入 `App.Start()` / `App.Stop()`
- manager 关闭时不会影响现有 API/gateway 构建

## 13.4 `cmd/wukongim`

至少覆盖：

- `WK_MANAGER_*` 配置解析
- manager 开启时的配置校验
- `WK_MANAGER_USERS` JSON 解析

## 14. 风险与后续扩展

## 14.1 当前风险

- 静态用户密码直接放配置，适合第一版框架验证，但不适合作为长期企业认证方案
- JWT 不做吊销与黑名单，权限收紧只能依赖 token 到期或服务端实时按用户名重新判权
- 节点列表中的 slot 统计依赖 `ListObservedRuntimeViews()`，若观察视图暂时落后，展示会随观察延迟而变化

## 14.2 扩展路径

在此骨架上，后续可以按同一模式继续扩展：

- `GET /manager/slots`
- `GET /manager/slots/:id`
- `GET /manager/channel-runtime-meta`
- `GET /manager/channel-runtime-meta/:channel_id`
- 写操作接口与 `w` 权限

这些后续接口仍应遵循：

- access 层只负责协议与鉴权
- usecase 层负责聚合查询或管理编排
- app 层统一装配

## 15. 决策摘要

本次设计最终确定以下关键决策：

1. manager API 使用独立端口与独立 server，不复用当前业务 API
2. 第一版只做一个节点列表接口，但完整搭建登录、JWT、权限和 app 装配链路
3. 鉴权采用“静态用户 + 登录签发 JWT + 服务端按用户名实时判权”
4. 新增 `internal/usecase/management`，避免把聚合逻辑堆进 access 层
5. 节点列表返回稳定 DTO，字段覆盖后台首屏所需的基础信息与角色统计信息
6. `pkg/cluster.API` 增加最小只读能力 `ControllerLeaderID() uint64`

这套方案优先解决“管理面入口框架缺失”的问题，并为后续分布式数据管理接口扩展提供稳定基础。
