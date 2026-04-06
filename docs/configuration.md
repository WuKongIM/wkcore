# Configuration

## Overview

`WuKongIM` 通过 `wukongim.conf` 和环境变量加载启动配置。

- 配置文件格式是 `KEY=value`
- 配置文件键名与环境变量键名完全一致
- 键名统一使用 `WK_` 前缀
- 单节点部署统一按“单节点集群”描述

仓库根目录提供示例文件 `wukongim.conf.example`。

## Startup

直接启动时，程序按以下顺序查找默认配置文件：

1. `./wukongim.conf`
2. `./conf/wukongim.conf`
3. `/etc/wukongim/wukongim.conf`

示例：

```bash
go run ./cmd/wukongim
```

也可以显式指定配置文件路径：

```bash
go run ./cmd/wukongim -config ./wukongim.conf
```

如果默认路径没有配置文件，但环境变量已经提供了完整配置，程序也可以直接启动。

## Environment Override

环境变量优先级高于配置文件。

示例：

```bash
export WK_API_LISTEN_ADDR=127.0.0.1:5001
go run ./cmd/wukongim -config ./wukongim.conf
```

## Key Shape

标量字段直接写字符串值：

```conf
WK_NODE_ID=1
WK_NODE_NAME=node-1
WK_NODE_DATA_DIR=./data/node-1
WK_CLUSTER_LISTEN_ADDR=127.0.0.1:7000
WK_CLUSTER_FORWARD_TIMEOUT=2s
```

列表字段使用 JSON 字符串整体写入：

```conf
WK_CLUSTER_NODES=[{"id":1,"addr":"127.0.0.1:7000"}]
WK_CLUSTER_GROUPS=[{"id":1,"peers":[1]}]
WK_GATEWAY_LISTENERS=[{"name":"tcp-wkproto","network":"tcp","address":"0.0.0.0:5100","transport":"gnet","protocol":"wkproto"}]
```

环境变量覆盖列表时，按整个字段整体替换，不支持局部 merge。

## Common Keys

- `WK_NODE_ID`
- `WK_NODE_NAME`
- `WK_NODE_DATA_DIR`
- `WK_CLUSTER_LISTEN_ADDR`
- `WK_CLUSTER_NODES`
- `WK_CLUSTER_GROUPS`
- `WK_API_LISTEN_ADDR`
- `WK_GATEWAY_LISTENERS`

更多示例以 `wukongim.conf.example` 为准。
