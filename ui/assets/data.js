window.ADMIN_UI_DATA = {
  cluster: {
    name: "cluster-alpha",
    deployment: "单节点集群",
    window: "最近 15 分钟",
    refresh: "自动刷新 10s",
  },
  nav: [
    {
      label: "概览",
      items: [
        {
          key: "dashboard",
          text: "Dashboard",
          path: "dashboard.html",
          icon: "layout-dashboard",
        },
      ],
    },
    {
      label: "集群",
      items: [
        {
          key: "nodes",
          text: "节点管理",
          path: "nodes.html",
          icon: "server",
        },
        {
          key: "groups",
          text: "分区管理",
          path: "placeholder/groups.html",
          icon: "blocks",
        },
        {
          key: "network",
          text: "网络监控",
          path: "placeholder/network.html",
          icon: "waypoints",
        },
        {
          key: "topology",
          text: "拓扑视图",
          path: "placeholder/topology.html",
          icon: "share-2",
        },
      ],
    },
    {
      label: "连接",
      items: [
        {
          key: "connections",
          text: "在线连接",
          path: "placeholder/connections.html",
          icon: "plug-zap",
        },
      ],
    },
  ],
  pages: {
    dashboard: {
      title: "Dashboard",
      description: "主体骨架已就位，关键指标和风险摘要将在本阶段后续步骤填充。",
    },
    nodes: {
      title: "节点管理",
      description: "节点表格和详情抽屉将在本阶段后续步骤填充。",
    },
    groups: {
      title: "分区管理",
      description: "该页面将在下一阶段扩展",
      tags: ["Group 分布", "Leader / Follower", "Rebalance 入口"],
    },
    network: {
      title: "网络监控",
      description: "该页面将在下一阶段扩展",
      tags: ["RPC Latency", "丢包与重试", "跨节点链路"],
    },
    topology: {
      title: "拓扑视图",
      description: "该页面将在下一阶段扩展",
      tags: ["Node Graph", "Group 映射", "异常定位"],
    },
    connections: {
      title: "在线连接",
      description: "该页面将在下一阶段扩展",
      tags: ["连接状态", "设备分布", "会话诊断"],
    },
  },
  dashboard: {
    windows: ["15m", "1h", "6h"],
    metrics: [
      { label: "在线节点数", value: "3 / 3", hint: "Healthy 2 · Degraded 1", tone: "neutral" },
      { label: "异常节点数", value: "1", hint: "wk-node3 latency elevated", tone: "warning" },
      { label: "总 Group 数", value: "64", hint: "22 / 21 / 21 分布", tone: "neutral" },
      { label: "当前连接数", value: "34,195", hint: "较 15m 前 +4.8%", tone: "accent" },
    ],
    risks: [
      {
        level: "critical",
        title: "wk-node3 RPC Latency 持续抬升",
        detail: "18.4ms，明显高于集群均值 6.3ms",
        path: "nodes.html?status=degraded",
      },
      {
        level: "warning",
        title: "Follower replication lag 超过阈值",
        detail: "node-3 落后 2 个心跳窗口",
        path: "nodes.html?status=degraded",
      },
      {
        level: "info",
        title: "wk-node1 连接数高于集群均值",
        detail: "写流量集中，建议继续观察负载均衡",
        path: "nodes.html",
      },
    ],
    snapshot: [
      "accent", "soft", "soft", "healthy", "soft", "accent",
      "soft", "healthy", "soft", "warning", "soft", "accent",
      "accent", "soft", "soft", "soft", "healthy", "soft",
      "soft", "warning", "soft", "healthy", "soft", "accent",
    ],
    trend: [
      { label: "10:00", value: 38 },
      { label: "10:05", value: 42 },
      { label: "10:10", value: 40 },
      { label: "10:15", value: 48 },
      { label: "10:20", value: 54 },
      { label: "10:25", value: 52 },
      { label: "10:30", value: 58 },
    ],
    hotNodes: [
      { name: "wk-node3", status: "degraded", meta: "RPC 18.4ms · Group 21" },
      { name: "wk-node1", status: "online", meta: "连接 12,480 · Leader" },
      { name: "wk-node2", status: "online", meta: "RPC 6.2ms · Follower" },
    ],
  },
};
