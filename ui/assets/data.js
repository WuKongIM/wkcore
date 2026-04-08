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
};
