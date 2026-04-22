export type NavigationItem = {
  href: string
  title: string
  description: string
}

export type NavigationGroup = {
  label: string
  items: NavigationItem[]
}

export const navigationGroups: NavigationGroup[] = [
  {
    label: "Overview",
    items: [
      {
        href: "/dashboard",
        title: "Dashboard",
        description: "Cluster summary and entry point.",
      },
    ],
  },
  {
    label: "Runtime",
    items: [
      {
        href: "/nodes",
        title: "Nodes",
        description: "Node inventory and lifecycle shell.",
      },
      {
        href: "/channels",
        title: "Channels",
        description: "Channel list and drill-in shell.",
      },
      {
        href: "/connections",
        title: "Connections",
        description: "Connection inventory shell.",
      },
      {
        href: "/slots",
        title: "Slots",
        description: "Slot distribution and status shell.",
      },
    ],
  },
  {
    label: "Observability",
    items: [
      {
        href: "/network",
        title: "Network",
        description: "Cluster traffic and transport observation shell.",
      },
      {
        href: "/topology",
        title: "Topology",
        description: "Replica and node relationship shell.",
      },
    ],
  },
]

export const navigationItems = navigationGroups.flatMap((group) => group.items)

export const pageMetadata = new Map(
  navigationItems.map((item) => [item.href, item] as const),
)
