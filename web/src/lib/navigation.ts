export type NavigationItem = {
  href: string
  title: string
}

export type NavigationGroup = {
  label: string
  items: NavigationItem[]
}

export const navigationGroups: NavigationGroup[] = [
  {
    label: "Overview",
    items: [{ href: "/dashboard", title: "Dashboard" }],
  },
  {
    label: "Runtime",
    items: [
      { href: "/nodes", title: "Nodes" },
      { href: "/channels", title: "Channels" },
      { href: "/connections", title: "Connections" },
      { href: "/slots", title: "Slots" },
    ],
  },
  {
    label: "Observability",
    items: [
      { href: "/network", title: "Network" },
      { href: "/topology", title: "Topology" },
    ],
  },
]

export const pageTitles = new Map(
  navigationGroups.flatMap((group) =>
    group.items.map((item) => [item.href, item.title] as const),
  ),
)
