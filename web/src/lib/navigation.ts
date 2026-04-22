import type { LucideIcon } from "lucide-react"
import {
  Cable,
  Database,
  LayoutDashboard,
  MessageSquare,
  Radar,
  Server,
  Waypoints,
} from "lucide-react"

export type NavigationItem = {
  href: string
  title: string
  description: string
  icon: LucideIcon
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
        description: "Runtime summary and operator entry points.",
        icon: LayoutDashboard,
      },
    ],
  },
  {
    label: "Runtime",
    items: [
      {
        href: "/nodes",
        title: "Nodes",
        description: "Node inventory, roles, and lifecycle status.",
        icon: Server,
      },
      {
        href: "/channels",
        title: "Channels",
        description: "Channel lists and runtime drill-in status.",
        icon: MessageSquare,
      },
      {
        href: "/connections",
        title: "Connections",
        description: "Connection inventory and transport state.",
        icon: Cable,
      },
      {
        href: "/slots",
        title: "Slots",
        description: "Slot distribution and runtime status.",
        icon: Database,
      },
    ],
  },
  {
    label: "Observability",
    items: [
      {
        href: "/network",
        title: "Network",
        description: "Transport summary and runtime status.",
        icon: Radar,
      },
      {
        href: "/topology",
        title: "Topology",
        description: "Replica relationships and topology context.",
        icon: Waypoints,
      },
    ],
  },
]

export const navigationItems = navigationGroups.flatMap((group) => group.items)

export const pageMetadata = new Map(
  navigationItems.map((item) => [item.href, item] as const),
)
