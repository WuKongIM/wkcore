import { Navigate, createBrowserRouter, type RouteObject } from "react-router-dom"

import { AppShell } from "@/app/layout/app-shell"
import { ChannelsPage } from "@/pages/channels/page"
import { ConnectionsPage } from "@/pages/connections/page"
import { DashboardPage } from "@/pages/dashboard/page"
import { NetworkPage } from "@/pages/network/page"
import { NodesPage } from "@/pages/nodes/page"
import { SlotsPage } from "@/pages/slots/page"
import { TopologyPage } from "@/pages/topology/page"

export const routes: RouteObject[] = [
  {
    path: "/",
    element: <AppShell />,
    children: [
      { index: true, element: <Navigate replace to="/dashboard" /> },
      { path: "dashboard", element: <DashboardPage /> },
      { path: "nodes", element: <NodesPage /> },
      { path: "channels", element: <ChannelsPage /> },
      { path: "connections", element: <ConnectionsPage /> },
      { path: "slots", element: <SlotsPage /> },
      { path: "network", element: <NetworkPage /> },
      { path: "topology", element: <TopologyPage /> },
    ],
  },
]

export const router = createBrowserRouter(routes)
