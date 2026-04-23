import { render, screen } from "@testing-library/react"
import { RouterProvider, createMemoryRouter } from "react-router-dom"
import { beforeEach, vi } from "vitest"

import { AppProviders } from "@/app/providers"
import { routes } from "@/app/router"
import { useAuthStore } from "@/auth/auth-store"

const getOverviewMock = vi.fn()
const getTasksMock = vi.fn()
const getNodesMock = vi.fn()
const getChannelRuntimeMetaMock = vi.fn()
const getConnectionsMock = vi.fn()
const getSlotsMock = vi.fn()

vi.mock("@/lib/manager-api", async (importOriginal) => {
  const actual = await importOriginal<typeof import("@/lib/manager-api")>()
  return {
    ...actual,
    getOverview: (...args: unknown[]) => getOverviewMock(...args),
    getTasks: (...args: unknown[]) => getTasksMock(...args),
    getNodes: (...args: unknown[]) => getNodesMock(...args),
    getChannelRuntimeMeta: (...args: unknown[]) => getChannelRuntimeMetaMock(...args),
    getConnections: (...args: unknown[]) => getConnectionsMock(...args),
    getSlots: (...args: unknown[]) => getSlotsMock(...args),
  }
})

beforeEach(() => {
  localStorage.clear()
  getOverviewMock.mockReset()
  getTasksMock.mockReset()
  getNodesMock.mockReset()
  getChannelRuntimeMetaMock.mockReset()
  getConnectionsMock.mockReset()
  getSlotsMock.mockReset()

  getOverviewMock.mockResolvedValue({
    generated_at: "2026-04-23T08:00:00Z",
    cluster: { controller_leader_id: 1 },
    nodes: { total: 1, alive: 1, suspect: 0, dead: 0, draining: 0 },
    slots: {
      total: 1,
      ready: 1,
      quorum_lost: 0,
      leader_missing: 0,
      unreported: 0,
      peer_mismatch: 0,
      epoch_lag: 0,
    },
    tasks: { total: 0, pending: 0, retrying: 0, failed: 0 },
    anomalies: {
      slots: {
        quorum_lost: { count: 0, items: [] },
        leader_missing: { count: 0, items: [] },
        sync_mismatch: { count: 0, items: [] },
      },
      tasks: {
        failed: { count: 0, items: [] },
        retrying: { count: 0, items: [] },
      },
    },
  })
  getTasksMock.mockResolvedValue({ total: 0, items: [] })
  getNodesMock.mockResolvedValue({
    total: 1,
    items: [{
      node_id: 1,
      addr: "127.0.0.1:7000",
      status: "alive",
      last_heartbeat_at: "2026-04-23T08:00:00Z",
      is_local: true,
      capacity_weight: 1,
      controller: { role: "leader" },
      slot_stats: { count: 1, leader_count: 1 },
    }],
  })
  getChannelRuntimeMetaMock.mockResolvedValue({
    items: [{
      channel_id: "alpha",
      channel_type: 1,
      slot_id: 9,
      channel_epoch: 11,
      leader_epoch: 5,
      leader: 2,
      replicas: [1, 2, 3],
      isr: [1, 2],
      min_isr: 2,
      status: "active",
    }],
    has_more: false,
  })
  getConnectionsMock.mockResolvedValue({
    total: 1,
    items: [{
      session_id: 101,
      uid: "u1",
      device_id: "device-a",
      device_flag: "app",
      device_level: "master",
      slot_id: 9,
      state: "active",
      listener: "tcp",
      connected_at: "2026-04-23T08:00:00Z",
      remote_addr: "10.0.0.1:5000",
      local_addr: "127.0.0.1:7000",
    }],
  })
  getSlotsMock.mockResolvedValue({
    total: 1,
    items: [{
      slot_id: 9,
      state: { quorum: "ready", sync: "in_sync" },
      assignment: { desired_peers: [1, 2, 3], config_epoch: 7, balance_version: 4 },
      runtime: {
        current_peers: [1, 2, 3],
        leader_id: 2,
        healthy_voters: 3,
        has_quorum: true,
        observed_config_epoch: 7,
        last_report_at: "2026-04-23T08:00:00Z",
      },
    }],
  })

  useAuthStore.setState({
    status: "authenticated",
    isHydrated: true,
    username: "admin",
    tokenType: "Bearer",
    accessToken: "token-1",
    expiresAt: "2099-04-22T12:00:00Z",
    permissions: [],
  })
})

it.each([
  ["/dashboard", "Dashboard", "Operations Summary"],
  ["/nodes", "Nodes", "Node Inventory"],
  ["/channels", "Channels", "Channel Runtime"],
  ["/connections", "Connections", "Connection Inventory"],
  ["/slots", "Slots", "Slot Inventory"],
])("renders %s shell", async (path, title, section) => {
  const router = createMemoryRouter(routes, { initialEntries: [path] })

  render(
    <AppProviders>
      <RouterProvider router={router} />
    </AppProviders>,
  )

  expect(await screen.findByRole("heading", { name: title })).toBeInTheDocument()
  expect(screen.getByText(section)).toBeInTheDocument()
  expect(screen.queryByText(/workspace/i)).not.toBeInTheDocument()
})

it.each([
  ["/network", "Network", /does not expose transport or throughput endpoints/i],
  ["/topology", "Topology", /does not expose replica topology endpoints/i],
])("renders %s unavailable manager scope", async (path, title, message) => {
  const router = createMemoryRouter(routes, { initialEntries: [path] })

  render(
    <AppProviders>
      <RouterProvider router={router} />
    </AppProviders>,
  )

  expect(await screen.findByRole("heading", { name: title })).toBeInTheDocument()
  expect(screen.getByText("Manager API Coverage")).toBeInTheDocument()
  expect(screen.getByText(message)).toBeInTheDocument()
})

test("dashboard shows monochrome workbench sections", async () => {
  const router = createMemoryRouter(routes, { initialEntries: ["/dashboard"] })

  render(
    <AppProviders>
      <RouterProvider router={router} />
    </AppProviders>,
  )

  expect(await screen.findByRole("heading", { name: "Dashboard" })).toBeInTheDocument()
  expect(screen.getByText("Operations Summary")).toBeInTheDocument()
  expect(screen.getAllByText("Alert List").length).toBeGreaterThan(0)
  expect(screen.getAllByText("Control Queue").length).toBeGreaterThan(0)
  expect(screen.queryByText("Pin board")).not.toBeInTheDocument()
})
