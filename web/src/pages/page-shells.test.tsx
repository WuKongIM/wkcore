import { render, screen } from "@testing-library/react"
import { RouterProvider, createMemoryRouter } from "react-router-dom"
import { beforeEach } from "vitest"

import { AppProviders } from "@/app/providers"
import { routes } from "@/app/router"
import { useAuthStore } from "@/auth/auth-store"

beforeEach(() => {
  localStorage.clear()
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
  ["/channels", "Channels", "Channel List"],
  ["/connections", "Connections", "Connection Table"],
  ["/slots", "Slots", "Slot Status"],
  ["/network", "Network", "Transport Summary"],
  ["/topology", "Topology", "Topology View"],
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

test("dashboard shows monochrome workbench sections", async () => {
  const router = createMemoryRouter(routes, { initialEntries: ["/dashboard"] })

  render(
    <AppProviders>
      <RouterProvider router={router} />
    </AppProviders>,
  )

  expect(await screen.findByRole("heading", { name: "Dashboard" })).toBeInTheDocument()
  expect(screen.getByText("Operations Summary")).toBeInTheDocument()
  expect(screen.getByText("Alert List")).toBeInTheDocument()
  expect(screen.getByText("Control Queue")).toBeInTheDocument()
  expect(screen.queryByText("Pin board")).not.toBeInTheDocument()
})
