import { render, screen } from "@testing-library/react"
import { RouterProvider, createMemoryRouter } from "react-router-dom"

import { routes } from "@/app/router"

it.each([
  ["/dashboard", "Dashboard", "Operations Snapshot"],
  ["/nodes", "Nodes", "Node Inventory"],
  ["/channels", "Channels", "Channel List"],
  ["/connections", "Connections", "Connection Table"],
  ["/slots", "Slots", "Slot Health"],
  ["/network", "Network", "Traffic Overview"],
  ["/topology", "Topology", "Topology Canvas"],
])("renders %s shell", async (path, title, section) => {
  const router = createMemoryRouter(routes, { initialEntries: [path] })

  render(<RouterProvider router={router} />)

  expect(await screen.findByRole("heading", { name: title })).toBeInTheDocument()
  expect(screen.getByText(section)).toBeInTheDocument()
})

test("dashboard shows the operations showcase blocks", async () => {
  const router = createMemoryRouter(routes, { initialEntries: ["/dashboard"] })

  render(<RouterProvider router={router} />)

  expect(await screen.findByText("Operations Snapshot")).toBeInTheDocument()
  expect(screen.getByText("Active alerts lane")).toBeInTheDocument()
})
