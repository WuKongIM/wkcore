import { render, screen, within } from "@testing-library/react"
import { RouterProvider, createMemoryRouter } from "react-router-dom"

import { routes } from "@/app/router"

test("shows the current route title and description", async () => {
  const router = createMemoryRouter(routes, { initialEntries: ["/network"] })

  render(<RouterProvider router={router} />)

  expect(within(screen.getByRole("banner")).getByText("Network")).toBeInTheDocument()
  expect(
    within(screen.getByRole("banner")).getByText("Transport summary and runtime status."),
  ).toBeInTheDocument()
})

test("removes legacy topbar pills while keeping global actions", async () => {
  const router = createMemoryRouter(routes, { initialEntries: ["/network"] })

  render(<RouterProvider router={router} />)

  expect(await screen.findByRole("button", { name: /refresh/i })).toBeInTheDocument()
  expect(screen.getByRole("button", { name: /search/i })).toBeInTheDocument()
  expect(screen.queryByText("Control plane")).not.toBeInTheDocument()
  expect(screen.queryByText("Manager shell")).not.toBeInTheDocument()
})
