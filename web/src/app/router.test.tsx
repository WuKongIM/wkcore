import { render, screen } from "@testing-library/react"
import { RouterProvider, createMemoryRouter } from "react-router-dom"

import { routes } from "@/app/router"

test("redirects / to /dashboard", async () => {
  const router = createMemoryRouter(routes, { initialEntries: ["/"] })

  render(<RouterProvider router={router} />)

  expect(
    await screen.findByRole("heading", { name: "Dashboard" }),
  ).toBeInTheDocument()
})

test("renders sidebar, topbar, and page outlet inside the app shell", async () => {
  const router = createMemoryRouter(routes, { initialEntries: ["/nodes"] })

  render(<RouterProvider router={router} />)

  expect(await screen.findByLabelText("Primary navigation")).toBeInTheDocument()
  expect(screen.getByRole("banner")).toBeInTheDocument()
  expect(screen.getByRole("main")).toBeInTheDocument()
})
