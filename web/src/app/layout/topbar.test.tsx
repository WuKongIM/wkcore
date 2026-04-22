import { render, screen } from "@testing-library/react"
import { RouterProvider, createMemoryRouter } from "react-router-dom"

import { routes } from "@/app/router"

test("shows the current route title and description", async () => {
  const router = createMemoryRouter(routes, { initialEntries: ["/network"] })

  render(<RouterProvider router={router} />)

  expect(await screen.findByRole("heading", { name: "Network" })).toBeInTheDocument()
  expect(
    screen.getByText("Cluster traffic and transport observation shell."),
  ).toBeInTheDocument()
})
