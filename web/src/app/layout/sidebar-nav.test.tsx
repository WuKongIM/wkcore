import { render, screen } from "@testing-library/react"
import { RouterProvider, createMemoryRouter } from "react-router-dom"

import { routes } from "@/app/router"

test("marks the current navigation item with aria-current", async () => {
  const router = createMemoryRouter(routes, { initialEntries: ["/slots"] })

  render(<RouterProvider router={router} />)

  expect(await screen.findByRole("link", { name: "Slots" })).toHaveAttribute(
    "aria-current",
    "page",
  )
})
