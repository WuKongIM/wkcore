import { render, screen, within } from "@testing-library/react"

import { MetricPlaceholder } from "@/components/shell/metric-placeholder"
import { PageHeader } from "@/components/shell/page-header"
import { PlaceholderBlock } from "@/components/shell/placeholder-block"

test("page header renders a flat tool row", () => {
  render(
    <PageHeader
      title="Dashboard"
      description="Runtime summary."
      actions={<button type="button">Refresh</button>}
    >
      <div>Scope: single-node cluster</div>
    </PageHeader>,
  )

  expect(screen.getByRole("heading", { name: "Dashboard" })).toBeInTheDocument()
  expect(screen.getByText("Scope: single-node cluster")).toBeInTheDocument()
  expect(screen.getByRole("button", { name: "Refresh" })).toBeInTheDocument()
})

test("metric placeholder is a compact data cell", () => {
  render(<MetricPlaceholder label="Nodes" hint="Registered node count." />)

  expect(screen.getByText("Nodes")).toBeInTheDocument()
  expect(screen.getByText("--")).toBeInTheDocument()
  expect(screen.getByText("Registered node count.")).toBeInTheDocument()
  expect(screen.queryByText("Ready")).not.toBeInTheDocument()
})

test("table placeholder exposes structural table rows", () => {
  render(<PlaceholderBlock kind="table" />)

  const table = screen.getByTestId("placeholder-table")

  expect(within(table).getAllByTestId("placeholder-table-row")).toHaveLength(3)
})
