import { Button } from "@/components/ui/button"
import { PageContainer } from "@/components/shell/page-container"
import { PageHeader } from "@/components/shell/page-header"
import { PlaceholderBlock } from "@/components/shell/placeholder-block"
import { SectionCard } from "@/components/shell/section-card"

export function NodesPage() {
  return (
    <PageContainer>
      <PageHeader
        actions={
          <>
            <Button size="sm" variant="outline">
              Filters
            </Button>
            <Button size="sm">Inspect</Button>
          </>
        }
        description="Node operations land here first: inventory, role, and lifecycle shells."
        eyebrow="Runtime"
        title="Nodes workspace"
      />
      <SectionCard
        description="Toolbar, filters, and node list rows will replace this placeholder in later iterations."
        title="Node Inventory"
      >
        <div className="mb-4 grid gap-3 md:grid-cols-[1.2fr_0.9fr_0.9fr_auto]">
          <PlaceholderBlock className="min-h-14" kind="panel" />
          <PlaceholderBlock className="min-h-14" kind="panel" />
          <PlaceholderBlock className="min-h-14" kind="panel" />
          <PlaceholderBlock className="min-h-14" kind="panel" />
        </div>
        <PlaceholderBlock kind="table" />
      </SectionCard>
    </PageContainer>
  )
}
