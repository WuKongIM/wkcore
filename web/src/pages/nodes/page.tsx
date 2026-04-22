import { Button } from "@/components/ui/button"
import { PageContainer } from "@/components/shell/page-container"
import { PageHeader } from "@/components/shell/page-header"
import { PlaceholderBlock } from "@/components/shell/placeholder-block"
import { SectionCard } from "@/components/shell/section-card"

export function NodesPage() {
  return (
    <PageContainer>
      <PageHeader
        title="Nodes"
        description="Node inventory, roles, and runtime status."
        actions={
          <>
            <Button size="sm" variant="outline">
              Refresh
            </Button>
            <Button size="sm">Inspect</Button>
          </>
        }
      >
        <div className="flex flex-wrap gap-2 text-xs text-muted-foreground">
          <div className="rounded-md border border-border bg-background px-3 py-2">
            Scope: all nodes
          </div>
          <div className="rounded-md border border-border bg-background px-3 py-2">
            Status: static
          </div>
        </div>
      </PageHeader>
      <SectionCard
        description="Primary table placeholder for node inventory and role status."
        title="Node Inventory"
      >
        <div className="mb-4 grid gap-3 md:grid-cols-[1.2fr_0.9fr_0.9fr_auto]">
          <PlaceholderBlock className="min-h-14" kind="panel" />
          <PlaceholderBlock className="min-h-14" kind="panel" />
          <PlaceholderBlock className="min-h-14" kind="panel" />
          <PlaceholderBlock className="min-h-14" kind="detail" />
        </div>
        <PlaceholderBlock kind="table" />
      </SectionCard>
    </PageContainer>
  )
}
