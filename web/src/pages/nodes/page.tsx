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
              Filter lanes
            </Button>
            <Button size="sm">Inspect nodes</Button>
          </>
        }
        description="Node operations land here first: inventory, role, and lifecycle shells."
        eyebrow="Runtime"
        title="Nodes workspace"
      >
        <div className="flex flex-wrap gap-3 text-xs text-muted-foreground">
          <div className="rounded-full border border-white/70 bg-white/75 px-3 py-1.5 shadow-sm">
            Role-aware list shell
          </div>
          <div className="rounded-full border border-white/70 bg-white/75 px-3 py-1.5 shadow-sm">
            Operator actions reserved
          </div>
        </div>
      </PageHeader>
      <SectionCard
        description="Toolbar, filters, and node list rows will replace this placeholder in later iterations."
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
