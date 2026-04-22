import { Button } from "@/components/ui/button"
import { PageContainer } from "@/components/shell/page-container"
import { PageHeader } from "@/components/shell/page-header"
import { PlaceholderBlock } from "@/components/shell/placeholder-block"
import { SectionCard } from "@/components/shell/section-card"

export function TopologyPage() {
  return (
    <PageContainer>
      <PageHeader
        title="Topology"
        description="Replica relationships, node adjacency, and context detail."
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
            Scope: cluster graph
          </div>
          <div className="rounded-md border border-border bg-background px-3 py-2">
            Status: static
          </div>
        </div>
      </PageHeader>
      <section className="grid gap-4 xl:grid-cols-[1.4fr_0.85fr]">
        <SectionCard
          description="Primary placeholder for topology structure and relationship context."
          title="Topology View"
        >
          <PlaceholderBlock kind="canvas" />
        </SectionCard>
        <SectionCard
          description="Secondary placeholder for selected-node context and notes."
          title="Context Detail"
        >
          <PlaceholderBlock kind="detail" />
        </SectionCard>
      </section>
    </PageContainer>
  )
}
