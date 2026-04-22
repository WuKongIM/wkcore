import { Button } from "@/components/ui/button"
import { PageContainer } from "@/components/shell/page-container"
import { PageHeader } from "@/components/shell/page-header"
import { PlaceholderBlock } from "@/components/shell/placeholder-block"
import { SectionCard } from "@/components/shell/section-card"

export function ConnectionsPage() {
  return (
    <PageContainer>
      <PageHeader
        actions={
          <>
            <Button size="sm" variant="outline">
              Filter lanes
            </Button>
            <Button size="sm">Refresh shell</Button>
          </>
        }
        description="Connection-level occupancy, transport, and client state will appear here."
        eyebrow="Runtime"
        title="Connections workspace"
      >
        <div className="flex flex-wrap gap-3 text-xs text-muted-foreground">
          <div className="rounded-full border border-white/70 bg-white/75 px-3 py-1.5 shadow-sm">
            Session list shell
          </div>
          <div className="rounded-full border border-white/70 bg-white/75 px-3 py-1.5 shadow-sm">
            Transport drill-in reserved
          </div>
        </div>
      </PageHeader>
      <SectionCard
        description="Reserved for query controls and connection row placeholders."
        title="Connection Table"
      >
        <div className="mb-4 grid gap-3 md:grid-cols-[1.4fr_1fr_1fr_auto]">
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
