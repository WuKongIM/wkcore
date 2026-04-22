import { Button } from "@/components/ui/button"
import { MetricPlaceholder } from "@/components/shell/metric-placeholder"
import { PageContainer } from "@/components/shell/page-container"
import { PageHeader } from "@/components/shell/page-header"
import { PlaceholderBlock } from "@/components/shell/placeholder-block"
import { SectionCard } from "@/components/shell/section-card"

export function SlotsPage() {
  return (
    <PageContainer>
      <PageHeader
        title="Slots"
        description="Slot distribution, leader placement, and movement status."
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
            Scope: all slots
          </div>
          <div className="rounded-md border border-border bg-background px-3 py-2">
            Status: static
          </div>
        </div>
      </PageHeader>
      <section className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
        <MetricPlaceholder hint="Leader coverage placeholder." label="Leaders" />
        <MetricPlaceholder hint="Follower coverage placeholder." label="Followers" />
        <MetricPlaceholder hint="Replica balance placeholder." label="Replicas" />
        <MetricPlaceholder hint="Pending move placeholder." label="Moves" />
      </section>
      <SectionCard
        description="Primary table placeholder for slot assignment and balance state."
        title="Slot Status"
      >
        <PlaceholderBlock kind="table" />
      </SectionCard>
    </PageContainer>
  )
}
