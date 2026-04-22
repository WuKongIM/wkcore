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
        actions={
          <>
            <Button size="sm" variant="outline">
              Filter set
            </Button>
            <Button size="sm">Leader actions</Button>
          </>
        }
        description="Slot ownership, leader placement, and movement controls will land here later."
        eyebrow="Runtime"
        title="Slots workspace"
      />
      <section className="grid gap-4 md:grid-cols-3 xl:grid-cols-4">
        <MetricPlaceholder hint="Leader coverage placeholder." label="Leaders" />
        <MetricPlaceholder hint="Follower coverage placeholder." label="Followers" />
        <MetricPlaceholder hint="Replica balance placeholder." label="Replicas" />
        <MetricPlaceholder hint="Pending move placeholder." label="Moves" />
      </section>
      <SectionCard
        description="Reserved for slot health summaries and the main slot list."
        title="Slot Health"
      >
        <PlaceholderBlock kind="table" />
      </SectionCard>
    </PageContainer>
  )
}
