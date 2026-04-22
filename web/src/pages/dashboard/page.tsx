import { Button } from "@/components/ui/button"
import { MetricPlaceholder } from "@/components/shell/metric-placeholder"
import { PageContainer } from "@/components/shell/page-container"
import { PageHeader } from "@/components/shell/page-header"
import { PlaceholderBlock } from "@/components/shell/placeholder-block"
import { SectionCard } from "@/components/shell/section-card"

export function DashboardPage() {
  return (
    <PageContainer>
      <PageHeader
        actions={
          <>
            <Button size="sm" variant="outline">
              Export
            </Button>
            <Button size="sm">Pin board</Button>
          </>
        }
        description="Start from the cluster-wide shell, then drill into runtime and observation pages."
        eyebrow="Overview"
        title="Dashboard workspace"
      />
      <section className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
        <MetricPlaceholder hint="Cluster-wide node state placeholder." label="Nodes" />
        <MetricPlaceholder hint="Channel health placeholder." label="Channels" />
        <MetricPlaceholder hint="Connection capacity placeholder." label="Connections" />
        <MetricPlaceholder hint="Slot assignment placeholder." label="Slots" />
      </section>
      <section className="grid gap-4 xl:grid-cols-[1.3fr_1fr]">
        <SectionCard
          description="Reserved for global health, anomalies, and role distribution."
          title="Cluster Summary"
        >
          <PlaceholderBlock kind="panel" />
        </SectionCard>
        <SectionCard
          description="Reserved for alerts, retries, and operator reminders."
          title="Recent Events"
        >
          <PlaceholderBlock kind="list" />
        </SectionCard>
      </section>
    </PageContainer>
  )
}
