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
        title="Dashboard"
        description="Runtime summary, queues, and operator-facing overview."
        actions={
          <>
            <Button size="sm" variant="outline">
              Refresh
            </Button>
            <Button size="sm">Export</Button>
          </>
        }
      >
        <div className="flex flex-wrap gap-2 text-xs text-muted-foreground">
          <div className="rounded-md border border-border bg-background px-3 py-2">
            Scope: single-node cluster
          </div>
          <div className="rounded-md border border-border bg-background px-3 py-2">
            Status: static
          </div>
          <div className="rounded-md border border-border bg-background px-3 py-2">
            Feed: placeholders only
          </div>
        </div>
      </PageHeader>

      <section className="grid gap-3 md:grid-cols-2 xl:grid-cols-4">
        <MetricPlaceholder hint="Registered node count." label="Nodes" />
        <MetricPlaceholder hint="Channel summary placeholder." label="Channels" />
        <MetricPlaceholder hint="Connection summary placeholder." label="Connections" />
        <MetricPlaceholder hint="Slot summary placeholder." label="Slots" />
      </section>

      <section className="grid gap-4 xl:grid-cols-[1.15fr_0.85fr]">
        <SectionCard
          description="Primary runtime table placeholder."
          title="Operations Summary"
        >
          <PlaceholderBlock kind="table" />
        </SectionCard>
        <SectionCard
          description="Compact operator-facing status items."
          title="Alert List"
        >
          <PlaceholderBlock kind="list" />
        </SectionCard>
      </section>

      <section className="grid gap-4 xl:grid-cols-[0.95fr_1.05fr]">
        <SectionCard
          description="Dense status-row placeholder."
          title="Replication Status"
        >
          <PlaceholderBlock kind="detail" />
        </SectionCard>
        <SectionCard
          description="Secondary work queue table placeholder."
          title="Control Queue"
        >
          <PlaceholderBlock kind="table" />
        </SectionCard>
      </section>
    </PageContainer>
  )
}
