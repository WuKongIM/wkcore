import { Button } from "@/components/ui/button"
import { MetricPlaceholder } from "@/components/shell/metric-placeholder"
import { PageContainer } from "@/components/shell/page-container"
import { PageHeader } from "@/components/shell/page-header"
import { PlaceholderBlock } from "@/components/shell/placeholder-block"
import { SectionCard } from "@/components/shell/section-card"

export function NetworkPage() {
  return (
    <PageContainer>
      <PageHeader
        title="Network"
        description="Transport summary, throughput placeholders, and runtime detail."
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
            Scope: transport
          </div>
          <div className="rounded-md border border-border bg-background px-3 py-2">
            Status: static
          </div>
        </div>
      </PageHeader>
      <section className="grid gap-4 md:grid-cols-3">
        <MetricPlaceholder hint="Ingress placeholder." label="Ingress" />
        <MetricPlaceholder hint="Egress placeholder." label="Egress" />
        <MetricPlaceholder hint="RPC latency placeholder." label="RPC latency" />
      </section>
      <section className="grid gap-4 xl:grid-cols-[1.2fr_0.8fr]">
        <SectionCard
          description="Primary placeholder for transport summary tables and trends."
          title="Transport Summary"
        >
          <PlaceholderBlock kind="panel" />
        </SectionCard>
        <SectionCard
          description="Secondary placeholder for transport detail and anomaly notes."
          title="Transport Detail"
        >
          <PlaceholderBlock kind="detail" />
        </SectionCard>
      </section>
    </PageContainer>
  )
}
