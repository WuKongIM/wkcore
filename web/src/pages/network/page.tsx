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
        actions={
          <>
            <Button size="sm" variant="outline">
              Scope lane
            </Button>
            <Button size="sm">Inspect transport</Button>
          </>
        }
        description="Transport-level visibility will grow here with throughput, latency, and error views."
        eyebrow="Observability"
        title="Network workspace"
      >
        <div className="flex flex-wrap gap-3 text-xs text-muted-foreground">
          <div className="rounded-full border border-white/70 bg-white/75 px-3 py-1.5 shadow-sm">
            Throughput shell
          </div>
          <div className="rounded-full border border-white/70 bg-white/75 px-3 py-1.5 shadow-sm">
            RPC pressure lane
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
          description="Reserved for throughput, saturation, and transport trend blocks."
          title="Traffic Overview"
        >
          <PlaceholderBlock kind="panel" />
        </SectionCard>
        <SectionCard
          description="Reserved for anomaly details and retry hints."
          title="Transport Detail"
        >
          <PlaceholderBlock kind="detail" />
        </SectionCard>
      </section>
    </PageContainer>
  )
}
