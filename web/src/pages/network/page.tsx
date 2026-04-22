import { MetricPlaceholder } from "@/components/shell/metric-placeholder"
import { PageContainer } from "@/components/shell/page-container"
import { PageHeader } from "@/components/shell/page-header"
import { PlaceholderBlock } from "@/components/shell/placeholder-block"
import { SectionCard } from "@/components/shell/section-card"

export function NetworkPage() {
  return (
    <PageContainer>
      <PageHeader
        description="Transport-level visibility will grow here with throughput, latency, and error views."
        eyebrow="Observability"
        title="Network workspace"
      />
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
