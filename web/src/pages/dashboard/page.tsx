import { BellRing, Radar, Rows3 } from "lucide-react"

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
              Export shell
            </Button>
            <Button size="sm">Pin board</Button>
          </>
        }
        description="Track the control plane shell first, then drill into runtime inventory and observation views from the same console language."
        eyebrow="Overview"
        title="Dashboard workspace"
      >
        <div className="grid gap-3 md:grid-cols-3">
          <div className="rounded-2xl border border-primary/12 bg-white/72 px-4 py-3 text-sm text-muted-foreground shadow-sm">
            <div className="text-[11px] font-semibold uppercase tracking-[0.22em] text-primary/80">
              Posture
            </div>
            <div className="mt-2 font-medium text-foreground">Single-node cluster shell</div>
          </div>
          <div className="rounded-2xl border border-white/60 bg-white/72 px-4 py-3 text-sm text-muted-foreground shadow-sm">
            <div className="text-[11px] font-semibold uppercase tracking-[0.22em] text-primary/80">
              Surface
            </div>
            <div className="mt-2 font-medium text-foreground">Operational skeleton ready</div>
          </div>
          <div className="rounded-2xl border border-white/60 bg-white/72 px-4 py-3 text-sm text-muted-foreground shadow-sm">
            <div className="text-[11px] font-semibold uppercase tracking-[0.22em] text-primary/80">
              Feed
            </div>
            <div className="mt-2 font-medium text-foreground">Static placeholders only</div>
          </div>
        </div>
      </PageHeader>

      <section className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
        <MetricPlaceholder hint="Cluster-wide node state placeholder." label="Nodes" />
        <MetricPlaceholder hint="Channel health placeholder." label="Channels" />
        <MetricPlaceholder hint="Connection capacity placeholder." label="Connections" />
        <MetricPlaceholder hint="Slot assignment placeholder." label="Slots" />
      </section>

      <section className="grid gap-4 xl:grid-cols-[1.2fr_0.8fr]">
        <SectionCard
          action={<Radar className="size-4 text-primary/80" />}
          description="Reserved for global health, topology pressure, and controller-facing runtime posture."
          title="Operations Snapshot"
        >
          <PlaceholderBlock kind="panel" />
        </SectionCard>
        <SectionCard
          action={<BellRing className="size-4 text-primary/80" />}
          description="Reserved for alert lanes, retries, and operator reminders that need fast visibility."
          title="Active alerts lane"
        >
          <PlaceholderBlock kind="list" />
        </SectionCard>
      </section>

      <section className="grid gap-4 xl:grid-cols-[0.95fr_1.05fr]">
        <SectionCard
          action={<Rows3 className="size-4 text-primary/80" />}
          description="Reserved for slot, channel, and replica balance posture blocks."
          title="Replication posture"
        >
          <PlaceholderBlock kind="detail" />
        </SectionCard>
        <SectionCard
          description="Reserved for pending manager tasks, leader actions, and control queue surfaces."
          title="Control queue"
        >
          <PlaceholderBlock kind="table" />
        </SectionCard>
      </section>
    </PageContainer>
  )
}
