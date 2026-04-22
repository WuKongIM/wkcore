import { Button } from "@/components/ui/button"
import { PageContainer } from "@/components/shell/page-container"
import { PageHeader } from "@/components/shell/page-header"
import { PlaceholderBlock } from "@/components/shell/placeholder-block"
import { SectionCard } from "@/components/shell/section-card"

export function TopologyPage() {
  return (
    <PageContainer>
      <PageHeader
        actions={
          <>
            <Button size="sm" variant="outline">
              Layout lane
            </Button>
            <Button size="sm">Focus path</Button>
          </>
        }
        description="Replica relationships, node adjacency, and future canvas controls will live here."
        eyebrow="Observability"
        title="Topology workspace"
      >
        <div className="flex flex-wrap gap-3 text-xs text-muted-foreground">
          <div className="rounded-full border border-white/70 bg-white/75 px-3 py-1.5 shadow-sm">
            Canvas shell
          </div>
          <div className="rounded-full border border-white/70 bg-white/75 px-3 py-1.5 shadow-sm">
            Selection detail lane
          </div>
        </div>
      </PageHeader>
      <section className="grid gap-4 xl:grid-cols-[1.4fr_0.85fr]">
        <SectionCard
          description="Reserved for the future graph canvas and relationship overlays."
          title="Topology Canvas"
        >
          <PlaceholderBlock kind="canvas" />
        </SectionCard>
        <SectionCard
          description="Reserved for selection detail and legend content."
          title="Context Detail"
        >
          <PlaceholderBlock kind="detail" />
        </SectionCard>
      </section>
    </PageContainer>
  )
}
