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
              Layout
            </Button>
            <Button size="sm">Focus path</Button>
          </>
        }
        description="Replica relationships, node adjacency, and future canvas controls will live here."
        eyebrow="Observability"
        title="Topology workspace"
      />
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
