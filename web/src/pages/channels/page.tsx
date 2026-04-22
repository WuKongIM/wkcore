import { Button } from "@/components/ui/button"
import { PageContainer } from "@/components/shell/page-container"
import { PageHeader } from "@/components/shell/page-header"
import { PlaceholderBlock } from "@/components/shell/placeholder-block"
import { SectionCard } from "@/components/shell/section-card"

export function ChannelsPage() {
  return (
    <PageContainer>
      <PageHeader
        actions={
          <>
            <Button size="sm" variant="outline">
              Filter set
            </Button>
            <Button size="sm">Inspect</Button>
          </>
        }
        description="The channel view will host list filters, health tags, and drill-in actions."
        eyebrow="Runtime"
        title="Channels workspace"
      />
      <SectionCard
        description="Reserved for channel-level list controls and summary strips."
        title="Channel List"
      >
        <div className="mb-4 grid gap-3 md:grid-cols-[1.3fr_1fr_1fr_auto]">
          <PlaceholderBlock className="min-h-14" kind="panel" />
          <PlaceholderBlock className="min-h-14" kind="panel" />
          <PlaceholderBlock className="min-h-14" kind="panel" />
          <PlaceholderBlock className="min-h-14" kind="panel" />
        </div>
        <PlaceholderBlock kind="table" />
      </SectionCard>
    </PageContainer>
  )
}
