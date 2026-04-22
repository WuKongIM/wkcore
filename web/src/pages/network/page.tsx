import { ResourceState } from "@/components/manager/resource-state"
import { PageContainer } from "@/components/shell/page-container"
import { PageHeader } from "@/components/shell/page-header"
import { SectionCard } from "@/components/shell/section-card"

export function NetworkPage() {
  return (
    <PageContainer>
      <PageHeader
        title="Network"
        description="Transport coverage notes for manager surfaces that are not yet exposed."
      >
        <div className="flex flex-wrap gap-2 text-xs text-muted-foreground">
          <div className="rounded-md border border-border bg-background px-3 py-2">
            Scope: transport
          </div>
          <div className="rounded-md border border-border bg-background px-3 py-2">
            Status: not exposed
          </div>
        </div>
      </PageHeader>
      <SectionCard
        description="This view remains informational until transport-oriented manager APIs are available."
        title="Manager API Coverage"
      >
        <ResourceState
          description="The current manager API does not expose transport or throughput endpoints yet."
          kind="empty"
          title="Network"
        />
      </SectionCard>
    </PageContainer>
  )
}
