import { ResourceState } from "@/components/manager/resource-state"
import { PageContainer } from "@/components/shell/page-container"
import { PageHeader } from "@/components/shell/page-header"
import { SectionCard } from "@/components/shell/section-card"

export function ConnectionsPage() {
  return (
    <PageContainer>
      <PageHeader
        title="Connections"
        description="Connection coverage notes for manager surfaces that are not yet exposed."
      >
        <div className="flex flex-wrap gap-2 text-xs text-muted-foreground">
          <div className="rounded-md border border-border bg-background px-3 py-2">
            Scope: all clients
          </div>
          <div className="rounded-md border border-border bg-background px-3 py-2">
            Status: not exposed
          </div>
        </div>
      </PageHeader>
      <SectionCard
        description="This area stays intentionally read-only until a matching manager endpoint exists."
        title="Manager API Coverage"
      >
        <ResourceState
          description="The current manager API does not expose connection inventory endpoints yet."
          kind="empty"
          title="Connections"
        />
      </SectionCard>
    </PageContainer>
  )
}
