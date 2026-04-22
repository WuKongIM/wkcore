import { useCallback, useEffect, useMemo, useState } from "react"

import { ResourceState } from "@/components/manager/resource-state"
import { StatusBadge } from "@/components/manager/status-badge"
import { Button } from "@/components/ui/button"
import { PageContainer } from "@/components/shell/page-container"
import { PageHeader } from "@/components/shell/page-header"
import { SectionCard } from "@/components/shell/section-card"
import { ManagerApiError, getOverview, getTasks } from "@/lib/manager-api"
import type {
  ManagerOverviewResponse,
  ManagerTask,
  ManagerTasksResponse,
} from "@/lib/manager-api.types"

type DashboardState = {
  overview: ManagerOverviewResponse | null
  tasks: ManagerTasksResponse | null
  loading: boolean
  refreshing: boolean
  error: Error | null
}

function formatTimestamp(value: string) {
  return new Date(value).toLocaleString()
}

function formatErrorKind(error: Error | null) {
  if (!(error instanceof ManagerApiError)) {
    return "error" as const
  }
  if (error.status === 403) {
    return "forbidden" as const
  }
  if (error.status === 503) {
    return "unavailable" as const
  }
  return "error" as const
}

function buildAlertItems(overview: ManagerOverviewResponse) {
  return [
    ...overview.anomalies.slots.quorum_lost.items.map((item) => ({
      key: `slot-quorum-${item.slot_id}`,
      title: `Slot ${item.slot_id}`,
      detail: `${item.quorum} / ${item.sync}`,
      tone: item.quorum,
    })),
    ...overview.anomalies.tasks.retrying.items.map((item) => ({
      key: `task-retrying-${item.slot_id}`,
      title: `Slot ${item.slot_id}`,
      detail: `${item.kind} ${item.status}`,
      tone: item.status,
    })),
  ]
}

function renderTaskRow(task: ManagerTask) {
  return (
    <tr className="border-t border-border" key={`${task.slot_id}-${task.kind}-${task.step}`}>
      <td className="px-3 py-3 text-sm font-medium text-foreground">Slot {task.slot_id}</td>
      <td className="px-3 py-3 text-sm text-foreground">{task.kind}</td>
      <td className="px-3 py-3 text-sm text-foreground">
        <StatusBadge value={task.status} />
      </td>
      <td className="px-3 py-3 text-sm text-muted-foreground">{task.attempt}</td>
      <td className="px-3 py-3 text-sm text-muted-foreground">{task.last_error || "-"}</td>
    </tr>
  )
}

export function DashboardPage() {
  const [state, setState] = useState<DashboardState>({
    overview: null,
    tasks: null,
    loading: true,
    refreshing: false,
    error: null,
  })

  const loadDashboard = useCallback(async (refreshing: boolean) => {
    setState((current) => ({
      ...current,
      loading: refreshing ? current.loading : true,
      refreshing,
      error: null,
    }))

    try {
      const [overview, tasks] = await Promise.all([getOverview(), getTasks()])
      setState({ overview, tasks, loading: false, refreshing: false, error: null })
    } catch (error) {
      setState({
        overview: null,
        tasks: null,
        loading: false,
        refreshing: false,
        error: error instanceof Error ? error : new Error("dashboard request failed"),
      })
    }
  }, [])

  useEffect(() => {
    void loadDashboard(false)
  }, [loadDashboard])

  const alertItems = useMemo(
    () => (state.overview ? buildAlertItems(state.overview) : []),
    [state.overview],
  )

  return (
    <PageContainer>
      <PageHeader
        title="Dashboard"
        description="Runtime summary, queues, and operator-facing overview."
        actions={
          <>
            <Button
              onClick={() => {
                void loadDashboard(true)
              }}
              size="sm"
              variant="outline"
            >
              {state.refreshing ? "Refreshing..." : "Refresh"}
            </Button>
            <Button disabled size="sm">
              Export
            </Button>
          </>
        }
      >
        <div className="flex flex-wrap gap-2 text-xs text-muted-foreground">
          <div className="rounded-md border border-border bg-background px-3 py-2">
            Scope: single-node cluster
          </div>
          <div className="rounded-md border border-border bg-background px-3 py-2">
            {state.overview ? `Generated: ${formatTimestamp(state.overview.generated_at)}` : "Generated: pending"}
          </div>
          <div className="rounded-md border border-border bg-background px-3 py-2">
            {state.overview
              ? `Controller leader: ${state.overview.cluster.controller_leader_id}`
              : "Controller leader: pending"}
          </div>
        </div>
      </PageHeader>

      {state.loading ? <ResourceState kind="loading" title="Dashboard" /> : null}
      {!state.loading && state.error ? (
        <ResourceState
          kind={formatErrorKind(state.error)}
          onRetry={() => {
            void loadDashboard(false)
          }}
          title="Dashboard"
        />
      ) : null}
      {!state.loading && !state.error && state.overview && state.tasks ? (
        <>
          <section className="grid gap-3 md:grid-cols-2 xl:grid-cols-4">
            <SectionCard description="Current controller leader node." title="Controller leader">
              <div className="text-3xl font-semibold text-foreground">
                {state.overview.cluster.controller_leader_id}
              </div>
            </SectionCard>
            <SectionCard description="Total tracked cluster nodes." title="Nodes">
              <div className="text-3xl font-semibold text-foreground">{state.overview.nodes.total}</div>
            </SectionCard>
            <SectionCard description="Slots currently reporting quorum." title="Ready slots">
              <div className="text-3xl font-semibold text-foreground">{state.overview.slots.ready}</div>
            </SectionCard>
            <SectionCard description="Tracked reconcile tasks." title="Tasks">
              <div className="text-3xl font-semibold text-foreground">{state.overview.tasks.total}</div>
            </SectionCard>
          </section>

          <section className="grid gap-4 xl:grid-cols-[1.15fr_0.85fr]">
            <SectionCard
              description="Current cluster counters and queue state."
              title="Operations Summary"
            >
              <div className="grid gap-3 md:grid-cols-2">
                <div className="rounded-lg border border-border bg-muted/30 px-3 py-3">
                  <div className="text-xs uppercase tracking-[0.16em] text-muted-foreground">Nodes</div>
                  <div className="mt-2 text-sm text-foreground">
                    alive {state.overview.nodes.alive} / draining {state.overview.nodes.draining}
                  </div>
                </div>
                <div className="rounded-lg border border-border bg-muted/30 px-3 py-3">
                  <div className="text-xs uppercase tracking-[0.16em] text-muted-foreground">Slots</div>
                  <div className="mt-2 text-sm text-foreground">
                    ready {state.overview.slots.ready} / quorum lost {state.overview.slots.quorum_lost}
                  </div>
                </div>
                <div className="rounded-lg border border-border bg-muted/30 px-3 py-3">
                  <div className="text-xs uppercase tracking-[0.16em] text-muted-foreground">Tasks</div>
                  <div className="mt-2 text-sm text-foreground">
                    pending {state.overview.tasks.pending} / retrying {state.overview.tasks.retrying}
                  </div>
                </div>
                <div className="rounded-lg border border-border bg-muted/30 px-3 py-3">
                  <div className="text-xs uppercase tracking-[0.16em] text-muted-foreground">Generated</div>
                  <div className="mt-2 text-sm text-foreground">{formatTimestamp(state.overview.generated_at)}</div>
                </div>
              </div>
            </SectionCard>
            <SectionCard
              description="Slot and task anomalies sampled from the manager overview endpoint."
              title="Alert List"
            >
              {alertItems.length > 0 ? (
                <div className="space-y-3">
                  {alertItems.map((item) => (
                    <div className="rounded-lg border border-border bg-muted/20 px-3 py-3" key={item.key}>
                      <div className="flex items-center justify-between gap-3">
                        <div className="text-sm font-medium text-foreground">{item.title}</div>
                        <StatusBadge value={item.tone} />
                      </div>
                      <div className="mt-2 text-sm text-muted-foreground">{item.detail}</div>
                    </div>
                  ))}
                </div>
              ) : (
                <ResourceState kind="empty" title="Alert List" />
              )}
            </SectionCard>
          </section>

          <section>
            <SectionCard
              description="Current reconcile queue from the manager tasks endpoint."
              title="Control Queue"
            >
              {state.tasks.items.length > 0 ? (
                <div className="overflow-x-auto rounded-lg border border-border">
                  <table className="w-full border-collapse">
                    <thead className="bg-muted/40 text-left text-xs uppercase tracking-[0.14em] text-muted-foreground">
                      <tr>
                        <th className="px-3 py-3">Slot</th>
                        <th className="px-3 py-3">Kind</th>
                        <th className="px-3 py-3">Status</th>
                        <th className="px-3 py-3">Attempt</th>
                        <th className="px-3 py-3">Last error</th>
                      </tr>
                    </thead>
                    <tbody>{state.tasks.items.map(renderTaskRow)}</tbody>
                  </table>
                </div>
              ) : (
                <ResourceState kind="empty" title="Control Queue" />
              )}
            </SectionCard>
          </section>
        </>
      ) : null}
    </PageContainer>
  )
}
