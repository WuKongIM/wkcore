import type { FormEvent } from "react"
import { useCallback, useEffect, useMemo, useState } from "react"

import { useAuthStore } from "@/auth/auth-store"
import { ActionFormDialog } from "@/components/manager/action-form-dialog"
import { ConfirmDialog } from "@/components/manager/confirm-dialog"
import { DetailSheet } from "@/components/manager/detail-sheet"
import { KeyValueList } from "@/components/manager/key-value-list"
import { ResourceState } from "@/components/manager/resource-state"
import { StatusBadge } from "@/components/manager/status-badge"
import { TableToolbar } from "@/components/manager/table-toolbar"
import { PageContainer } from "@/components/shell/page-container"
import { PageHeader } from "@/components/shell/page-header"
import { SectionCard } from "@/components/shell/section-card"
import { Button } from "@/components/ui/button"
import {
  ManagerApiError,
  getSlot,
  getSlots,
  rebalanceSlots,
  recoverSlot,
  transferSlotLeader,
} from "@/lib/manager-api"
import type {
  ManagerSlotDetailResponse,
  ManagerSlotRebalanceResponse,
  ManagerSlotsResponse,
} from "@/lib/manager-api.types"

type SlotsState = {
  slots: ManagerSlotsResponse | null
  loading: boolean
  refreshing: boolean
  error: Error | null
}

const recoverStrategies = [{
  value: "latest_live_replica",
  label: "Latest live replica",
}]

function formatTimestamp(value: string) {
  return new Date(value).toLocaleString()
}

function mapErrorKind(error: Error | null) {
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

function hasPermission(
  permissions: { resource: string; actions: string[] }[],
  resource: string,
  action: string,
) {
  return permissions.some((permission) => {
    if (permission.resource !== resource) {
      return false
    }
    return permission.actions.includes(action) || permission.actions.includes("*")
  })
}

function formatNodeList(nodeIds: number[]) {
  return nodeIds.length > 0 ? nodeIds.join(", ") : "-"
}

export function SlotsPage() {
  const permissions = useAuthStore((state) => state.permissions)
  const canWriteSlots = useMemo(
    () => hasPermission(permissions, "cluster.slot", "w"),
    [permissions],
  )
  const [state, setState] = useState<SlotsState>({
    slots: null,
    loading: true,
    refreshing: false,
    error: null,
  })
  const [selectedSlotId, setSelectedSlotId] = useState<number | null>(null)
  const [detail, setDetail] = useState<ManagerSlotDetailResponse | null>(null)
  const [detailLoading, setDetailLoading] = useState(false)
  const [detailError, setDetailError] = useState<Error | null>(null)
  const [transferOpen, setTransferOpen] = useState(false)
  const [transferPending, setTransferPending] = useState(false)
  const [transferError, setTransferError] = useState("")
  const [targetNodeId, setTargetNodeId] = useState("")
  const [recoverOpen, setRecoverOpen] = useState(false)
  const [recoverPending, setRecoverPending] = useState(false)
  const [recoverError, setRecoverError] = useState("")
  const [recoverStrategy, setRecoverStrategy] = useState(recoverStrategies[0].value)
  const [rebalanceOpen, setRebalanceOpen] = useState(false)
  const [rebalancePending, setRebalancePending] = useState(false)
  const [rebalanceError, setRebalanceError] = useState("")
  const [rebalancePlan, setRebalancePlan] = useState<ManagerSlotRebalanceResponse | null>(null)

  const loadSlots = useCallback(async (refreshing: boolean) => {
    setState((current) => ({
      ...current,
      loading: refreshing ? current.loading : true,
      refreshing,
      error: null,
    }))

    try {
      const slots = await getSlots()
      setState({ slots, loading: false, refreshing: false, error: null })
    } catch (error) {
      setState({
        slots: null,
        loading: false,
        refreshing: false,
        error: error instanceof Error ? error : new Error("slot request failed"),
      })
    }
  }, [])

  const loadSlotDetail = useCallback(async (slotId: number) => {
    setDetailLoading(true)
    setDetailError(null)

    try {
      const nextDetail = await getSlot(slotId)
      setDetail(nextDetail)
    } catch (error) {
      setDetail(null)
      setDetailError(error instanceof Error ? error : new Error("slot detail failed"))
    } finally {
      setDetailLoading(false)
    }
  }, [])

  useEffect(() => {
    void loadSlots(false)
  }, [loadSlots])

  const openDetail = useCallback(
    async (slotId: number) => {
      setSelectedSlotId(slotId)
      await loadSlotDetail(slotId)
    },
    [loadSlotDetail],
  )

  const closeDetail = useCallback((open: boolean) => {
    if (open) {
      return
    }
    setSelectedSlotId(null)
    setDetail(null)
    setDetailError(null)
    setTransferOpen(false)
    setTransferError("")
    setTargetNodeId("")
    setRecoverOpen(false)
    setRecoverError("")
    setRecoverStrategy(recoverStrategies[0].value)
  }, [])

  const refreshOpenDetail = useCallback(async (slotId: number) => {
    await loadSlots(true)
    await loadSlotDetail(slotId)
  }, [loadSlotDetail, loadSlots])

  const submitTransfer = useCallback(async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault()

    if (!selectedSlotId) {
      return
    }

    const parsedTargetNodeId = Number(targetNodeId)
    if (!Number.isInteger(parsedTargetNodeId) || parsedTargetNodeId <= 0) {
      setTransferError("Enter a valid target node ID.")
      return
    }

    setTransferPending(true)
    setTransferError("")

    try {
      await transferSlotLeader(selectedSlotId, { targetNodeId: parsedTargetNodeId })
      setTransferOpen(false)
      setTargetNodeId("")
      await refreshOpenDetail(selectedSlotId)
    } catch (error) {
      setTransferError(error instanceof Error ? error.message : "slot transfer failed")
    } finally {
      setTransferPending(false)
    }
  }, [refreshOpenDetail, selectedSlotId, targetNodeId])

  const submitRecover = useCallback(async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault()

    if (!selectedSlotId) {
      return
    }

    if (!recoverStrategy) {
      setRecoverError("Select a recovery strategy.")
      return
    }

    setRecoverPending(true)
    setRecoverError("")

    try {
      await recoverSlot(selectedSlotId, { strategy: recoverStrategy })
      setRecoverOpen(false)
      await refreshOpenDetail(selectedSlotId)
    } catch (error) {
      setRecoverError(error instanceof Error ? error.message : "slot recovery failed")
    } finally {
      setRecoverPending(false)
    }
  }, [recoverStrategy, refreshOpenDetail, selectedSlotId])

  const runRebalance = useCallback(async () => {
    setRebalancePending(true)
    setRebalanceError("")

    try {
      const nextPlan = await rebalanceSlots()
      setRebalancePlan(nextPlan)
      setRebalanceOpen(false)
    } catch (error) {
      setRebalanceError(error instanceof Error ? error.message : "slot rebalance failed")
    } finally {
      setRebalancePending(false)
    }
  }, [])

  const slotSummary = useMemo(() => {
    const items = state.slots?.items ?? []
    return {
      total: items.length,
      ready: items.filter((item) => item.state.quorum === "ready").length,
      inSync: items.filter((item) => item.state.sync === "in_sync").length,
      leaders: items.filter((item) => item.runtime.leader_id > 0).length,
    }
  }, [state.slots])

  return (
    <PageContainer>
      <PageHeader
        title="Slots"
        description="Slot distribution, leader placement, and movement status."
        actions={
          <>
            <Button
              onClick={() => {
                void loadSlots(true)
              }}
              size="sm"
              variant="outline"
            >
              {state.refreshing ? "Refreshing..." : "Refresh"}
            </Button>
            <Button
              disabled={!canWriteSlots}
              onClick={() => {
                setRebalanceOpen(true)
                setRebalanceError("")
              }}
              size="sm"
            >
              Rebalance slots
            </Button>
          </>
        }
      >
        <div className="flex flex-wrap gap-2 text-xs text-muted-foreground">
          <div className="rounded-md border border-border bg-background px-3 py-2">
            Scope: all slots
          </div>
          <div className="rounded-md border border-border bg-background px-3 py-2">
            {state.slots ? `Total: ${state.slots.total}` : "Total: pending"}
          </div>
        </div>
      </PageHeader>

      {state.loading ? <ResourceState kind="loading" title="Slots" /> : null}
      {!state.loading && state.error ? (
        <ResourceState
          kind={mapErrorKind(state.error)}
          onRetry={() => {
            void loadSlots(false)
          }}
          title="Slots"
        />
      ) : null}
      {!state.loading && !state.error && state.slots ? (
        <>
          <section className="grid gap-3 md:grid-cols-2 xl:grid-cols-4">
            <SectionCard description="Slots currently reporting a leader." title="Leader coverage">
              <div className="text-3xl font-semibold text-foreground">{slotSummary.leaders}</div>
            </SectionCard>
            <SectionCard description="Slots whose quorum state is ready." title="Ready slots">
              <div className="text-3xl font-semibold text-foreground">{slotSummary.ready}</div>
            </SectionCard>
            <SectionCard description="Slots whose sync state is in sync." title="In sync">
              <div className="text-3xl font-semibold text-foreground">{slotSummary.inSync}</div>
            </SectionCard>
            <SectionCard description="Physical slots currently tracked." title="Tracked slots">
              <div className="text-3xl font-semibold text-foreground">{slotSummary.total}</div>
            </SectionCard>
          </section>

          <SectionCard
            description="Current assignment and runtime state from the manager slot endpoints."
            title="Slot Inventory"
          >
            <TableToolbar
              description="Inspect one slot to view task state or run operator actions."
              onRefresh={() => {
                void loadSlots(true)
              }}
              refreshing={state.refreshing}
              title="Cluster slots"
            />
            {state.slots.items.length > 0 ? (
              <div className="overflow-x-auto rounded-lg border border-border">
                <table className="w-full border-collapse">
                  <thead className="bg-muted/40 text-left text-xs uppercase tracking-[0.14em] text-muted-foreground">
                    <tr>
                      <th className="px-3 py-3">Slot</th>
                      <th className="px-3 py-3">Quorum</th>
                      <th className="px-3 py-3">Sync</th>
                      <th className="px-3 py-3">Desired peer set</th>
                      <th className="px-3 py-3">Current peer set</th>
                      <th className="px-3 py-3">Leader</th>
                      <th className="px-3 py-3">Actions</th>
                    </tr>
                  </thead>
                  <tbody>
                    {state.slots.items.map((slot) => (
                      <tr className="border-t border-border" key={slot.slot_id}>
                        <td className="px-3 py-3 text-sm font-medium text-foreground">
                          {`Slot ${slot.slot_id}`}
                        </td>
                        <td className="px-3 py-3 text-sm text-foreground">
                          <StatusBadge value={slot.state.quorum} />
                        </td>
                        <td className="px-3 py-3 text-sm text-foreground">
                          <StatusBadge value={slot.state.sync} />
                        </td>
                        <td className="px-3 py-3 text-sm text-muted-foreground">
                          {formatNodeList(slot.assignment.desired_peers)}
                        </td>
                        <td className="px-3 py-3 text-sm text-muted-foreground">
                          {formatNodeList(slot.runtime.current_peers)}
                        </td>
                        <td className="px-3 py-3 text-sm text-muted-foreground">{slot.runtime.leader_id}</td>
                        <td className="px-3 py-3 text-sm text-foreground">
                          <Button
                            aria-label={`Inspect slot ${slot.slot_id}`}
                            onClick={() => {
                              void openDetail(slot.slot_id)
                            }}
                            size="sm"
                            variant="outline"
                          >
                            Inspect
                          </Button>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            ) : (
              <ResourceState kind="empty" title="Slot Inventory" />
            )}
          </SectionCard>

          {rebalancePlan ? (
            <SectionCard
              description="Latest migration plan returned by the slot rebalance endpoint."
              title="Rebalance Plan"
            >
              {rebalancePlan.items.length > 0 ? (
                <div className="space-y-3">
                  {rebalancePlan.items.map((item) => (
                    <div className="rounded-lg border border-border bg-muted/20 px-3 py-3" key={item.hash_slot}>
                      <div className="text-sm font-medium text-foreground">{`Hash slot ${item.hash_slot}`}</div>
                      <div className="mt-1 text-sm text-muted-foreground">
                        {`From slot ${item.from_slot_id} to slot ${item.to_slot_id}`}
                      </div>
                    </div>
                  ))}
                </div>
              ) : (
                <ResourceState kind="empty" title="Rebalance Plan" />
              )}
            </SectionCard>
          ) : null}
        </>
      ) : null}

      <DetailSheet
        description={detail ? `Leader ${detail.runtime.leader_id}` : "Manager slot detail"}
        footer={
          detail ? (
            <div className="flex items-center justify-end gap-2">
              <Button
                disabled={!canWriteSlots}
                onClick={() => {
                  setTransferOpen(true)
                  setTransferError("")
                }}
                size="sm"
                variant="outline"
              >
                Transfer leader
              </Button>
              <Button
                disabled={!canWriteSlots}
                onClick={() => {
                  setRecoverOpen(true)
                  setRecoverError("")
                }}
                size="sm"
              >
                Recover slot
              </Button>
            </div>
          ) : null
        }
        onOpenChange={closeDetail}
        open={selectedSlotId !== null}
        title={detail ? `Slot ${detail.slot_id}` : "Slot detail"}
      >
        {detailLoading ? <ResourceState kind="loading" title="Slot detail" /> : null}
        {!detailLoading && detailError ? (
          <ResourceState
            kind={mapErrorKind(detailError)}
            onRetry={() => {
              if (selectedSlotId) {
                void loadSlotDetail(selectedSlotId)
              }
            }}
            title="Slot detail"
          />
        ) : null}
        {!detailLoading && !detailError && detail ? (
          <KeyValueList
            items={[
              { label: "Desired peers", value: formatNodeList(detail.assignment.desired_peers) },
              { label: "Current peers", value: formatNodeList(detail.runtime.current_peers) },
              { label: "Task status", value: detail.task ? <StatusBadge value={detail.task.status} /> : "-" },
              { label: "Task step", value: detail.task?.step ?? "-" },
              { label: "Quorum", value: <StatusBadge value={detail.state.quorum} /> },
              { label: "Sync", value: <StatusBadge value={detail.state.sync} /> },
              { label: "Leader ID", value: detail.runtime.leader_id },
              { label: "Healthy voters", value: detail.runtime.healthy_voters },
              { label: "Config epoch", value: detail.assignment.config_epoch },
              { label: "Observed epoch", value: detail.runtime.observed_config_epoch },
              { label: "Last report", value: formatTimestamp(detail.runtime.last_report_at) },
              { label: "Last error", value: detail.task?.last_error || "-" },
            ]}
          />
        ) : null}
      </DetailSheet>

      <ActionFormDialog
        description={selectedSlotId ? `Move slot ${selectedSlotId} leadership to another assigned node.` : undefined}
        error={transferError}
        onOpenChange={(open) => {
          setTransferOpen(open)
          if (!open) {
            setTransferError("")
            setTargetNodeId("")
          }
        }}
        onSubmit={(event) => {
          void submitTransfer(event)
        }}
        open={transferOpen}
        pending={transferPending}
        submitLabel="Transfer"
        title="Transfer leader"
      >
        <label className="grid gap-2 text-sm text-foreground">
          <span>Target node ID</span>
          <input
            className="rounded-md border border-border bg-background px-3 py-2 text-sm outline-none"
            inputMode="numeric"
            onChange={(event) => setTargetNodeId(event.target.value)}
            value={targetNodeId}
          />
        </label>
      </ActionFormDialog>

      <ActionFormDialog
        description={selectedSlotId ? `Run a manager recover flow for slot ${selectedSlotId}.` : undefined}
        error={recoverError}
        onOpenChange={(open) => {
          setRecoverOpen(open)
          if (!open) {
            setRecoverError("")
            setRecoverStrategy(recoverStrategies[0].value)
          }
        }}
        onSubmit={(event) => {
          void submitRecover(event)
        }}
        open={recoverOpen}
        pending={recoverPending}
        submitLabel="Recover"
        title="Recover slot"
      >
        <label className="grid gap-2 text-sm text-foreground">
          <span>Recovery strategy</span>
          <select
            className="rounded-md border border-border bg-background px-3 py-2 text-sm outline-none"
            onChange={(event) => setRecoverStrategy(event.target.value)}
            value={recoverStrategy}
          >
            {recoverStrategies.map((strategy) => (
              <option key={strategy.value} value={strategy.value}>
                {strategy.label}
              </option>
            ))}
          </select>
        </label>
      </ActionFormDialog>

      <ConfirmDialog
        confirmLabel="Confirm"
        description="Generate a slot rebalance migration plan through the manager API."
        error={rebalanceError}
        onConfirm={() => {
          void runRebalance()
        }}
        onOpenChange={(open) => {
          setRebalanceOpen(open)
          if (!open) {
            setRebalanceError("")
          }
        }}
        open={rebalanceOpen}
        pending={rebalancePending}
        title="Rebalance slots"
      />
    </PageContainer>
  )
}
