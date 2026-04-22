import { useCallback, useEffect, useMemo, useState } from "react"

import { useAuthStore } from "@/auth/auth-store"
import { ConfirmDialog } from "@/components/manager/confirm-dialog"
import { DetailSheet } from "@/components/manager/detail-sheet"
import { KeyValueList } from "@/components/manager/key-value-list"
import { ResourceState } from "@/components/manager/resource-state"
import { StatusBadge } from "@/components/manager/status-badge"
import { TableToolbar } from "@/components/manager/table-toolbar"
import { Button } from "@/components/ui/button"
import { PageContainer } from "@/components/shell/page-container"
import { PageHeader } from "@/components/shell/page-header"
import { SectionCard } from "@/components/shell/section-card"
import {
  ManagerApiError,
  getNode,
  getNodes,
  markNodeDraining,
  resumeNode,
} from "@/lib/manager-api"
import type {
  ManagerNodeDetailResponse,
  ManagerNodesResponse,
} from "@/lib/manager-api.types"

type NodeAction = "drain" | "resume" | null

type NodesState = {
  nodes: ManagerNodesResponse | null
  loading: boolean
  refreshing: boolean
  error: Error | null
}

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

export function NodesPage() {
  const permissions = useAuthStore((state) => state.permissions)
  const canWriteNodes = useMemo(
    () => hasPermission(permissions, "cluster.node", "w"),
    [permissions],
  )
  const [state, setState] = useState<NodesState>({
    nodes: null,
    loading: true,
    refreshing: false,
    error: null,
  })
  const [selectedNodeId, setSelectedNodeId] = useState<number | null>(null)
  const [detail, setDetail] = useState<ManagerNodeDetailResponse | null>(null)
  const [detailLoading, setDetailLoading] = useState(false)
  const [detailError, setDetailError] = useState<Error | null>(null)
  const [pendingAction, setPendingAction] = useState<NodeAction>(null)
  const [actionError, setActionError] = useState<string>("")
  const [actionPending, setActionPending] = useState(false)

  const loadNodes = useCallback(async (refreshing: boolean) => {
    setState((current) => ({
      ...current,
      loading: refreshing ? current.loading : true,
      refreshing,
      error: null,
    }))

    try {
      const nodes = await getNodes()
      setState({ nodes, loading: false, refreshing: false, error: null })
    } catch (error) {
      setState({
        nodes: null,
        loading: false,
        refreshing: false,
        error: error instanceof Error ? error : new Error("node request failed"),
      })
    }
  }, [])

  const loadNodeDetail = useCallback(async (nodeId: number) => {
    setDetailLoading(true)
    setDetailError(null)

    try {
      const nextDetail = await getNode(nodeId)
      setDetail(nextDetail)
    } catch (error) {
      setDetail(null)
      setDetailError(error instanceof Error ? error : new Error("node detail failed"))
    } finally {
      setDetailLoading(false)
    }
  }, [])

  useEffect(() => {
    void loadNodes(false)
  }, [loadNodes])

  const openDetail = useCallback(
    async (nodeId: number) => {
      setSelectedNodeId(nodeId)
      await loadNodeDetail(nodeId)
    },
    [loadNodeDetail],
  )

  const closeDetail = useCallback((open: boolean) => {
    if (open) {
      return
    }
    setSelectedNodeId(null)
    setDetail(null)
    setDetailError(null)
    setPendingAction(null)
    setActionError("")
  }, [])

  const runAction = useCallback(async () => {
    if (!selectedNodeId || !pendingAction) {
      return
    }

    setActionPending(true)
    setActionError("")

    try {
      if (pendingAction === "drain") {
        await markNodeDraining(selectedNodeId)
      } else {
        await resumeNode(selectedNodeId)
      }
      setPendingAction(null)
      await loadNodes(true)
      await loadNodeDetail(selectedNodeId)
    } catch (error) {
      setActionError(error instanceof Error ? error.message : "node action failed")
    } finally {
      setActionPending(false)
    }
  }, [loadNodeDetail, loadNodes, pendingAction, selectedNodeId])

  return (
    <PageContainer>
      <PageHeader
        title="Nodes"
        description="Node inventory, roles, and runtime status."
        actions={
          <>
            <Button
              onClick={() => {
                void loadNodes(true)
              }}
              size="sm"
              variant="outline"
            >
              {state.refreshing ? "Refreshing..." : "Refresh"}
            </Button>
            <Button disabled size="sm">
              Inspect
            </Button>
          </>
        }
      >
        <div className="flex flex-wrap gap-2 text-xs text-muted-foreground">
          <div className="rounded-md border border-border bg-background px-3 py-2">
            Scope: all nodes
          </div>
          <div className="rounded-md border border-border bg-background px-3 py-2">
            {state.nodes ? `Total: ${state.nodes.total}` : "Total: pending"}
          </div>
        </div>
      </PageHeader>

      {state.loading ? <ResourceState kind="loading" title="Nodes" /> : null}
      {!state.loading && state.error ? (
        <ResourceState
          kind={mapErrorKind(state.error)}
          onRetry={() => {
            void loadNodes(false)
          }}
          title="Nodes"
        />
      ) : null}
      {!state.loading && !state.error && state.nodes ? (
        <SectionCard
          description="Current node placement, role, and lifecycle state from the manager API."
          title="Node Inventory"
        >
          <TableToolbar
            description="Inspect a node for hosted slot details or run lifecycle actions."
            onRefresh={() => {
              void loadNodes(true)
            }}
            refreshing={state.refreshing}
            title="Cluster nodes"
          />
          {state.nodes.items.length > 0 ? (
            <div className="overflow-x-auto rounded-lg border border-border">
              <table className="w-full border-collapse">
                <thead className="bg-muted/40 text-left text-xs uppercase tracking-[0.14em] text-muted-foreground">
                  <tr>
                    <th className="px-3 py-3">Node</th>
                    <th className="px-3 py-3">Address</th>
                    <th className="px-3 py-3">Status</th>
                    <th className="px-3 py-3">Controller</th>
                    <th className="px-3 py-3">Slots</th>
                    <th className="px-3 py-3">Actions</th>
                  </tr>
                </thead>
                <tbody>
                  {state.nodes.items.map((node) => (
                    <tr className="border-t border-border" key={node.node_id}>
                      <td className="px-3 py-3 text-sm font-medium text-foreground">{node.node_id}</td>
                      <td className="px-3 py-3 text-sm text-foreground">{node.addr}</td>
                      <td className="px-3 py-3 text-sm text-foreground">
                        <StatusBadge value={node.status} />
                      </td>
                      <td className="px-3 py-3 text-sm text-muted-foreground">{node.controller.role}</td>
                      <td className="px-3 py-3 text-sm text-muted-foreground">
                        {node.slot_stats.count} / leaders {node.slot_stats.leader_count}
                      </td>
                      <td className="px-3 py-3 text-sm text-foreground">
                        <div className="flex items-center gap-2">
                          <Button
                            aria-label={`Inspect node ${node.node_id}`}
                            onClick={() => {
                              void openDetail(node.node_id)
                            }}
                            size="sm"
                            variant="outline"
                          >
                            Inspect
                          </Button>
                          <Button
                            disabled={!canWriteNodes}
                            onClick={() => {
                              setSelectedNodeId(node.node_id)
                              setPendingAction(node.status === "draining" ? "resume" : "drain")
                              setActionError("")
                            }}
                            size="sm"
                            variant="outline"
                          >
                            {node.status === "draining" ? "Resume" : "Drain"}
                          </Button>
                        </div>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          ) : (
            <ResourceState kind="empty" title="Node Inventory" />
          )}
        </SectionCard>
      ) : null}

      <DetailSheet
        description={detail ? `Address ${detail.addr}` : "Manager node detail"}
        footer={
          detail ? (
            <div className="flex items-center justify-end gap-2">
              <Button
                disabled={!canWriteNodes}
                onClick={() => {
                  setPendingAction(detail.status === "draining" ? "resume" : "drain")
                  setActionError("")
                }}
                size="sm"
              >
                {detail.status === "draining" ? "Resume node" : "Drain node"}
              </Button>
            </div>
          ) : null
        }
        onOpenChange={closeDetail}
        open={selectedNodeId !== null}
        title={detail ? `Node ${detail.node_id}` : "Node detail"}
      >
        {detailLoading ? <ResourceState kind="loading" title="Node detail" /> : null}
        {!detailLoading && detailError ? (
          <ResourceState
            kind={mapErrorKind(detailError)}
            onRetry={() => {
              if (selectedNodeId) {
                void loadNodeDetail(selectedNodeId)
              }
            }}
            title="Node detail"
          />
        ) : null}
        {!detailLoading && !detailError && detail ? (
          <div className="space-y-4">
            <KeyValueList
              items={[
                { label: "Address", value: detail.addr },
                { label: "Status", value: <StatusBadge value={detail.status} /> },
                { label: "Controller role", value: detail.controller.role },
                { label: "Last heartbeat", value: formatTimestamp(detail.last_heartbeat_at) },
                { label: "Capacity weight", value: detail.capacity_weight },
                { label: "Hosted IDs", value: detail.slots.hosted_ids.join(", ") || "-" },
                { label: "Leader IDs", value: detail.slots.leader_ids.join(", ") || "-" },
              ]}
            />
          </div>
        ) : null}
      </DetailSheet>

      <ConfirmDialog
        confirmLabel="Confirm"
        description={
          pendingAction === "drain"
            ? `Move traffic away from node ${selectedNodeId}.`
            : `Resume normal scheduling for node ${selectedNodeId}.`
        }
        error={actionError}
        onConfirm={() => {
          void runAction()
        }}
        onOpenChange={(open) => {
          if (!open) {
            setPendingAction(null)
            setActionError("")
          }
        }}
        open={pendingAction !== null}
        pending={actionPending}
        title={pendingAction === "resume" ? "Resume node" : "Drain node"}
      />
    </PageContainer>
  )
}
