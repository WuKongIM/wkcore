import { useCallback, useEffect, useMemo, useState } from "react"

import { DetailSheet } from "@/components/manager/detail-sheet"
import { KeyValueList } from "@/components/manager/key-value-list"
import { ResourceState } from "@/components/manager/resource-state"
import { StatusBadge } from "@/components/manager/status-badge"
import { TableToolbar } from "@/components/manager/table-toolbar"
import { Button } from "@/components/ui/button"
import { PageContainer } from "@/components/shell/page-container"
import { PageHeader } from "@/components/shell/page-header"
import { SectionCard } from "@/components/shell/section-card"
import { ManagerApiError, getConnection, getConnections } from "@/lib/manager-api"
import type {
  ManagerConnectionDetailResponse,
  ManagerConnectionsResponse,
} from "@/lib/manager-api.types"

type ConnectionsState = {
  connections: ManagerConnectionsResponse | null
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

function formatDevice(value: { device_flag: string; device_level: string }) {
  return `${value.device_flag} / ${value.device_level}`
}

export function ConnectionsPage() {
  const [state, setState] = useState<ConnectionsState>({
    connections: null,
    loading: true,
    refreshing: false,
    error: null,
  })
  const [selectedSessionId, setSelectedSessionId] = useState<number | null>(null)
  const [detail, setDetail] = useState<ManagerConnectionDetailResponse | null>(null)
  const [detailLoading, setDetailLoading] = useState(false)
  const [detailError, setDetailError] = useState<Error | null>(null)

  const loadConnections = useCallback(async (refreshing: boolean) => {
    setState((current) => ({
      ...current,
      loading: refreshing ? current.loading : true,
      refreshing,
      error: null,
    }))

    try {
      const connections = await getConnections()
      setState({ connections, loading: false, refreshing: false, error: null })
    } catch (error) {
      setState({
        connections: null,
        loading: false,
        refreshing: false,
        error: error instanceof Error ? error : new Error("connection request failed"),
      })
    }
  }, [])

  const loadConnectionDetail = useCallback(async (sessionId: number) => {
    setDetailLoading(true)
    setDetailError(null)

    try {
      const nextDetail = await getConnection(sessionId)
      setDetail(nextDetail)
    } catch (error) {
      setDetail(null)
      setDetailError(error instanceof Error ? error : new Error("connection detail failed"))
    } finally {
      setDetailLoading(false)
    }
  }, [])

  useEffect(() => {
    void loadConnections(false)
  }, [loadConnections])

  const openDetail = useCallback(
    async (sessionId: number) => {
      setSelectedSessionId(sessionId)
      await loadConnectionDetail(sessionId)
    },
    [loadConnectionDetail],
  )

  const closeDetail = useCallback((open: boolean) => {
    if (open) {
      return
    }
    setSelectedSessionId(null)
    setDetail(null)
    setDetailError(null)
  }, [])

  const summary = useMemo(() => {
    const items = state.connections?.items ?? []
    return {
      total: items.length,
      users: new Set(items.map((item) => item.uid)).size,
      slots: new Set(items.map((item) => item.slot_id)).size,
      listeners: new Set(items.map((item) => item.listener)).size,
    }
  }, [state.connections])

  return (
    <PageContainer>
      <PageHeader
        title="Connections"
        description="Local connection inventory, listener surface, and per-session detail."
        actions={
          <Button
            onClick={() => {
              void loadConnections(true)
            }}
            size="sm"
            variant="outline"
          >
            {state.refreshing ? "Refreshing..." : "Refresh"}
          </Button>
        }
      >
        <div className="flex flex-wrap gap-2 text-xs text-muted-foreground">
          <div className="rounded-md border border-border bg-background px-3 py-2">
            Scope: local node
          </div>
          <div className="rounded-md border border-border bg-background px-3 py-2">
            {state.connections ? `Total: ${state.connections.total}` : "Total: pending"}
          </div>
        </div>
      </PageHeader>

      {state.loading ? <ResourceState kind="loading" title="Connections" /> : null}
      {!state.loading && state.error ? (
        <ResourceState
          kind={mapErrorKind(state.error)}
          onRetry={() => {
            void loadConnections(false)
          }}
          title="Connections"
        />
      ) : null}
      {!state.loading && !state.error && state.connections ? (
        <>
          <section className="grid gap-3 md:grid-cols-2 xl:grid-cols-4">
            <SectionCard description="Local sessions currently listed by the manager API." title="Sessions">
              <div className="text-3xl font-semibold text-foreground">{summary.total}</div>
            </SectionCard>
            <SectionCard description="Distinct user IDs represented in the local view." title="Users">
              <div className="text-3xl font-semibold text-foreground">{summary.users}</div>
            </SectionCard>
            <SectionCard description="Distinct slot IDs represented in the local view." title="Slots">
              <div className="text-3xl font-semibold text-foreground">{summary.slots}</div>
            </SectionCard>
            <SectionCard description="Listener types represented in the local view." title="Listeners">
              <div className="text-3xl font-semibold text-foreground">{summary.listeners}</div>
            </SectionCard>
          </section>

          <SectionCard
            description="Current local connection records from the manager connections endpoints."
            title="Connection Inventory"
          >
            <TableToolbar
              description="Inspect one connection to view addresses, slot ownership, and session metadata."
              onRefresh={() => {
                void loadConnections(true)
              }}
              refreshing={state.refreshing}
              title="Local connections"
            />
            {state.connections.items.length > 0 ? (
              <div className="overflow-x-auto rounded-lg border border-border">
                <table className="w-full border-collapse">
                  <thead className="bg-muted/40 text-left text-xs uppercase tracking-[0.14em] text-muted-foreground">
                    <tr>
                      <th className="px-3 py-3">Session</th>
                      <th className="px-3 py-3">UID</th>
                      <th className="px-3 py-3">Device ID</th>
                      <th className="px-3 py-3">Device</th>
                      <th className="px-3 py-3">Listener</th>
                      <th className="px-3 py-3">State</th>
                      <th className="px-3 py-3">Connected At</th>
                      <th className="px-3 py-3">Actions</th>
                    </tr>
                  </thead>
                  <tbody>
                    {state.connections.items.map((connection) => (
                      <tr className="border-t border-border" key={connection.session_id}>
                        <td className="px-3 py-3 text-sm font-medium text-foreground">
                          {connection.session_id}
                        </td>
                        <td className="px-3 py-3 text-sm text-foreground">{connection.uid}</td>
                        <td className="px-3 py-3 text-sm text-muted-foreground">
                          {connection.device_id}
                        </td>
                        <td className="px-3 py-3 text-sm text-muted-foreground">
                          {formatDevice(connection)}
                        </td>
                        <td className="px-3 py-3 text-sm text-muted-foreground">
                          {connection.listener}
                        </td>
                        <td className="px-3 py-3 text-sm text-foreground">
                          <StatusBadge value={connection.state} />
                        </td>
                        <td className="px-3 py-3 text-sm text-muted-foreground">
                          {formatTimestamp(connection.connected_at)}
                        </td>
                        <td className="px-3 py-3 text-sm text-foreground">
                          <Button
                            aria-label={`Inspect connection ${connection.session_id}`}
                            onClick={() => {
                              void openDetail(connection.session_id)
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
              <ResourceState kind="empty" title="Connection Inventory" />
            )}
          </SectionCard>
        </>
      ) : null}

      <DetailSheet
        description={detail ? `UID ${detail.uid}` : "Manager connection detail"}
        onOpenChange={closeDetail}
        open={selectedSessionId !== null}
        title={detail ? `Connection ${detail.session_id}` : "Connection detail"}
      >
        {detailLoading ? <ResourceState kind="loading" title="Connection detail" /> : null}
        {!detailLoading && detailError ? (
          <ResourceState
            kind={mapErrorKind(detailError)}
            onRetry={() => {
              if (selectedSessionId !== null) {
                void loadConnectionDetail(selectedSessionId)
              }
            }}
            title="Connection detail"
          />
        ) : null}
        {!detailLoading && !detailError && detail ? (
          <KeyValueList
            items={[
              { label: "Session ID", value: detail.session_id },
              { label: "UID", value: detail.uid },
              { label: "Device ID", value: detail.device_id },
              { label: "Device flag", value: detail.device_flag },
              { label: "Device level", value: detail.device_level },
              { label: "Slot ID", value: detail.slot_id },
              { label: "Listener", value: detail.listener },
              { label: "State", value: <StatusBadge value={detail.state} /> },
              { label: "Connected at", value: formatTimestamp(detail.connected_at) },
              { label: "Remote address", value: detail.remote_addr || "-" },
              { label: "Local address", value: detail.local_addr || "-" },
            ]}
          />
        ) : null}
      </DetailSheet>
    </PageContainer>
  )
}
