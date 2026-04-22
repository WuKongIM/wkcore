import { useCallback, useEffect, useMemo, useState } from "react"

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
  getChannelRuntimeMeta,
  getChannelRuntimeMetaDetail,
} from "@/lib/manager-api"
import type {
  ManagerChannelRuntimeMetaDetailResponse,
  ManagerChannelRuntimeMetaListResponse,
} from "@/lib/manager-api.types"

type ChannelsState = {
  channels: ManagerChannelRuntimeMetaListResponse | null
  loading: boolean
  refreshing: boolean
  loadingMore: boolean
  error: Error | null
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

function formatNodeList(nodeIds: number[]) {
  return nodeIds.length > 0 ? nodeIds.join(", ") : "-"
}

export function ChannelsPage() {
  const [state, setState] = useState<ChannelsState>({
    channels: null,
    loading: true,
    refreshing: false,
    loadingMore: false,
    error: null,
  })
  const [selectedChannel, setSelectedChannel] = useState<{
    channelId: string
    channelType: number
  } | null>(null)
  const [detail, setDetail] = useState<ManagerChannelRuntimeMetaDetailResponse | null>(null)
  const [detailLoading, setDetailLoading] = useState(false)
  const [detailError, setDetailError] = useState<Error | null>(null)

  const loadChannels = useCallback(async (refreshing: boolean) => {
    setState((current) => ({
      ...current,
      loading: refreshing ? current.loading : true,
      refreshing,
      error: null,
    }))

    try {
      const channels = await getChannelRuntimeMeta()
      setState({
        channels,
        loading: false,
        refreshing: false,
        loadingMore: false,
        error: null,
      })
    } catch (error) {
      setState({
        channels: null,
        loading: false,
        refreshing: false,
        loadingMore: false,
        error: error instanceof Error ? error : new Error("channel runtime request failed"),
      })
    }
  }, [])

  const loadMoreChannels = useCallback(async () => {
    const cursor = state.channels?.next_cursor
    if (!cursor) {
      return
    }

    setState((current) => ({
      ...current,
      loadingMore: true,
    }))

    try {
      const nextPage = await getChannelRuntimeMeta({ cursor })
      setState((current) => ({
        channels: current.channels
          ? {
              items: [...current.channels.items, ...nextPage.items],
              has_more: nextPage.has_more,
              next_cursor: nextPage.next_cursor,
            }
          : nextPage,
        loading: false,
        refreshing: false,
        loadingMore: false,
        error: null,
      }))
    } catch (error) {
      setState((current) => ({
        ...current,
        loadingMore: false,
        error: error instanceof Error ? error : new Error("channel runtime request failed"),
      }))
    }
  }, [state.channels?.next_cursor])

  const loadChannelDetail = useCallback(async (channelType: number, channelId: string) => {
    setDetailLoading(true)
    setDetailError(null)

    try {
      const nextDetail = await getChannelRuntimeMetaDetail(channelType, channelId)
      setDetail(nextDetail)
    } catch (error) {
      setDetail(null)
      setDetailError(error instanceof Error ? error : new Error("channel runtime detail failed"))
    } finally {
      setDetailLoading(false)
    }
  }, [])

  useEffect(() => {
    void loadChannels(false)
  }, [loadChannels])

  const openDetail = useCallback(
    async (channelType: number, channelId: string) => {
      setSelectedChannel({ channelId, channelType })
      await loadChannelDetail(channelType, channelId)
    },
    [loadChannelDetail],
  )

  const closeDetail = useCallback((open: boolean) => {
    if (open) {
      return
    }
    setSelectedChannel(null)
    setDetail(null)
    setDetailError(null)
  }, [])

  const summary = useMemo(() => {
    const items = state.channels?.items ?? []
    return {
      total: items.length,
      active: items.filter((item) => item.status === "active").length,
      slots: new Set(items.map((item) => item.slot_id)).size,
      leaders: new Set(items.map((item) => item.leader)).size,
    }
  }, [state.channels])

  return (
    <PageContainer>
      <PageHeader
        title="Channels"
        description="Channel runtime inventory, pagination, and drill-in detail."
        actions={
          <Button
            onClick={() => {
              void loadChannels(true)
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
            Scope: all channels
          </div>
          <div className="rounded-md border border-border bg-background px-3 py-2">
            {state.channels ? `Loaded: ${state.channels.items.length}` : "Loaded: pending"}
          </div>
        </div>
      </PageHeader>

      {state.loading ? <ResourceState kind="loading" title="Channels" /> : null}
      {!state.loading && state.error ? (
        <ResourceState
          kind={mapErrorKind(state.error)}
          onRetry={() => {
            void loadChannels(false)
          }}
          title="Channels"
        />
      ) : null}
      {!state.loading && !state.error && state.channels ? (
        <>
          <section className="grid gap-3 md:grid-cols-2 xl:grid-cols-4">
            <SectionCard description="Total runtime items loaded in this page set." title="Loaded channels">
              <div className="text-3xl font-semibold text-foreground">{summary.total}</div>
            </SectionCard>
            <SectionCard description="Runtime items currently marked active." title="Active">
              <div className="text-3xl font-semibold text-foreground">{summary.active}</div>
            </SectionCard>
            <SectionCard description="Distinct physical slots represented in view." title="Slots">
              <div className="text-3xl font-semibold text-foreground">{summary.slots}</div>
            </SectionCard>
            <SectionCard description="Distinct leader nodes represented in view." title="Leaders">
              <div className="text-3xl font-semibold text-foreground">{summary.leaders}</div>
            </SectionCard>
          </section>

          <SectionCard
            description="Paged runtime metadata from the channel manager endpoints."
            title="Channel Runtime"
          >
            <TableToolbar
              actions={
                state.channels.has_more ? (
                  <Button
                    onClick={() => {
                      void loadMoreChannels()
                    }}
                    size="sm"
                    variant="outline"
                  >
                    {state.loadingMore ? "Loading..." : "Load more"}
                  </Button>
                ) : null
              }
              description="Inspect one channel to view slot ownership and runtime lease metadata."
              onRefresh={() => {
                void loadChannels(true)
              }}
              refreshing={state.refreshing}
              title="Channel inventory"
            />
            {state.channels.items.length > 0 ? (
              <div className="overflow-x-auto rounded-lg border border-border">
                <table className="w-full border-collapse">
                  <thead className="bg-muted/40 text-left text-xs uppercase tracking-[0.14em] text-muted-foreground">
                    <tr>
                      <th className="px-3 py-3">Channel ID</th>
                      <th className="px-3 py-3">Type</th>
                      <th className="px-3 py-3">Slot</th>
                      <th className="px-3 py-3">Leader</th>
                      <th className="px-3 py-3">Status</th>
                      <th className="px-3 py-3">Actions</th>
                    </tr>
                  </thead>
                  <tbody>
                    {state.channels.items.map((channel) => (
                      <tr className="border-t border-border" key={`${channel.channel_type}-${channel.channel_id}`}>
                        <td className="px-3 py-3 text-sm font-medium text-foreground">{channel.channel_id}</td>
                        <td className="px-3 py-3 text-sm text-muted-foreground">{channel.channel_type}</td>
                        <td className="px-3 py-3 text-sm text-muted-foreground">{channel.slot_id}</td>
                        <td className="px-3 py-3 text-sm text-muted-foreground">{channel.leader}</td>
                        <td className="px-3 py-3 text-sm text-foreground">
                          <StatusBadge value={channel.status} />
                        </td>
                        <td className="px-3 py-3 text-sm text-foreground">
                          <Button
                            aria-label={`Inspect channel ${channel.channel_id}`}
                            onClick={() => {
                              void openDetail(channel.channel_type, channel.channel_id)
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
              <ResourceState kind="empty" title="Channel Runtime" />
            )}
          </SectionCard>
        </>
      ) : null}

      <DetailSheet
        description={detail ? `Slot ${detail.slot_id}` : "Manager channel runtime detail"}
        onOpenChange={closeDetail}
        open={selectedChannel !== null}
        title={detail ? `Channel ${detail.channel_id}` : "Channel detail"}
      >
        {detailLoading ? <ResourceState kind="loading" title="Channel detail" /> : null}
        {!detailLoading && detailError ? (
          <ResourceState
            kind={mapErrorKind(detailError)}
            onRetry={() => {
              if (selectedChannel) {
                void loadChannelDetail(selectedChannel.channelType, selectedChannel.channelId)
              }
            }}
            title="Channel detail"
          />
        ) : null}
        {!detailLoading && !detailError && detail ? (
          <KeyValueList
            items={[
              { label: "Channel ID", value: detail.channel_id },
              { label: "Channel type", value: detail.channel_type },
              { label: "Slot ID", value: detail.slot_id },
              { label: "Hash slot", value: detail.hash_slot },
              { label: "Status", value: <StatusBadge value={detail.status} /> },
              { label: "Leader", value: detail.leader },
              { label: "Replicas", value: formatNodeList(detail.replicas) },
              { label: "ISR", value: formatNodeList(detail.isr) },
              { label: "Min ISR", value: detail.min_isr },
              { label: "Channel epoch", value: detail.channel_epoch },
              { label: "Leader epoch", value: detail.leader_epoch },
              { label: "Features", value: detail.features },
              { label: "Lease until ms", value: detail.lease_until_ms },
            ]}
          />
        ) : null}
      </DetailSheet>
    </PageContainer>
  )
}
