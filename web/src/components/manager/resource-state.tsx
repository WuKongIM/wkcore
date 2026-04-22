import { Button } from "@/components/ui/button"

type ResourceStateKind = "loading" | "empty" | "forbidden" | "unavailable" | "error"

type ResourceStateProps = {
  kind: ResourceStateKind
  title: string
  description?: string
  retryLabel?: string
  onRetry?: () => void
}

const defaultDescriptions: Record<ResourceStateKind, string> = {
  loading: "Loading manager data.",
  empty: "No manager data is available for this view yet.",
  forbidden: "You do not have permission to view this manager resource.",
  unavailable: "The manager service is currently unavailable.",
  error: "The manager request failed.",
}

export function ResourceState({
  kind,
  title,
  description,
  retryLabel = "Retry",
  onRetry,
}: ResourceStateProps) {
  return (
    <div
      className="rounded-xl border border-border bg-card px-5 py-6 text-sm text-muted-foreground"
      data-kind={kind}
      role="status"
    >
      <div className="text-sm font-semibold text-foreground">{title}</div>
      <p className="mt-2 max-w-2xl leading-6">{description ?? defaultDescriptions[kind]}</p>
      {onRetry ? (
        <Button className="mt-4" onClick={onRetry} size="sm" variant="outline">
          {retryLabel}
        </Button>
      ) : null}
    </div>
  )
}
