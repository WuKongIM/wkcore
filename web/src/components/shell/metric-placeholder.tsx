import { Card, CardContent } from "@/components/ui/card"

type MetricPlaceholderProps = {
  label: string
  hint?: string
}

export function MetricPlaceholder({ label, hint }: MetricPlaceholderProps) {
  return (
    <Card className="bg-card/95">
      <CardContent className="space-y-4 pt-4">
        <div className="flex items-center justify-between gap-3">
          <span className="text-sm font-medium text-foreground">{label}</span>
          <span className="rounded-full bg-primary/8 px-2 py-1 text-[11px] font-medium text-primary">
            Pending
          </span>
        </div>
        <div className="space-y-2">
          <div className="h-8 w-20 rounded-lg bg-muted" />
          <div className="h-2 w-28 rounded-full bg-border/70" />
        </div>
        <p className="text-xs text-muted-foreground">{hint ?? "Waiting for live manager data."}</p>
      </CardContent>
    </Card>
  )
}
