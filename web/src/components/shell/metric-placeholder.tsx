import { Activity, ArrowUpRight } from "lucide-react"

import { Card, CardContent } from "@/components/ui/card"

type MetricPlaceholderProps = {
  label: string
  hint?: string
}

export function MetricPlaceholder({ label, hint }: MetricPlaceholderProps) {
  return (
    <Card className="border border-white/70 bg-white/90 shadow-[0_22px_48px_-34px_rgba(15,23,42,0.38)]">
      <CardContent className="space-y-4 pt-4">
        <div className="flex items-center justify-between gap-3">
          <span className="text-sm font-medium tracking-[0.01em] text-foreground">{label}</span>
          <span className="inline-flex items-center gap-1 rounded-full border border-emerald-500/10 bg-emerald-500/10 px-2.5 py-1 text-[11px] font-medium text-emerald-700">
            <Activity className="size-3" />
            Ready
          </span>
        </div>
        <div className="rounded-[22px] border border-primary/10 bg-linear-to-br from-primary/10 via-primary/5 to-transparent p-4">
          <div className="flex items-start justify-between gap-3">
            <div className="space-y-2">
              <div className="h-9 w-24 rounded-xl bg-white/75 shadow-inner shadow-primary/8" />
              <div className="h-2 w-24 rounded-full bg-primary/14" />
            </div>
            <ArrowUpRight className="size-4 text-primary/80" />
          </div>
        </div>
        <p className="text-xs leading-5 text-muted-foreground">
          {hint ?? "Waiting for live manager data."}
        </p>
      </CardContent>
    </Card>
  )
}
