import { Bell, Search, Sparkles } from "lucide-react"
import { useLocation } from "react-router-dom"

import { Button } from "@/components/ui/button"
import { pageMetadata } from "@/lib/navigation"

export function Topbar() {
  const location = useLocation()
  const page = pageMetadata.get(location.pathname) ?? pageMetadata.get("/dashboard")

  return (
    <header className="border-b border-white/60 bg-background/80 px-8 py-5 backdrop-blur-xl" role="banner">
      <div className="flex items-start justify-between gap-6">
        <div className="space-y-3">
          <div className="flex flex-wrap items-center gap-2">
            <span className="rounded-full border border-primary/15 bg-primary/8 px-3 py-1 text-[11px] font-semibold uppercase tracking-[0.24em] text-primary">
              Control plane
            </span>
            <span className="rounded-full border border-border/80 bg-white/75 px-3 py-1 text-[11px] font-medium uppercase tracking-[0.22em] text-muted-foreground shadow-sm">
              Manager shell
            </span>
          </div>
          <div>
            <h1 className="text-3xl font-semibold tracking-[-0.02em] text-foreground">
              {page?.title}
            </h1>
            <p className="mt-2 max-w-2xl text-sm leading-6 text-muted-foreground">
              {page?.description}
            </p>
          </div>
        </div>
        <div className="flex flex-wrap items-center justify-end gap-3">
          <div className="hidden items-center gap-2 rounded-2xl border border-white/70 bg-white/85 px-3 py-2 text-xs text-muted-foreground shadow-sm md:flex">
            <Sparkles className="size-3.5 text-primary" />
            <span>Precision Console</span>
          </div>
          <Button size="sm" variant="outline">
            <Bell className="size-3.5" />
            Alerts
          </Button>
          <Button size="sm" variant="outline">
            Refresh
          </Button>
          <Button size="sm">
            <Search className="size-3.5" />
            Search
          </Button>
        </div>
      </div>
    </header>
  )
}
