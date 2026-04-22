import { useLocation } from "react-router-dom"

import { Button } from "@/components/ui/button"
import { pageMetadata } from "@/lib/navigation"

export function Topbar() {
  const location = useLocation()
  const page = pageMetadata.get(location.pathname) ?? pageMetadata.get("/dashboard")

  return (
    <header className="border-b border-border bg-background/90 px-8 py-5 backdrop-blur" role="banner">
      <div className="flex items-start justify-between gap-6">
        <div>
          <h1 className="text-2xl font-semibold tracking-tight text-foreground">
            {page?.title}
          </h1>
          <p className="mt-2 text-sm text-muted-foreground">{page?.description}</p>
        </div>
        <div className="flex items-center gap-3">
          <Button size="sm" variant="outline">
            Refresh
          </Button>
          <Button size="sm">Search</Button>
        </div>
      </div>
    </header>
  )
}
