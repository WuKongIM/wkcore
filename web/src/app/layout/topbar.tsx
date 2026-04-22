import { Search } from "lucide-react"
import { useLocation } from "react-router-dom"

import { useAuthStore } from "@/auth/auth-store"
import { Button } from "@/components/ui/button"
import { pageMetadata } from "@/lib/navigation"

export function Topbar() {
  const location = useLocation()
  const page = pageMetadata.get(location.pathname) ?? pageMetadata.get("/dashboard")
  const username = useAuthStore((state) => state.username)
  const logout = useAuthStore((state) => state.logout)

  return (
    <header className="border-b border-border bg-background px-6 py-3" role="banner">
      <div className="flex items-center justify-between gap-4">
        <div className="min-w-0">
          <div className="text-sm font-semibold text-foreground">{page?.title}</div>
          <p className="text-xs text-muted-foreground">{page?.description}</p>
        </div>
        <div className="flex items-center gap-3">
          <div className="flex items-center gap-2">
            <Button size="sm" variant="outline">
              Refresh
            </Button>
            <Button size="sm" variant="outline">
              <Search className="size-3.5" />
              Search
            </Button>
          </div>
          <div className="flex items-center gap-2 border-l border-border pl-3">
            <span className="text-xs text-muted-foreground">{username}</span>
            <Button onClick={logout} size="sm" variant="outline">
              Logout
            </Button>
          </div>
        </div>
      </div>
    </header>
  )
}
