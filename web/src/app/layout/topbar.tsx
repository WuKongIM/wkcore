import { useLocation } from "react-router-dom"

import { pageTitles } from "@/lib/navigation"

export function Topbar() {
  const location = useLocation()
  const title = pageTitles.get(location.pathname) ?? "Dashboard"

  return (
    <header className="border-b border-border bg-background/90 px-8 py-5 backdrop-blur" role="banner">
      <h1 className="text-2xl font-semibold tracking-tight text-foreground">{title}</h1>
    </header>
  )
}
