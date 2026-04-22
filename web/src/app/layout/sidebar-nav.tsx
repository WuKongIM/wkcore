import { NavLink } from "react-router-dom"

import { cn } from "@/lib/utils"
import { navigationGroups } from "@/lib/navigation"

export function SidebarNav() {
  return (
    <nav
      aria-label="Primary navigation"
      className="flex w-72 shrink-0 flex-col border-r border-sidebar-border bg-sidebar px-4 py-6"
    >
      <div className="mb-8 px-3">
        <div className="text-lg font-semibold text-foreground">WuKongIM</div>
        <p className="mt-1 text-sm text-muted-foreground">Management shell</p>
      </div>
      <div className="space-y-6">
        {navigationGroups.map((group) => (
          <section key={group.label} className="space-y-2">
            <div className="px-3 text-xs font-medium uppercase tracking-[0.2em] text-muted-foreground">
              {group.label}
            </div>
            <div className="space-y-1">
              {group.items.map((item) => (
                <NavLink
                  key={item.href}
                  className={({ isActive }) =>
                    cn(
                      "block rounded-lg px-3 py-2 text-sm transition",
                      isActive
                        ? "bg-background text-foreground shadow-sm"
                        : "text-muted-foreground hover:bg-background hover:text-foreground",
                    )
                  }
                  to={item.href}
                >
                  {item.title}
                </NavLink>
              ))}
            </div>
          </section>
        ))}
      </div>
    </nav>
  )
}
