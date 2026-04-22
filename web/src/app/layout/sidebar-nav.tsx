import { Cpu, Orbit, ShieldCheck } from "lucide-react"
import { NavLink } from "react-router-dom"

import { cn } from "@/lib/utils"
import { navigationGroups } from "@/lib/navigation"

export function SidebarNav() {
  return (
    <nav
      aria-label="Primary navigation"
      className="flex w-80 shrink-0 flex-col border-r border-sidebar-border bg-sidebar/90 px-5 py-6 backdrop-blur-xl"
    >
      <div className="rounded-3xl border border-white/60 bg-white/75 px-4 py-4 shadow-[0_20px_45px_-28px_rgba(15,23,42,0.35)]">
        <div className="flex items-start justify-between gap-4">
          <div>
            <div className="text-[11px] font-semibold uppercase tracking-[0.28em] text-primary/80">
              Precision Console
            </div>
            <div className="mt-2 text-xl font-semibold text-foreground">WuKongIM</div>
            <p className="mt-1 text-sm leading-6 text-muted-foreground">
              Runtime surfaces, cluster posture, and manager-facing shell state.
            </p>
          </div>
          <div className="rounded-2xl bg-primary/10 p-2 text-primary shadow-inner shadow-primary/10">
            <Orbit className="size-5" />
          </div>
        </div>
      </div>

      <div className="mt-8 space-y-6">
        {navigationGroups.map((group) => (
          <section key={group.label} className="space-y-2">
            <div className="px-3 text-[11px] font-semibold uppercase tracking-[0.28em] text-muted-foreground/90">
              {group.label}
            </div>
            <div className="space-y-1.5">
              {group.items.map((item) => (
                <NavLink
                  key={item.href}
                  aria-label={item.title}
                  className={({ isActive }) =>
                    cn(
                      "group relative block overflow-hidden rounded-2xl border px-4 py-3 text-sm transition-all",
                      isActive
                        ? "border-primary/20 bg-white text-foreground shadow-[0_14px_32px_-24px_rgba(37,99,235,0.75)]"
                        : "border-transparent text-muted-foreground hover:border-border/70 hover:bg-white/70 hover:text-foreground",
                    )
                  }
                  to={item.href}
                >
                  {({ isActive }) => (
                    <>
                      <span
                        className={cn(
                          "absolute inset-y-3 left-1.5 w-1 rounded-full transition-all",
                          isActive ? "bg-primary shadow-[0_0_18px_rgba(37,99,235,0.55)]" : "bg-transparent",
                        )}
                      />
                      <span className="pl-3 font-medium tracking-[0.01em]">{item.title}</span>
                      <span className="mt-1 block pl-3 text-xs leading-5 text-muted-foreground">
                        {item.description}
                      </span>
                    </>
                  )}
                </NavLink>
              ))}
            </div>
          </section>
        ))}
      </div>

      <div className="mt-auto rounded-3xl border border-primary/10 bg-linear-to-br from-slate-950 to-slate-900 px-4 py-4 text-slate-50 shadow-[0_22px_48px_-28px_rgba(15,23,42,0.55)]">
        <div className="flex items-center justify-between gap-3">
          <div>
            <div className="text-[11px] font-semibold uppercase tracking-[0.28em] text-slate-400">
              Cluster status
            </div>
            <div className="mt-2 text-sm font-medium text-white">Single-node cluster</div>
          </div>
          <div className="rounded-2xl bg-emerald-400/12 p-2 text-emerald-300">
            <ShieldCheck className="size-4.5" />
          </div>
        </div>
        <div className="mt-4 space-y-2 text-xs text-slate-300/90">
          <div className="flex items-center justify-between gap-3 rounded-2xl border border-white/8 bg-white/6 px-3 py-2">
            <span className="inline-flex items-center gap-2">
              <span className="size-2 rounded-full bg-emerald-300" />
              Stable shell
            </span>
            <span>Ready</span>
          </div>
          <div className="flex items-center justify-between gap-3 rounded-2xl border border-white/8 bg-white/6 px-3 py-2">
            <span className="inline-flex items-center gap-2">
              <Cpu className="size-3.5" />
              No live feed yet
            </span>
            <span>Static</span>
          </div>
        </div>
      </div>
    </nav>
  )
}
