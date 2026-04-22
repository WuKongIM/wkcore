import type { PropsWithChildren, ReactNode } from "react"

import { cn } from "@/lib/utils"

type PageHeaderProps = PropsWithChildren<{
  title: string
  description: string
  eyebrow?: string
  actions?: ReactNode
  className?: string
}>

export function PageHeader({
  title,
  description,
  eyebrow,
  actions,
  className,
  children,
}: PageHeaderProps) {
  return (
    <section
      className={cn(
        "rounded-2xl border border-border/80 bg-card/90 p-6 shadow-sm shadow-slate-950/4",
        className,
      )}
    >
      <div className="flex flex-col gap-4 lg:flex-row lg:items-start lg:justify-between">
        <div className="space-y-2">
          {eyebrow ? (
            <p className="text-xs font-medium uppercase tracking-[0.24em] text-muted-foreground">
              {eyebrow}
            </p>
          ) : null}
          <div className="text-xl font-semibold tracking-tight text-foreground">{title}</div>
          <p className="max-w-3xl text-sm leading-6 text-muted-foreground">{description}</p>
        </div>
        {actions ? <div className="flex items-center gap-3">{actions}</div> : null}
      </div>
      {children ? <div className="mt-5">{children}</div> : null}
    </section>
  )
}
