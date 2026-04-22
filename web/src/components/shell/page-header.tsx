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
        "relative overflow-hidden rounded-[28px] border border-white/70 bg-linear-to-br from-white via-white/95 to-primary/6 p-6 shadow-[0_26px_54px_-34px_rgba(15,23,42,0.35)]",
        className,
      )}
    >
      <div className="absolute inset-x-0 top-0 h-px bg-linear-to-r from-transparent via-primary/60 to-transparent" />
      <div className="relative flex flex-col gap-5 lg:flex-row lg:items-start lg:justify-between">
        <div className="space-y-3">
          {eyebrow ? (
            <div className="inline-flex items-center rounded-full border border-primary/10 bg-primary/8 px-3 py-1 text-[11px] font-semibold uppercase tracking-[0.28em] text-primary">
              {eyebrow}
            </div>
          ) : null}
          <div className="space-y-2">
            <div className="text-[1.65rem] font-semibold tracking-[-0.03em] text-foreground">
              {title}
            </div>
            <p className="max-w-3xl text-sm leading-6 text-muted-foreground">{description}</p>
          </div>
        </div>
        {actions ? <div className="flex flex-wrap items-center gap-3">{actions}</div> : null}
      </div>
      {children ? <div className="relative mt-6">{children}</div> : null}
    </section>
  )
}
