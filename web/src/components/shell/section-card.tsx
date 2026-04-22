import type { PropsWithChildren, ReactNode } from "react"

import {
  Card,
  CardAction,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card"
import { cn } from "@/lib/utils"

type SectionCardProps = PropsWithChildren<{
  title: string
  description?: string
  action?: ReactNode
  className?: string
}>

export function SectionCard({
  title,
  description,
  action,
  className,
  children,
}: SectionCardProps) {
  return (
    <Card
      className={cn(
        "border border-white/65 bg-white/88 shadow-[0_22px_48px_-34px_rgba(15,23,42,0.42)] backdrop-blur-sm",
        className,
      )}
    >
      <CardHeader className="border-b border-border/60 bg-linear-to-r from-white via-white to-primary/4">
        <CardTitle className="tracking-[-0.015em] text-foreground">{title}</CardTitle>
        {description ? <CardDescription className="leading-6">{description}</CardDescription> : null}
        {action ? <CardAction>{action}</CardAction> : null}
      </CardHeader>
      <CardContent className="pt-5">{children}</CardContent>
    </Card>
  )
}
