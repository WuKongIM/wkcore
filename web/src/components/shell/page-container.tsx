import type { PropsWithChildren } from "react"

import { cn } from "@/lib/utils"

type PageContainerProps = PropsWithChildren<{
  className?: string
}>

export function PageContainer({ children, className }: PageContainerProps) {
  return (
    <div className={cn("mx-auto flex w-full max-w-7xl flex-col gap-6 px-8 py-8", className)}>
      {children}
    </div>
  )
}
