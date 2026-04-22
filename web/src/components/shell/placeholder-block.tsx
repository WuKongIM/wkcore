import { cn } from "@/lib/utils"

type PlaceholderBlockProps = {
  kind?: "panel" | "list" | "table" | "canvas" | "detail"
  className?: string
}

const kindClasses: Record<NonNullable<PlaceholderBlockProps["kind"]>, string> = {
  panel: "min-h-48",
  list: "min-h-40",
  table: "min-h-64",
  canvas: "min-h-[26rem]",
  detail: "min-h-56",
}

export function PlaceholderBlock({
  kind = "panel",
  className,
}: PlaceholderBlockProps) {
  return (
    <div
      className={cn(
        "relative overflow-hidden rounded-2xl border border-dashed border-border/80 bg-muted/40",
        "before:absolute before:inset-x-0 before:top-0 before:h-px before:bg-linear-to-r before:from-transparent before:via-primary/35 before:to-transparent",
        "after:absolute after:inset-0 after:bg-[linear-gradient(135deg,rgba(255,255,255,0.45),transparent_45%)]",
        kindClasses[kind],
        className,
      )}
    >
      <div className="absolute inset-x-4 top-4 flex gap-2 opacity-80">
        <span className="h-2.5 w-14 rounded-full bg-border/80" />
        <span className="h-2.5 w-10 rounded-full bg-border/60" />
      </div>
      <div className="absolute inset-x-4 bottom-4 grid gap-2">
        <span className="h-2 rounded-full bg-border/80" />
        <span className="h-2 w-4/5 rounded-full bg-border/60" />
        <span className="h-2 w-3/5 rounded-full bg-border/50" />
      </div>
    </div>
  )
}
