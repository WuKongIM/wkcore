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

const kindOverlays: Record<NonNullable<PlaceholderBlockProps["kind"]>, string> = {
  panel: "bg-[radial-gradient(circle_at_top_right,rgba(37,99,235,0.08),transparent_36%)]",
  list: "bg-[linear-gradient(180deg,rgba(37,99,235,0.05),transparent_32%)]",
  table: "bg-[linear-gradient(180deg,rgba(255,255,255,0.66),rgba(255,255,255,0.1)),linear-gradient(90deg,rgba(37,99,235,0.04)_1px,transparent_1px)] bg-[size:auto,64px_64px]",
  canvas: "bg-[radial-gradient(circle_at_center,rgba(37,99,235,0.08),transparent_44%),linear-gradient(90deg,rgba(37,99,235,0.05)_1px,transparent_1px),linear-gradient(rgba(37,99,235,0.05)_1px,transparent_1px)] bg-[size:auto,32px_32px,32px_32px]",
  detail: "bg-[linear-gradient(160deg,rgba(37,99,235,0.08),transparent_42%)]",
}

export function PlaceholderBlock({
  kind = "panel",
  className,
}: PlaceholderBlockProps) {
  return (
    <div
      className={cn(
        "relative overflow-hidden rounded-[24px] border border-dashed border-primary/18 bg-muted/55",
        "before:absolute before:inset-x-0 before:top-0 before:h-px before:bg-linear-to-r before:from-transparent before:via-primary/45 before:to-transparent",
        "after:absolute after:inset-0 after:bg-[linear-gradient(135deg,rgba(255,255,255,0.46),transparent_44%)]",
        kindClasses[kind],
        kindOverlays[kind],
        className,
      )}
    >
      <div className="absolute inset-x-4 top-4 flex gap-2 opacity-90">
        <span className="h-2.5 w-14 rounded-full bg-primary/18" />
        <span className="h-2.5 w-10 rounded-full bg-border/80" />
      </div>
      <div className="absolute inset-x-4 top-12 space-y-2 opacity-90">
        <span className="block h-2.5 w-2/3 rounded-full bg-border/90" />
        {kind === "table" ? (
          <>
            <span className="block h-px w-full bg-border/80" />
            <span className="block h-px w-full bg-border/60" />
            <span className="block h-px w-full bg-border/40" />
          </>
        ) : null}
      </div>
      <div className="absolute inset-x-4 bottom-4 grid gap-2">
        <span className="h-2 rounded-full bg-border/85" />
        <span className="h-2 w-4/5 rounded-full bg-border/65" />
        <span className="h-2 w-3/5 rounded-full bg-border/50" />
      </div>
    </div>
  )
}
