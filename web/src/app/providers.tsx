import { TooltipProvider } from "@/components/ui/tooltip"

type AppProvidersProps = {
  children: React.ReactNode
}

export function AppProviders({ children }: AppProvidersProps) {
  return <TooltipProvider>{children}</TooltipProvider>
}
