export function LoginPage() {
  return (
    <main className="flex min-h-screen items-center justify-center bg-background px-6 py-12">
      <section className="w-full max-w-md rounded-xl border border-border bg-card p-8 text-card-foreground">
        <div className="text-[11px] font-semibold uppercase tracking-[0.24em] text-muted-foreground">
          WuKongIM Manager
        </div>
        <h1 className="mt-3 text-2xl font-semibold tracking-tight text-foreground">Sign in</h1>
        <p className="mt-2 text-sm text-muted-foreground">
          Authenticate to access cluster and runtime management views.
        </p>
      </section>
    </main>
  )
}
