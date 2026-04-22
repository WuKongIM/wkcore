import { useState, type FormEvent } from "react"
import { useNavigate } from "react-router-dom"

import { useAuthStore } from "@/auth/auth-store"
import { Button } from "@/components/ui/button"
import { ManagerApiError } from "@/lib/manager-api"

function getLoginErrorMessage(error: unknown) {
  if (error instanceof ManagerApiError) {
    if (error.status === 400) {
      return "Request is invalid."
    }
    if (error.status === 401) {
      return "Invalid username or password."
    }
    if (error.status >= 500) {
      return "Login service is unavailable. Please try again."
    }
  }

  return "Unable to sign in right now."
}

export function LoginPage() {
  const login = useAuthStore((state) => state.login)
  const navigate = useNavigate()
  const [username, setUsername] = useState("")
  const [password, setPassword] = useState("")
  const [isSubmitting, setIsSubmitting] = useState(false)
  const [errorMessage, setErrorMessage] = useState("")

  async function handleSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault()
    setIsSubmitting(true)
    setErrorMessage("")

    try {
      await login({ username, password })
      navigate("/dashboard", { replace: true })
    } catch (error) {
      setErrorMessage(getLoginErrorMessage(error))
    } finally {
      setIsSubmitting(false)
    }
  }

  return (
    <main className="min-h-screen bg-background px-6 py-12">
      <div className="mx-auto flex min-h-[calc(100vh-6rem)] w-full max-w-6xl items-center gap-10 lg:grid lg:grid-cols-[1.15fr_0.85fr]">
        <section className="max-w-2xl">
          <div className="text-[11px] font-semibold uppercase tracking-[0.28em] text-muted-foreground">
            WuKongIM Manager
          </div>
          <h1 className="mt-4 text-4xl font-semibold tracking-tight text-foreground sm:text-5xl">
            Sign in
          </h1>
          <p className="mt-4 max-w-xl text-sm leading-7 text-muted-foreground sm:text-base">
            Authenticate to inspect nodes, slots, channels, and runtime control views from a
            single monochrome operations console.
          </p>
          <div className="mt-8 grid gap-3 text-sm text-muted-foreground sm:max-w-xl sm:grid-cols-3">
            <div className="rounded-xl border border-border bg-card px-4 py-4">
              Node inventory
            </div>
            <div className="rounded-xl border border-border bg-card px-4 py-4">
              Slot coordination
            </div>
            <div className="rounded-xl border border-border bg-card px-4 py-4">
              Runtime status
            </div>
          </div>
        </section>

        <section className="w-full rounded-2xl border border-border bg-card p-8 text-card-foreground shadow-sm">
          <div className="text-[11px] font-semibold uppercase tracking-[0.24em] text-muted-foreground">
            Cluster access
          </div>
          <h2 className="mt-3 text-2xl font-semibold tracking-tight text-foreground">
            Manager credentials
          </h2>
          <p className="mt-2 text-sm text-muted-foreground">
            Use the static manager account configured by the server.
          </p>

          <form className="mt-8 space-y-5" onSubmit={handleSubmit}>
            <label className="block space-y-2">
              <span className="text-sm font-medium text-foreground">Username</span>
              <input
                autoComplete="username"
                className="w-full rounded-xl border border-input bg-background px-3 py-2.5 text-sm text-foreground outline-none transition focus:border-foreground"
                name="username"
                onChange={(event) => setUsername(event.target.value)}
                type="text"
                value={username}
              />
            </label>

            <label className="block space-y-2">
              <span className="text-sm font-medium text-foreground">Password</span>
              <input
                autoComplete="current-password"
                className="w-full rounded-xl border border-input bg-background px-3 py-2.5 text-sm text-foreground outline-none transition focus:border-foreground"
                name="password"
                onChange={(event) => setPassword(event.target.value)}
                type="password"
                value={password}
              />
            </label>

            {errorMessage ? (
              <div
                aria-live="polite"
                className="rounded-xl border border-destructive/20 bg-destructive/5 px-3 py-2 text-sm text-destructive"
                role="alert"
              >
                {errorMessage}
              </div>
            ) : null}

            <Button className="w-full" disabled={isSubmitting} size="lg" type="submit">
              {isSubmitting ? "Signing in..." : "Sign in"}
            </Button>
          </form>
        </section>
      </div>
    </main>
  )
}
