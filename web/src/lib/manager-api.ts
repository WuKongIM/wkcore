import { getManagerApiBaseUrl } from "@/lib/env"

export type ManagerAuthConfig = {
  getAccessToken: () => string
  onUnauthorized: () => void
}

export type ManagerLoginCredentials = {
  username: string
  password: string
}

export type ManagerPermission = {
  resource: string
  actions: string[]
}

export type ManagerSession = {
  username: string
  tokenType: string
  accessToken: string
  expiresAt: string
  permissions: ManagerPermission[]
}

type ManagerErrorResponse = {
  error?: string
  message?: string
}

type ManagerLoginResponse = {
  username: string
  token_type: string
  access_token: string
  expires_at: string
  permissions: ManagerPermission[]
}

export class ManagerApiError extends Error {
  status: number
  error: string

  constructor(status: number, error: string, message: string) {
    super(message)
    this.name = "ManagerApiError"
    this.status = status
    this.error = error
  }
}

let authConfig: ManagerAuthConfig | null = null

export function configureManagerAuth(config: ManagerAuthConfig) {
  authConfig = config
}

export function resetManagerAuthConfig() {
  authConfig = null
}

function buildManagerUrl(path: string) {
  const base = getManagerApiBaseUrl()
  if (!base) {
    return path
  }

  return `${base}${path.startsWith("/") ? path : `/${path}`}`
}

async function parseManagerError(response: Response) {
  let payload: ManagerErrorResponse | null = null

  try {
    payload = (await response.json()) as ManagerErrorResponse
  } catch {
    payload = null
  }

  return new ManagerApiError(
    response.status,
    payload?.error ?? "request_failed",
    payload?.message ?? `Request failed with status ${response.status}`,
  )
}

export async function managerFetch(path: string, init?: RequestInit) {
  const headers = new Headers(init?.headers)
  headers.set("Accept", "application/json")

  if (init?.body && !headers.has("Content-Type")) {
    headers.set("Content-Type", "application/json")
  }

  const token = authConfig?.getAccessToken()?.trim()
  if (token) {
    headers.set("Authorization", `Bearer ${token}`)
  }

  const response = await fetch(buildManagerUrl(path), {
    ...init,
    headers,
  })

  if (!response.ok) {
    const error = await parseManagerError(response)
    if (response.status === 401) {
      authConfig?.onUnauthorized()
    }
    throw error
  }

  return response
}

export async function loginManager(credentials: ManagerLoginCredentials): Promise<ManagerSession> {
  const response = await managerFetch("/manager/login", {
    method: "POST",
    body: JSON.stringify(credentials),
  })
  const payload = (await response.json()) as ManagerLoginResponse

  return {
    username: payload.username,
    tokenType: payload.token_type,
    accessToken: payload.access_token,
    expiresAt: payload.expires_at,
    permissions: payload.permissions,
  }
}
