import { afterEach, beforeEach, describe, expect, it, vi } from "vitest"

import {
  configureManagerAuth,
  loginManager,
  managerFetch,
  resetManagerAuthConfig,
} from "@/lib/manager-api"

describe("manager api client", () => {
  const fetchMock = vi.fn()

  beforeEach(() => {
    fetchMock.mockReset()
    vi.stubGlobal("fetch", fetchMock)
    resetManagerAuthConfig()
  })

  afterEach(() => {
    vi.unstubAllGlobals()
    vi.unstubAllEnvs()
  })

  it("prefixes requests with VITE_API_BASE_URL and trims trailing slashes", async () => {
    vi.stubEnv("VITE_API_BASE_URL", "http://127.0.0.1:5301/")
    fetchMock.mockResolvedValue(new Response("{}", { status: 200 }))

    await managerFetch("/manager/nodes")

    expect(fetchMock).toHaveBeenCalledWith(
      "http://127.0.0.1:5301/manager/nodes",
      expect.objectContaining({
        headers: expect.any(Headers),
      }),
    )
  })

  it("adds the bearer token when auth is configured", async () => {
    configureManagerAuth({
      getAccessToken: () => "token-1",
      onUnauthorized: vi.fn(),
    })
    fetchMock.mockResolvedValue(new Response("{}", { status: 200 }))

    await managerFetch("/manager/nodes")

    const requestInit = fetchMock.mock.calls[0]?.[1] as { headers: Headers }
    expect(requestInit.headers.get("Authorization")).toBe("Bearer token-1")
  })

  it("calls onUnauthorized on 401 responses", async () => {
    const onUnauthorized = vi.fn()
    configureManagerAuth({ getAccessToken: () => "token-1", onUnauthorized })
    fetchMock.mockResolvedValue(
      new Response('{"error":"unauthorized","message":"unauthorized"}', { status: 401 }),
    )

    await expect(managerFetch("/manager/nodes")).rejects.toMatchObject({
      status: 401,
      error: "unauthorized",
    })
    expect(onUnauthorized).toHaveBeenCalledTimes(1)
  })

  it("maps the login response using current backend fields", async () => {
    fetchMock.mockResolvedValue(
      new Response(
        JSON.stringify({
          username: "admin",
          token_type: "Bearer",
          access_token: "token-1",
          expires_in: 3600,
          expires_at: "2099-04-22T12:00:00Z",
          permissions: [{ resource: "cluster.node", actions: ["r"] }],
        }),
        { status: 200 },
      ),
    )

    await expect(
      loginManager({ username: "admin", password: "secret" }),
    ).resolves.toEqual({
      username: "admin",
      tokenType: "Bearer",
      accessToken: "token-1",
      expiresAt: "2099-04-22T12:00:00Z",
      permissions: [{ resource: "cluster.node", actions: ["r"] }],
    })
  })
})
