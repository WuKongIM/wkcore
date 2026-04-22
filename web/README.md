# Web Admin Shell

## Commands
- `bun install`
- `bun run dev`
- `bun run test`
- `bun run build`

## Scope
The web app now includes the first authenticated manager shell flow:

- `/login` authenticates against `POST /manager/login`
- protected routes require a valid persisted JWT session
- `VITE_API_BASE_URL` optionally overrides the default same-origin `/manager/*` base

It still keeps the rest of the admin experience intentionally lightweight:

- no real nodes/slots/overview data fetching yet
- no charts or topology rendering
- no permission-based navigation trimming yet
