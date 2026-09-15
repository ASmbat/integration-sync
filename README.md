# integration-sync

A small Go framework for scheduling and monitoring third-party API
integrations — the pattern behind most "connect your vendor" features:
each integration runs on its own schedule, reports a normalized status,
and can be triggered on demand, without the scheduler or API layer ever
needing to know vendor-specific details.

## Why this exists

A compact demonstration of the integration-framework pattern: authentication,
incremental sync, rate-limit handling, and error normalization, behind a
single `Connector` interface — so adding the next vendor is new code in one
file, not a change to the scheduler or API.

## How it's structured

- **`internal/connector`** — the `Connector` interface plus implementations.
  - `github.go` is a real connector: it hits the public GitHub API, handles
    auth headers, parses rate-limit response headers, and distinguishes
    "vendor says slow down" (`rate_limited`) from a hard failure (`error`).
  - `mock.go` is a deterministic fake vendor used for local demos and tests
    without depending on network access or real rate limits.
- **`internal/scheduler`** — runs each registered connector on its own
  interval in its own goroutine, records results, and exposes `SyncNow` for
  on-demand/manual syncs (what a "reconnect" or "backfill" button would call).
- **`internal/store`** — an interface for persisting the latest status per
  connector, with an in-memory implementation. A production version would
  implement the same interface against a real database.
- **`internal/handler`** — the HTTP API surface.
- **`cmd/server`** — wiring and graceful shutdown.

## API

| Method | Path                      | Description                          |
|--------|---------------------------|----------------------------------------|
| GET    | `/connectors`             | Latest status of every connector       |
| GET    | `/connectors/{id}`        | Latest status of one connector         |
| POST   | `/connectors/{id}/sync`   | Trigger an immediate, out-of-cycle sync|
| GET    | `/healthz`                | Liveness check                         |

Example:

```bash
curl -X POST localhost:8080/connectors/github/sync
curl localhost:8080/connectors
```

```json
{
  "github": {
    "status": "ok",
    "records_synced": 12,
    "message": "58 API calls remaining this window",
    "synced_at": "2026-09-15T10:00:00Z"
  },
  "billing-vendor-demo": {
    "status": "rate_limited",
    "message": "simulated rate limit",
    "synced_at": "2026-09-15T10:00:30Z"
  }
}
```

## Running locally

```bash
make run
# optionally: GITHUB_USERNAME=someuser GITHUB_TOKEN=ghp_xxx make run
```

Without a token, the GitHub connector still works but is subject to GitHub's
low unauthenticated rate limit — a good way to actually see the
`rate_limited` status path exercised.

## Tests

```bash
make test
```

Covers the mock connector's failure cadence, the scheduler's immediate-sync
and manual-sync behavior, and the HTTP handlers' status codes.

## Design notes / trade-offs

- **Status granularity**: `ok` / `error` / `rate_limited` / `never_run` is
  intentionally small. A real system would likely add `partial` (some
  records synced before failure) and structured error codes.
- **In-memory store**: chosen for a runnable, dependency-free example. The
  `store.StatusStore` interface is the seam for swapping in Postgres or
  similar without touching the scheduler or handlers.
- **No credential storage/rotation** included — out of scope for this
  example, but would sit behind the same `Connector` interface (a
  connector's constructor takes credentials; rotation just means
  constructing a new instance with fresh ones).
