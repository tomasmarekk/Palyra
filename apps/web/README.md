# Web Console Runtime

This package now hosts the Web Console runtime through M39 diagnostics/telemetry surfaces.

## What ships in M39

- Cookie-session web console with CSRF-protected mutating workflows.
- Operator pages for:
  - approvals inbox + approve/deny decisions,
  - cron job create/list/enable-disable/run-now and run log inspection,
  - memory search and scoped purge,
  - skills install/verify/audit/quarantine/enable,
  - browser profile/relay/download controls,
  - audit event browsing with server-side filters,
  - diagnostics snapshot surface (model provider status + rate limits, auth profile health, browserd sessions/budgets/failures).
- Sensitive-value safety baseline in UI:
  - sensitive keys are redacted by default,
  - explicit reveal toggle is required.
- Typed `ConsoleApiClient` for `/console/v1/*` endpoints.
- Frontend tests for auth gating and approval/cron operator workflows.

## Local commands

- Install dependencies:
  - `npm --prefix apps/web ci`
- Lint:
  - `npm --prefix apps/web run lint`
- Typecheck:
  - `npm --prefix apps/web run typecheck`
- Tests:
  - `npm --prefix apps/web run test:run`
- Build:
  - `npm --prefix apps/web run build`

## Notes

- The console is designed for same-origin deployment with `palyrad` `/console/v1` HTTP routes.
- API calls always use `credentials: include` to bind requests to the session cookie.
