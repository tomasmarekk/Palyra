# Web Console Runtime

This package now hosts the M35 Web Console v1 surface.

## What ships in M35

- Cookie-session web console with CSRF-protected mutating workflows.
- Operator pages for:
  - approvals inbox + approve/deny decisions,
  - cron job create/list/enable-disable/run-now and run log inspection,
  - memory search and scoped purge,
  - skills install/verify/audit/quarantine/enable,
  - audit event browsing with server-side filters.
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
