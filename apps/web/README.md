# Web Console Runtime

This package now hosts the Web Console runtime.

## What ships in M39

- Cookie-session web console with CSRF-protected mutating workflows.
- Operator pages for:
  - approvals inbox + approve/deny decisions,
  - cron job create/list/enable-disable/run-now and run log inspection,
  - memory search and scoped purge,
  - skills install/verify/audit/quarantine/enable,
  - browser profile/relay/download controls,
  - audit event browsing with server-side filters,
  - diagnostics snapshot surface (model provider status + rate limits, auth profile health, browserd sessions/budgets/failures, deployment posture/bind exposure summary).
- Sensitive-value safety baseline in UI:
  - sensitive keys are redacted by default,
  - explicit reveal toggle is required.
- Typed `ConsoleApiClient` for `/console/v1/*` endpoints.
- Frontend tests for auth gating and approval/cron operator workflows.

## Local commands

- Canonical bootstrap:
  - `npm --prefix apps/web run bootstrap`
- Verify installed dependencies:
  - `npm --prefix apps/web run verify-install`
- Remove generated outputs and installed dependencies:
  - `npm --prefix apps/web run clean`
- Clean-room rebuild proof:
  - `npm --prefix apps/web run cleanroom:check`
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
- `apps/web/node_modules` is not a supported handoff artifact. Always rebuild from
  `package-lock.json` via `npm --prefix apps/web run bootstrap`.
- The clean-room guard verifies Node/npm ranges, launcher permissions, and the expected Rollup
  native optional package for the current OS/architecture.
