# Security Policy

This document describes how to report security vulnerabilities in **Palyra** and what to expect from the maintainer(s).

> Please **do not** open public GitHub issues for security vulnerabilities.  
> Public disclosure can put users at risk before a fix is available.

## Supported Versions

Palyra is currently developed on the `main` branch.

Security fixes are provided for:
- the **current `main` branch**, and
- the **latest released version** (if/when releases are published).

If you are using an older release, you may be asked to reproduce the issue on the latest version or `main`.

## Reporting a Vulnerability

### Preferred channel: GitHub private vulnerability reporting

Use the **“Report a vulnerability”** button in the repository’s *Security* tab, or open a new private advisory directly:

- https://github.com/marektomas-cz/Palyra/security/advisories/new

This creates a **private** report visible only to repository maintainers and allows coordinated disclosure.

### Alternative channel (optional)

If you cannot use GitHub private reporting, you can contact the maintainer(s) out-of-band:

- Email: **info@marektomas.com**

## What to include in the report

To help us triage and fix the issue quickly, include:
- A clear description of the vulnerability and potential impact
- Affected component(s) / crate(s) / binary(ies)
- Steps to reproduce (ideally a minimal PoC)
- Any known mitigations or workarounds
- The commit hash, tag, or version you tested
- Logs or stack traces (sanitize secrets)

If the report includes sensitive data (tokens, credentials, customer data), **remove it** before sending.

## Response and disclosure timeline

This project aims to:
- work towards a fix as quickly as practical, depending on severity and complexity

## Security best practices for contributors

- Keep CI green and do not bypass security checks.
- Avoid introducing new third‑party dependencies without justification.
- Do not commit secrets; use test tokens and local `.env` files excluded by `.gitignore`.
- If you add or change GitHub Actions, keep them **pinned** (this repo enforces pinned actions in CI).

Thank you for helping improve Palyra’s security.