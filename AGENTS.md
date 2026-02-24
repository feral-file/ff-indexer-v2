# Agent Guidance

When you change code:
- Prefer waiting for CI status checks when opening a PR.
- For local validation, run `make check` (lint + tests + coverage). This requires dependencies like `golangci-lint`.

If you cannot run checks, call that out explicitly in the PR summary.
