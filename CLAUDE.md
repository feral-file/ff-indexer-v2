# CLAUDE.md - FF Indexer v2

Use `AGENTS.md` as the source of truth for this repository.

Key repo rules for Claude Code:

- Follow the repo workflow: `spec -> design -> tasks -> tests -> implementation -> verification -> review -> merge`
- Read the authoritative docs listed in `AGENTS.md` before making substantial changes
- Run `make check` as the canonical local verification command for substantive work
- Keep coverage non-regressing versus the base branch unless the PR documents the exception
- Add doc comments for changed Go functions and packages
- For non-trivial changed Go functions, use the doc comment to capture reason, trade-offs, and constraints
- Keep changes simple: reduce branching, nesting, and oversized functions/files before accepting extra complexity
- Use Conventional Commits and keep PR bodies aligned with `.github/PULL_REQUEST_TEMPLATE.md`
- If full verification cannot run, say exactly what blocked it and what was run instead

When this file and `AGENTS.md` differ, follow `AGENTS.md`.
