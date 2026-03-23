# AGENTS.md - FF Indexer v2

Top-level repo contract for coding agents.

Tool-specific adapters live in `.cursor/`, `.codex/`, `opencode.json`, and `prompts/`.

## Purpose

`ff-indexer-v2` indexes blockchain activity and media/metadata state for Feral File services and exposes reliable downstream data and APIs.

## Coding defaults

- Prefer the simplest change that preserves correctness, reliability, and debuggability.
- Do not invent product, architecture, or API requirements that are not documented.
- Avoid silent failure paths; errors and degraded states should be explicit and actionable.
- Treat tests, generators, schema updates, and docs updates as part of the same change, not follow-up work.

## Coding style

- Follow standard Go style and Go doc conventions.
- Prefer comments more than usual when they add future maintenance value for later agentic coding sessions.
- Use comments to preserve design intent, trade-offs, invariants, failure modes, operational constraints, and reasons a simpler-looking alternative was not chosen.
- Store important amendment context close to the code when that context would otherwise be lost in a later session.
- Do not add filler comments that only restate obvious syntax or line-by-line behavior.

## Owner-owned guidance

These files are placeholders and should not be treated as fully defined policy until the repo owner fills them in:

- `docs/business_requirements.md`
- `docs/constraints.md`
- `docs/api_design.md`

Architecture boundaries are also still `TBD by repo owner`.

## Workflow

Default sequence:

`spec -> design -> tasks -> tests -> implementation -> verification -> review -> merge`

For substantial changes such as major features, refactors, schema changes, API changes, or architecture-affecting work:

1. Read the relevant docs and code, including the owner-owned docs above when available.
2. Summarize the current behavior, constraints, and invariants.
3. Write or update a short spec, design note, or implementation plan.
4. Define expected behavior and verification.
5. Add or update tests before implementing behavior changes.
6. Implement, verify, and review.

If a substantial change has no spec or design note, do not jump straight to implementation.

## Repo-specific change rules

- After GraphQL schema changes:

```bash
cd internal/api/graphql
go run github.com/99designs/gqlgen generate
```

- When interfaces with `//go:generate mockgen` are added, changed, or removed:

```bash
go generate ./...
```

- Do not edit generated mocks manually.
- For DB schema changes, update both `db/migrations/` and `db/init_pg_db.sql`, and update `pg_test_data` when needed.
- Update `docs/`, `README.md`, or `DEVELOPMENT.md` when behavior, setup, architecture, or operations materially change.
- For non-trivial exported Go types and functions, prefer Go doc comments that help a future reader understand purpose and constraints.

## Verification

Primary verification command:

```bash
make check
```

Run additional targeted generation or build checks when relevant. If you cannot run the full expected verification, say so explicitly.

## Review and done

Use the review contract in `prompts/code-review.md`.

Before merge, commit finalization, or PR completion:

1. Prepare a compact handoff with goal, scope, changed files/modules, decisions, tests, checks run, and known limitations.
2. Run a fresh-context review.
3. Address review findings, re-verify, and repeat until the reviewer returns `Verdict: accept`.

A change is done only when implementation, relevant tests, verification, documentation, and review are all complete.

## Commit style

Use Conventional Commits:

- `<type>(<optional-scope>): <description>`
- Types: `feat`, `fix`, `refactor`, `test`, `chore`, `docs`, `build`, `ci`, `perf`, `style`
