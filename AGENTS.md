# AGENTS.md - FF Indexer v2

Top-level repo contract for coding agents.

Tool-specific adapters live in `.cursor/`, `.codex/`, `opencode.json`, and `prompts/`.

## Purpose

`ff-indexer-v2` indexes blockchain activity and media/metadata state for Feral File services and exposes reliable downstream data and APIs.

## Authoritative read order

Read these files before making changes in the matching area:

1. `AGENTS.md` - repository workflow, definition of done, review loop, and documentation obligations
2. `README.md` - repo overview, component map, and documentation index
3. `DEVELOPMENT.md` - local setup, canonical verification command, and service-backed testing requirements
4. `docs/business_requirements.md` - product intent, users, and scope
5. `docs/constraints.md` - compatibility, operational, and security guardrails
6. `docs/api_design.md` - API conventions and contract evolution rules
7. `docs/architecture.md` - system boundaries, component responsibilities, and data flow
8. `docs/schema.md` - persistence contract, migration shape, and test data implications
9. `.github/workflows/lint.yaml` and `.github/workflows/test.yaml` - CI source of truth when changing verification behavior
10. `.github/PULL_REQUEST_TEMPLATE.md` - required PR structure before opening or updating a pull request

If requirements are ambiguous or these sources conflict, stop and ask instead of choosing a new contract silently.

## Coding defaults

- Prefer the simplest change that preserves correctness, reliability, and debuggability.
- Do not invent product, architecture, or API requirements that are not documented.
- Avoid silent failure paths; errors and degraded states should be explicit and actionable.
- Treat tests, generators, schema updates, and docs updates as part of the same change, not follow-up work.

## Engineering principles

Use these principles when evaluating designs, implementations, and questions.

### Simplicity by default

- Default to the simplest design that meets correctness and reliability for our current scale (around 3000 users), even in full production.
- Treat added complexity as a last resort, justified only when simpler designs fail in practice and trade-offs are understood.

### Reliable and transparent behavior

- Reliability comes first: the system may be slow or limited, but it should behave predictably.
- Do not hide system state. If something is slow, missing, or unavailable, make that explicit.
- Errors should be clear and actionable.
- Avoid failing silently or leaving users without a clear next step.

### Community contribution

- Keep the project easy to run locally.
- Keep setup, dependencies, and mental overhead low.

## Coding style

- Follow standard Go style and Go doc conventions.
- Prefer comments more than usual when they add future maintenance value for later agentic coding sessions.
- Use comments to preserve design intent, trade-offs, invariants, failure modes, operational constraints, and reasons a simpler-looking alternative was not chosen.
- Store important amendment context close to the code when that context would otherwise be lost in a later session.
- Do not add filler comments that only restate obvious syntax or line-by-line behavior.

## Product, API, and architecture guidance

Use these as the default sources of truth for scope and public contracts. Keep them aligned when behavior changes:

- `docs/business_requirements.md` - product intent, users, and in/out of scope
- `docs/constraints.md` - data, compatibility, operational, and security guardrails
- `docs/api_design.md` - API conventions and evolution rules
- `docs/architecture.md` - system components and data flow

## Workflow

Default sequence:

`spec -> design -> tasks -> tests -> implementation -> verification -> review -> merge`

For substantial changes such as major features, refactors, schema changes, API changes, or architecture-affecting work:

1. Read the relevant docs and code, including the product/API/architecture sources listed above.
2. Summarize the current behavior, constraints, and invariants.
3. Write or update a short spec, design note, or implementation plan.
4. Define expected behavior and verification.
5. Add or update tests before implementing behavior changes.
6. Implement, verify, and review.

If a substantial change has no spec or design note, do not jump straight to implementation.

## Definition of done

- Run `make check` after substantive code changes. If you cannot run it, say exactly what blocked verification.
- Keep coverage non-regressing versus the base branch. If a necessary change lowers coverage, call it out in the PR and explain why.
- Update `README.md`, `DEVELOPMENT.md`, `docs/`, or other authoritative docs when behavior, architecture, setup, or verification contracts change.
- Reduce branching and nesting before accepting a large function. The strict lint profile treats cyclomatic and cognitive complexity as first-class review concerns.
- Keep changed functions and files small. Add doc comments for changed Go functions so the intent stays readable in future sessions.
- For non-trivial changed Go functions, use the doc comment to capture `Reason:`, `Trade-offs:`, and `Constraints:` so future agents can understand why the current shape exists. This is instruction, not a special lint-only format.
- Review the full diff before handing off work or re-requesting review.
- Keep the PR description aligned with `.github/PULL_REQUEST_TEMPLATE.md`, including validation status and any remaining gaps.

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
- Add doc comments for changed Go functions. For non-trivial functions, prefer doc comments that help a future reader understand purpose, reason, trade-offs, and constraints.

## Verification

Primary verification command:

```bash
make check
```

This is the canonical local verification entrypoint. It runs, in order: `imports` (`goimports`), `fmt-check` (`gofmt -s -l`, matching CI’s go fmt check), `lint-local` (full-repo `golangci-lint` with CGO enabled), and `test` (`CGO_ENABLED=1` `go test -cover ./...`). Run `make fmt` to apply `gofmt -s -w` fixes when `fmt-check` fails.

The lint profile enforces cyclomatic and cognitive complexity, function and file length, and doc quality expectations for the code it analyzes.

**Note on lightweight mode:** The repository supports a CGO-disabled lightweight Docker mode for deployment. Local verification via `make check` runs `CGO_ENABLED=1` tests, which provide full code coverage. The `test-lightweight-build` Makefile target is available for optional lightweight build verification but is not part of the standard `make check` workflow or CI pipeline. Lightweight mode compatibility is verified through Docker builds rather than separate test runs.

CI still defines its own exact steps in `.github/workflows/test.yaml` and `.github/workflows/lint.yaml`. Run additional targeted generation or build checks when relevant. If you cannot run the full expected verification, say so explicitly.

## Review and done

Use the review contract in `prompts/code-review.md`.

Before merge, commit finalization, or PR completion:

1. Prepare a compact handoff with goal, scope, changed files/modules, decisions, tests, checks run, and known limitations.
2. Run a fresh-context review.
3. Address review findings, re-verify, and repeat until the reviewer returns `Verdict: accept`.

A change is done only when implementation, relevant tests, verification, documentation, and review are all complete.

- Prefer waiting for CI status checks before considering a PR done.
- If local `make check` or full CI cannot be run, state that explicitly in the PR description so reviewers know what was validated.
- When creating a GitHub issue, use the repository issue templates in `.github/ISSUE_TEMPLATE/` and complete every requested section.
- When creating a PR, use `.github/PULL_REQUEST_TEMPLATE.md` and keep the description aligned with the template fields.
- Do not replace the template structure with ad hoc prose; add extra context only after the required sections are complete.
- Before requesting review and after addressing review feedback, review the full diff, rerun `make check`, and summarize any remaining unverified risk.

## Commit style

Use Conventional Commits:

- `<type>(<optional-scope>): <description>`
- Types: `feat`, `fix`, `refactor`, `test`, `chore`, `docs`, `build`, `ci`, `perf`, `style`
