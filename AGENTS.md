# Agent guidance

This document is for AI assistants and automation working on this repository. Apply it when reviewing, designing, or changing code.

## Authoritative read order

Read these files before making changes in the matching area:

1. `AGENTS.md` - repository workflow, definition of done, review loop, and documentation obligations
2. `README.md` - repo overview, component map, and documentation index
3. `DEVELOPMENT.md` - local setup, canonical verification command, and service-backed testing requirements
4. `docs/architecture.md` - system boundaries, component responsibilities, and event/indexing flow
5. `docs/schema.md` - persistence contract, migration shape, and test data implications
6. `.github/workflows/lint.yaml` and `.github/workflows/test.yaml` - CI source of truth when changing verification behavior
7. `.github/PULL_REQUEST_TEMPLATE.md` - required PR structure before opening or updating a pull request

If requirements are ambiguous or these sources conflict, stop and ask instead of choosing a new contract silently.

## Engineering principles

Use these principles when evaluating designs, implementations, and questions.

**How to behave**

- Prefer the principles below when reviewing or proposing solutions.
- When suggesting changes, explain how they align with or improve adherence to these principles.
- If something appears to violate a principle, say so clearly and explain why.
- If a request is vague and compliance with the principles is unclear, ask clarifying questions instead of assuming requirements.
- Do not invent requirements or user needs. Question assumptions about scale, complexity, and necessity.

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

### Common pitfalls

**Over-engineering**

- Complexity without a clear current need.
- Designing for massive scale when the system only needs to support around 3000 users.
- Features or abstractions no one has asked for.
- Heavy infrastructure where simpler options suffice.

**Reliability and quality**

- Weak or missing error handling.
- Errors that are not actionable or informative.
- Too little logging or observability for debugging user issues.
- Logs that are noisy, oversized, or costly.

## Development workflow

### Definition of done

- Run `make post-implementation-check` after substantive code changes. If you cannot run it, say exactly what blocked verification.
- Keep coverage non-regressing versus the base branch. If a necessary change lowers coverage, call it out in the PR and explain why.
- Update `README.md`, `DEVELOPMENT.md`, `docs/`, or other authoritative docs when behavior, architecture, setup, or verification contracts change.
- Reduce branching and nesting before accepting a large function. The strict lint profile treats cyclomatic and cognitive complexity as first-class review concerns.
- Keep changed functions and files small. Add doc comments for changed Go functions so the intent stays readable in future sessions.
- For non-trivial changed Go functions, use the doc comment to capture `Reason:`, `Trade-offs:`, and `Constraints:` so future agents can understand why the current shape exists. This is instruction, not a special lint-only format.
- Review the full diff before handing off work or re-requesting review.
- Keep the PR description aligned with `.github/PULL_REQUEST_TEMPLATE.md`, including validation status and any remaining gaps.

### Code generation

**GraphQL schema**

After the GraphQL schema changes:

```bash
cd internal/api/graphql
go run github.com/99designs/gqlgen generate
```

**Mocks**

When interfaces with a `//go:generate mockgen` directive are added, changed, or removed, regenerate mocks:

```bash
go generate ./...
```

Do not edit generated mocks by hand; always use `go generate`.

### Validation

After substantive code changes, run:

```bash
make post-implementation-check
```

This is the canonical local verification entrypoint. It runs strict whole-file linting for Go files changed versus `main`, then runs the CI-aligned Go test package set with coverage output filtered the same way as CI.

The strict lint profile enforces:

- cyclomatic and cognitive complexity limits
- function and file length limits
- function and package doc comments

Use `make check` when you intentionally want a broader maintenance pass across imports, local linting, and the repo-wide test command.

### Database schema changes

1. Add a migration under `db/migrations/` using sequential names (`001.sql`, `002.sql`, …). Use a suffix for special cases (e.g. `011_backfill.sql`).
2. Mirror the same shape in `db/init_pg_db.sql` so fresh installs stay consistent.
3. Update `pg_test_data` when tests need new or changed data.

### Documentation

- **Schema or architecture changes:** update `docs/` as appropriate.
- **Major product or setup changes:** update `README.md` or `DEVELOPMENT.md` when features, architecture, setup, deployment, dependencies, or requirements change materially.

## Pull requests and CI

- Prefer waiting for CI status checks before considering a PR done.
- Use conventional commits for local commits unless the task explicitly requires another format.
- If local `make post-implementation-check` (or full CI) cannot be run, state that explicitly in the PR description so reviewers know what was validated.
- When creating a GitHub issue, use the repository issue templates in `.github/ISSUE_TEMPLATE/` and complete every requested section.
- When creating a PR, use `.github/PULL_REQUEST_TEMPLATE.md` and keep the description aligned with the template fields.
- Do not replace the template structure with ad hoc prose; add extra context only after the required sections are complete.
- Before requesting review and after addressing review feedback, review the full diff, rerun `make post-implementation-check`, and summarize any remaining unverified risk.
