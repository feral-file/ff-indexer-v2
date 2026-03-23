# Agent guidance

This document is for AI assistants and automation working on this repository. Apply it when reviewing, designing, or changing code.

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
make check
```

This formats code, runs linting, and runs tests (including coverage). Local runs need tools such as `golangci-lint`.

### Database schema changes

1. Add a migration under `db/migrations/` using sequential names (`001.sql`, `002.sql`, …). Use a suffix for special cases (e.g. `011_backfill.sql`).
2. Mirror the same shape in `db/init_pg_db.sql` so fresh installs stay consistent.
3. Update `pg_test_data` when tests need new or changed data.

### Documentation

- **Schema or architecture changes:** update `docs/` as appropriate.
- **Major product or setup changes:** update `README.md` or `DEVELOPMENT.md` when features, architecture, setup, deployment, dependencies, or requirements change materially.

## Pull requests and CI

- Prefer waiting for CI status checks before considering a PR done.
- If local `make check` (or full CI) cannot be run, state that explicitly in the PR description so reviewers know what was validated.
