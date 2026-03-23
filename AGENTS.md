# AGENTS.md - FF Indexer v2 Repository Contract

This file is the top-level working contract for humans and coding agents in this repository.

Detailed tool-specific behavior lives in `.cursor/`, `.codex/`, `opencode.json`, and `prompts/`.

## Repository purpose

- Project: `ff-indexer-v2`
- Purpose: index blockchain activity and media/metadata state for Feral File services, then expose reliable data to downstream systems and APIs
- Current scale assumption: around 3000 users

## Repo overlays

This repository follows the portable org-wide agentic engineering standard and adds repo-local overlays here:

- service-specific engineering principles
- Go and database workflow rules
- CI and review expectations

## Engineering principles

Use these principles when reviewing, designing, or changing code.

### How to behave

- Prefer the principles below as the default decision framework.
- Explain how proposed changes improve or protect adherence to these principles.
- If something violates a principle, say so clearly and explain why.
- If a request is vague and principle compliance is unclear, clarify before making a risky assumption.
- Do not invent requirements or user needs. Question assumptions about scale, complexity, and necessity.

### Simplicity by default

- Default to the simplest design that meets correctness and reliability for current production needs.
- Treat added complexity as a last resort, justified only when simpler designs fail in practice and the trade-offs are understood.

### Reliable and transparent behavior

- Reliability comes first: the system may be slow or limited, but it should behave predictably.
- Do not hide system state. If something is slow, missing, degraded, or unavailable, make that explicit.
- Errors should be clear, actionable, and easy to debug.
- Avoid failing silently or leaving operators without a clear next step.

### Community contribution

- Keep the project easy to run locally.
- Keep setup, dependencies, and mental overhead low.

### Common pitfalls

#### Over-engineering

- Complexity without a clear current need
- Designing for far larger scale than the system currently needs
- Features or abstractions no one has asked for
- Heavy infrastructure where simpler options suffice

#### Reliability and quality pitfalls

- Weak or missing error handling
- Errors that are not actionable or informative
- Too little logging or observability for debugging user issues
- Logs that are noisy, oversized, or costly

## Architecture and API guidance

These sections are intentionally left for the repo owner to complete.

### Business requirements

See `docs/business_requirements.md`.

Status: `TBD by repo owner`

### Product and operational constraints

See `docs/constraints.md`.

Status: `TBD by repo owner`

### Architecture boundaries

`TBD by repo owner`

### API design rules

See `docs/api_design.md`.

Status: `TBD by repo owner`

## Operating model

Default sequence:

`spec -> design -> tasks -> tests -> implementation -> verification -> review -> merge`

TDD is the default behavior-change workflow in this repository.

For small, low-risk changes, the flow can be compressed, but work must still stay scoped, verified, reviewed, and traceable.

## Required workflow for substantial changes

Before implementing any major feature, behavior change, refactor, schema change, or architectural update:

1. Read the relevant docs, code, and operational context, including business requirements and constraints when available.
2. Summarize the current behavior, constraints, and invariants.
3. Write or update a short spec, design note, or implementation plan.
4. Break the work into concrete tasks.
5. Define expected behavior and how it will be verified.
6. Add or update tests before implementation when behavior is changing.
7. Only then begin implementation.

If no relevant spec or design note exists for a substantial change, do not jump directly into implementation.

## TDD expectations

TDD is the default implementation discipline:

- write or update tests before implementing behavior changes
- let tests define expected behavior and guard against regressions
- implement until tests pass
- refactor while keeping tests green

Testing should match the shape of the change:

- unit tests for business or domain logic
- integration tests for database, GraphQL, queue, or service boundaries
- end-to-end or system tests for critical flows when appropriate

If a change does not reasonably fit strict test-first sequencing, note the deviation explicitly in the handoff or PR description.

## Development workflow

### Code generation

#### GraphQL schema

After GraphQL schema changes:

```bash
cd internal/api/graphql
go run github.com/99designs/gqlgen generate
```

#### Mocks

When interfaces with a `//go:generate mockgen` directive are added, changed, or removed:

```bash
go generate ./...
```

Do not edit generated mocks by hand.

### Database schema changes

1. Add a migration under `db/migrations/` using sequential names such as `001.sql`, `002.sql`, or `011_backfill.sql`.
2. Mirror the same shape in `db/init_pg_db.sql` so fresh installs remain correct.
3. Update `pg_test_data` when tests need new or changed data.

### Documentation

- Update `docs/` when schema, architecture, or operational behavior changes.
- Update `README.md` or `DEVELOPMENT.md` when setup, workflows, dependencies, or major behavior changes materially.

## Verification expectations

Required local verification path after substantive changes:

```bash
make check
```

This is the primary repo verification command and should remain usable by both humans and agents.

When relevant, also run targeted generators or workflow-specific checks such as:

- GraphQL generation
- mock generation
- migration or schema validation
- build validation for changed services

If you cannot run the full required verification, say so explicitly and state what was run instead.

## Definition of done

A task is complete only when:

1. The requested behavior or change is implemented.
2. Relevant tests have been added or updated.
3. Verification passes cleanly, or any gap is explicitly called out.
4. Documentation is updated when behavior, architecture, or operations changed.
5. Review has qualified the change.
6. The change is ready to merge without relying on unstated follow-up work.

## Review loop

After implementation, run a review loop before merge, commit, or PR finalization.

1. Create a compact handoff:
   - goal
   - scope
   - files or modules changed
   - key decisions and trade-offs
   - tests added or updated
   - checks run
   - known limitations, if any
2. Invoke a fresh-context reviewer sub-agent.
3. If review requests revisions, address them, re-run verification, update the handoff, and review again.
4. Only proceed once the reviewer ends with `Verdict: accept`.

The authoritative review contract lives in `prompts/code-review.md`.

## Pull requests and CI

- Treat required CI checks as enforcement of the policy in this file.
- Prefer waiting for CI before considering a PR done.
- If local `make check` or other expected verification could not be run, state that explicitly in the PR description.
- Secret scanning, linting, tests, and workflow validation should remain enabled in CI where practical.

## Commit message format

Use Conventional Commits:

- `<type>(<optional-scope>): <description>`
- Types: `feat`, `fix`, `refactor`, `test`, `chore`, `docs`, `build`, `ci`, `perf`, `style`
- Use `!` for breaking changes

## Rule references

- `.cursor/rules/01-repo-contract.mdc`
- `.cursor/rules/05-engineering-principles.mdc`
- `.cursor/rules/10-architecture-tbd.mdc`
- `.cursor/rules/11-api-design-tbd.mdc`
- `.cursor/rules/12-business-requirements-tbd.mdc`
- `.cursor/rules/13-constraints-tbd.mdc`
- `.cursor/rules/20-spec-driven.mdc`
- `.cursor/rules/30-testing-tdd.mdc`
- `.cursor/rules/40-development-workflow.mdc`
- `.cursor/rules/90-review-workflow.mdc`
