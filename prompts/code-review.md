### Review priority
1. Correctness and reliability of the submitted change
2. Compliance with `AGENTS.md` and the repo engineering principles
3. Simplicity relative to the current scale and operating needs
4. Test sufficiency, test placement, and documentation sufficiency

### Required review posture
- Do not review only for local diff correctness. Review whether the implementation is the right solution for the stated goal.
- Prefer clear, testable, maintainable designs over narrow patches that preserve flawed behavior.
- Do not invent repo-wide architecture or API requirements beyond what is already documented.
- Call out over-engineering, weak error handling, silent failure modes, and missing operational clarity.
- Do not speculate. Mention an alternative only when it is clearly better and actionable.

### Architecture and API note
- `docs/business_requirements.md`, `docs/constraints.md`, `docs/api_design.md`, `docs/architecture.md`, and `docs/schema.md` are useful context, but some of these docs may still be placeholders owned by the repo owner.
- If the change should update those docs or should force the repo owner to define missing guidance, call that out as a documentation or design gap.

### Tests and docs sufficiency review
For substantial changes, assess only real gaps:
1. Are there enough unit tests for new or changed logic?
2. Are there enough integration tests for DB, GraphQL, queue, or service boundary changes?
3. Do tests verify intended behavior rather than implementation details?
4. Does the change require updates to `docs/`, `README.md`, or `DEVELOPMENT.md`?
5. If documentation is missing, specify exactly what should be documented.

If there is no meaningful test or documentation gap, omit that section.

### Preferred output shape
Use only sections that have meaningful content:
1. Critical correctness issues
2. Reliability / maintainability issues
3. Better alternative designs
4. Test gaps
5. Documentation gaps

If there are no meaningful findings, provide a brief approval-style summary only.

### Verdict
End with exactly one line:

- `Verdict: accept`
- `Verdict: revise`

Use `accept` only when there are no blocking correctness, reliability, test, or documentation issues.
