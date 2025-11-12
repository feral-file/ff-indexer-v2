# Bug Report

## Description

<!-- Provide a clear and concise description of the bug -->

## Affected Component(s)

<!-- Mark all relevant components with an 'x' -->

- [ ] API Server (REST/GraphQL)
- [ ] Ethereum Event Emitter
- [ ] Tezos Event Emitter
- [ ] Event Bridge
- [ ] Worker Core
- [ ] Worker Media
- [ ] Database/Storage
- [ ] Infrastructure (PostgreSQL, NATS, Temporal)
- [ ] Other: <!-- Please specify -->

## Blockchain Network

<!-- Mark the affected network(s) with an 'x' -->

- [ ] Ethereum
- [ ] Tezos
- [ ] Both
- [ ] Not applicable

## Environment

<!-- Provide details about your environment -->

- **Version/Commit**: <!-- e.g., v2.1.0, commit hash, or branch name -->
- **Go Version**: <!-- e.g., go1.24 -->
- **PostgreSQL Version**: <!-- e.g., 15.3 -->
- **Deployment**: <!-- e.g., Docker Compose, Kubernetes, Local -->
- **Operating System**: <!-- e.g., macOS, Linux, Windows -->

## Steps to Reproduce

<!-- Provide detailed steps to reproduce the bug -->

1. 
2. 
3. 
4. 

## Expected Behavior

<!-- Describe what you expected to happen -->

## Actual Behavior

<!-- Describe what actually happened -->

## Error Messages / Logs

<!-- Paste relevant error messages, stack traces, or log excerpts -->

```
<!-- Paste logs here -->
```

## Configuration

<!-- If relevant, provide configuration details (sanitize sensitive information) -->

**Service Config** (if applicable):
```yaml
<!-- Paste relevant config section -->
```

**Environment Variables** (if applicable):
```bash
# Paste relevant env vars (sanitize sensitive values)
```

## Token/Contract Details

<!-- If the bug is related to a specific token or contract -->

- **Contract Address**: <!-- e.g., 0x... or tz... -->
- **Token ID**: <!-- e.g., 123 -->
- **Token CID**: <!-- e.g., eth:0x...:123 or tz:tz...:123 -->
- **Blockchain**: <!-- Ethereum or Tezos -->
- **Block Number/Level**: <!-- If relevant -->

## Workflow/Event Details

<!-- If the bug is related to a Temporal workflow or blockchain event -->

- **Workflow Type**: <!-- e.g., IndexTokenMint, IndexTokenMetadata -->
- **Event Type**: <!-- e.g., mint, transfer, burn, metadata_update -->
- **Workflow ID**: <!-- If available from Temporal UI -->
- **Event Subject**: <!-- e.g., events.ethereum.mint -->

## Additional Context

<!-- Add any other context, screenshots, or information that might be helpful -->

## Severity

<!-- Mark the severity level with an 'x' -->

- [ ] Critical (system down, data loss, security issue)
- [ ] High (major functionality broken)
- [ ] Medium (minor functionality broken, workaround available)
- [ ] Low (cosmetic issue, edge case)

## Possible Solution

<!-- If you have ideas on how to fix this, please share -->

