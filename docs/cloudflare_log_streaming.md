# Cloudflare application-log streaming

## Goal

Replace Sentry with the shared Cloudflare Pipeline Stream while retaining every
application log on standard output. The indexer must not write log files.

The wire contract comes from the sibling `ff-logging` repository:
`docs/log-ingestion.md` and `infra/log-schema.json`. Trusted services send NDJSON
directly to the authenticated Stream endpoint. Each line has the immutable
top-level schema `timestamp`, `level`, `service`, `environment`, and `message`.

## Design

Zap remains the application logging API and writes its normal development or
production encoding to stdout. When a Cloudflare API token is configured, a
second Zap core serializes each entry to the Stream schema and queues it for a
bounded in-memory batch sender.

Zap fields cannot become extra top-level fields because the Stream schema is
immutable. The remote `message` is therefore a JSON string containing the
original message plus its structured fields, caller, and stack trace. This
keeps fields such as `component`, `job_id`, and errors queryable without
changing the Cloudflare schema. Log contents are not redacted or censored by
the logging module.

The sender posts `application/x-ndjson` with `Authorization: Bearer <token>`.
It batches for at most one second and keeps each request below Cloudflare's
5 MiB request limit. The queue is memory-only and bounded to 64 MiB of record
data as well as 4,096 records, so a Cloudflare outage cannot consume unbounded
process memory or block indexer work. A full queue, oversized record, transport
failure, or non-2xx response is reported to
stderr. Redirects are not followed, which keeps the bearer token pinned to the
validated Stream endpoint. The corresponding stdout log is unaffected.
Shutdown waits up to the caller's flush timeout for records already accepted
into the queue.

This is best-effort remote delivery: without file storage, a process crash or
an exhausted in-memory queue can lose the remote copy. stdout remains the
primary complete process log.

## Configuration

- `logging.cloudflare_stream_url`: defaults to the shared
  `application_logs_stream` HTTP endpoint.
- `logging.cloudflare_api_token`: a producer-specific account token with the
  `Pipelines Send` permission. An empty token disables remote streaming.
- `logging.environment`: required when the token is set, for example
  `production` or `staging`.

The remote `service` value is the stable identifier `ff-indexer`.

## Verification

- Console configuration contains only `stdout` as its normal output path.
- Remote requests use Bearer authentication, NDJSON, and only the immutable
  Stream fields.
- Structured Zap fields survive inside the remote message string.
- Multiple records batch into one request and an explicit flush drains queued
  records.
- Invalid or partial Cloudflare logging configuration fails during config load.
