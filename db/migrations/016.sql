-- Postgres job queue: jobs table and address_indexing_jobs.job_id (Temporal workflow_id retained until 017)
CREATE TYPE job_status AS ENUM ('pending', 'running', 'succeeded', 'failed', 'canceled');

CREATE TABLE jobs (
    id BIGSERIAL PRIMARY KEY,
    queue TEXT NOT NULL,
    kind TEXT NOT NULL,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    status job_status NOT NULL DEFAULT 'pending',
    unique_key TEXT,
    run_after TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_error TEXT,
    cancel_requested BOOLEAN NOT NULL DEFAULT false,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ
);

CREATE UNIQUE INDEX jobs_unique_key_active ON jobs (queue, kind, unique_key) WHERE status IN ('pending', 'running') AND unique_key IS NOT NULL;
CREATE INDEX jobs_poll ON jobs (queue, run_after) WHERE status = 'pending';

ALTER TABLE address_indexing_jobs
    ADD COLUMN job_id BIGINT REFERENCES jobs(id) ON DELETE SET NULL;

CREATE TRIGGER update_jobs_updated_at
    BEFORE UPDATE ON jobs
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

COMMENT ON TYPE job_status IS 'Status of a row in the postgres-backed job queue';
COMMENT ON TABLE jobs IS 'Durable work queue: one row per unit of work (replaces Temporal workflow/activity for orchestration)';
COMMENT ON COLUMN jobs.queue IS 'Logical queue name (e.g. token_index, media_index) consumed by a worker pool';
COMMENT ON COLUMN jobs.kind IS 'Handler name registered with the job registry (workflow name successor)';
COMMENT ON COLUMN jobs.payload IS 'JSON arguments for the handler (wire format: array of raw JSON values)';
COMMENT ON COLUMN jobs.unique_key IS 'When set, enforces at most one active (pending or running) job per (queue, kind, unique_key)';
COMMENT ON COLUMN jobs.run_after IS 'Do not run this job before this time (used for scheduling and manual reschedule)';
COMMENT ON COLUMN jobs.last_error IS 'Terminal or latest failure message when status is failed';
COMMENT ON COLUMN jobs.cancel_requested IS 'When true, worker should cancel the in-flight handler context';
COMMENT ON COLUMN address_indexing_jobs.job_id IS 'Postgres job queue id when the address job is driven by the jobs table; null for legacy Temporal-only rows';
