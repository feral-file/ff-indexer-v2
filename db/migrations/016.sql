-- Migration 016: Postgres job queue (jobs) and address_indexing_jobs.job_id
-- Creates `jobs`, adds `address_indexing_jobs.job_id`, removes legacy Temporal `workflow_id` / `workflow_run_id`,
-- and enforces `job_id NOT NULL` with `FOREIGN KEY ... ON DELETE CASCADE` to `jobs(id)`.

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

-- Nullable FK first; rows without a queue job are removed, then we enforce NOT NULL + CASCADE.
ALTER TABLE address_indexing_jobs
    ADD COLUMN job_id BIGINT REFERENCES jobs(id) ON DELETE SET NULL;

-- Partial unique index on workflow_id (from migration 006; dropped with Temporal columns)
DROP INDEX IF EXISTS idx_address_indexing_job_workflow_id;

-- Legacy rows that never got a jobs.id cannot be used with the new API
DELETE FROM address_indexing_jobs WHERE job_id IS NULL;

ALTER TABLE address_indexing_jobs DROP COLUMN IF EXISTS workflow_id;
ALTER TABLE address_indexing_jobs DROP COLUMN IF EXISTS workflow_run_id;

ALTER TABLE address_indexing_jobs DROP CONSTRAINT IF EXISTS address_indexing_jobs_job_id_fkey;
ALTER TABLE address_indexing_jobs
    ADD CONSTRAINT address_indexing_jobs_job_id_fkey
    FOREIGN KEY (job_id) REFERENCES jobs(id) ON DELETE CASCADE;
ALTER TABLE address_indexing_jobs ALTER COLUMN job_id SET NOT NULL;

COMMENT ON TABLE address_indexing_jobs IS 'Tracks address-level indexing job status; linked to the postgres job queue via job_id (jobs.id).';
COMMENT ON COLUMN address_indexing_jobs.job_id IS 'Postgres job queue id (jobs.id) for this address indexing work unit';
