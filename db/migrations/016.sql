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

-- Nullable FK first; we'll backfill existing rows with corresponding jobs entries
ALTER TABLE address_indexing_jobs
    ADD COLUMN job_id BIGINT REFERENCES jobs(id) ON DELETE SET NULL;

-- Partial unique index on workflow_id (from migration 006; dropped with Temporal columns)
DROP INDEX IF EXISTS idx_address_indexing_job_workflow_id;

-- Backfill: Create jobs table rows for all existing address_indexing_jobs
-- This preserves historical data and allows resumption of paused/running jobs
DO $$
DECLARE
    aij_id BIGINT;
    aij_address TEXT;
    aij_chain blockchain_chain;
    aij_status indexing_job_status;
    aij_started_at TIMESTAMPTZ;
    aij_paused_at TIMESTAMPTZ;
    aij_completed_at TIMESTAMPTZ;
    aij_failed_at TIMESTAMPTZ;
    aij_canceled_at TIMESTAMPTZ;
    aij_created_at TIMESTAMPTZ;
    aij_updated_at TIMESTAMPTZ;
    new_job_id BIGINT;
    target_status job_status;
    target_run_after TIMESTAMPTZ;
    target_finished_at TIMESTAMPTZ;
    unique_key_value TEXT;
    job_exists BOOLEAN;
BEGIN
    FOR aij_id, aij_address, aij_chain, aij_status, aij_started_at, aij_paused_at, 
        aij_completed_at, aij_failed_at, aij_canceled_at, aij_created_at, aij_updated_at IN
        SELECT id, address, chain, status, started_at, paused_at, completed_at, 
               failed_at, canceled_at, created_at, updated_at
        FROM address_indexing_jobs 
        WHERE job_id IS NULL
        ORDER BY id  -- Process in order for consistency
    LOOP
        -- Map address_indexing_jobs.status to jobs.status
        target_status := CASE 
            WHEN aij_status = 'running' THEN 'running'::job_status
            WHEN aij_status = 'paused' THEN 'pending'::job_status
            WHEN aij_status = 'completed' THEN 'succeeded'::job_status
            WHEN aij_status = 'failed' THEN 'failed'::job_status
            WHEN aij_status = 'canceled' THEN 'canceled'::job_status
        END;
        
        -- Build unique key: 'index-token-owner-{chain}-{address}'
        unique_key_value := 'index-token-owner-' || aij_chain || '-' || aij_address;
        
        -- Check if an active job already exists (would violate unique constraint)
        SELECT EXISTS(
            SELECT 1 FROM jobs 
            WHERE queue = 'token_index' 
              AND kind = 'IndexTokenOwner'
              AND unique_key = unique_key_value
              AND status IN ('pending', 'running')
        ) INTO job_exists;
        
        -- Skip if active job exists to avoid unique constraint violation
        IF job_exists AND target_status IN ('pending', 'running') THEN
            RAISE NOTICE 'Skipping duplicate active job for address=% chain=%', aij_address, aij_chain;
            CONTINUE;
        END IF;
        
        -- For paused jobs, try to get quota_reset_at from watched_addresses, fallback to now()
        IF aij_status = 'paused' THEN
            SELECT COALESCE(wa.quota_reset_at, now())
            INTO target_run_after
            FROM watched_addresses wa
            WHERE wa.address = aij_address AND wa.chain = aij_chain::text;
            
            IF target_run_after IS NULL THEN
                target_run_after := now();
            END IF;
        ELSE
            target_run_after := aij_created_at;
        END IF;
        
        -- Calculate finished_at for terminal states
        target_finished_at := COALESCE(aij_completed_at, aij_failed_at, aij_canceled_at);
        
        -- Insert into jobs table
        INSERT INTO jobs (
            queue, 
            kind, 
            payload, 
            status, 
            unique_key, 
            run_after, 
            created_at, 
            updated_at, 
            started_at, 
            finished_at
        )
        VALUES (
            'token_index',
            'IndexTokenOwner',
            jsonb_build_array(to_jsonb(aij_address)),
            target_status,
            unique_key_value,
            target_run_after,
            aij_created_at,
            aij_updated_at,
            COALESCE(aij_started_at, aij_created_at),
            target_finished_at
        )
        RETURNING id INTO new_job_id;
        
        -- Update address_indexing_jobs with new job_id
        UPDATE address_indexing_jobs 
        SET job_id = new_job_id 
        WHERE id = aij_id;
        
        RAISE NOTICE 'Migrated address_indexing_job id=% to job id=%', aij_id, new_job_id;
    END LOOP;
END $$;

-- Drop legacy Temporal columns
ALTER TABLE address_indexing_jobs DROP COLUMN IF EXISTS workflow_id;
ALTER TABLE address_indexing_jobs DROP COLUMN IF EXISTS workflow_run_id;

-- Change FK constraint to CASCADE delete and enforce NOT NULL
ALTER TABLE address_indexing_jobs DROP CONSTRAINT IF EXISTS address_indexing_jobs_job_id_fkey;
ALTER TABLE address_indexing_jobs
    ADD CONSTRAINT address_indexing_jobs_job_id_fkey
    FOREIGN KEY (job_id) REFERENCES jobs(id) ON DELETE CASCADE;
ALTER TABLE address_indexing_jobs ALTER COLUMN job_id SET NOT NULL;

COMMENT ON TABLE address_indexing_jobs IS 'Tracks address-level indexing job status; linked to the postgres job queue via job_id (jobs.id).';
COMMENT ON COLUMN address_indexing_jobs.job_id IS 'Postgres job queue id (jobs.id) for this address indexing work unit';
