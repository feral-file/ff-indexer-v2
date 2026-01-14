-- Migration 006: Add address indexing jobs table
-- Description: Tracks address-level indexing job status independent of Temporal workflows

-- Create job status enum type
CREATE TYPE indexing_job_status AS ENUM (
    'running',     -- Job running
    'paused',      -- Job paused
    'failed',      -- Job failed
    'completed',   -- Job completed successfully
    'canceled'     -- Job canceled
);

-- Address indexing jobs table
CREATE TABLE address_indexing_jobs (
    id BIGSERIAL PRIMARY KEY,
    address TEXT NOT NULL,
    chain blockchain_chain NOT NULL,
    status indexing_job_status NOT NULL,
    
    -- Temporal workflow references
    workflow_id TEXT NOT NULL,
    workflow_run_id TEXT,
    
    -- Progress tracking
    tokens_processed INTEGER DEFAULT 0,
    current_min_block BIGINT,
    current_max_block BIGINT,
    
    -- Timing information
    started_at TIMESTAMPTZ NOT NULL,
    paused_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    failed_at TIMESTAMPTZ,
    canceled_at TIMESTAMPTZ,
    
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Indexes for API queries
CREATE UNIQUE INDEX idx_address_indexing_jobs_workflow_id ON address_indexing_jobs(workflow_id) WHERE status = 'running';
CREATE INDEX idx_address_indexing_jobs_address_chain_created ON address_indexing_jobs(address, chain, created_at DESC);
CREATE INDEX idx_address_indexing_jobs_status_created ON address_indexing_jobs(status, created_at DESC);

-- Auto-update trigger
CREATE TRIGGER update_address_indexing_jobs_updated_at
    BEFORE UPDATE ON address_indexing_jobs
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Comment
COMMENT ON TABLE address_indexing_jobs IS 'Tracks address-level indexing job status independent of Temporal workflows. Decouples job status from Temporal for easier querying via REST/GraphQL APIs.';
COMMENT ON COLUMN address_indexing_jobs.workflow_id IS 'Orchestrator workflow ID for correlation and cancellation operations';
COMMENT ON COLUMN address_indexing_jobs.workflow_run_id IS 'Orchestrator workflow run ID, may be null initially due to async workflow start';
