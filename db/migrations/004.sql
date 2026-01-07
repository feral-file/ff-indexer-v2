-- Migration 004: Add webhook notification tables
-- This migration adds support for webhook notifications to external clients

-- Webhook clients table
-- Stores registered webhook clients with their configuration
CREATE TABLE webhook_clients (
    id BIGSERIAL PRIMARY KEY,
    client_id VARCHAR(36) NOT NULL UNIQUE,    -- UUID for client identification
    webhook_url TEXT NOT NULL,                -- Client's webhook endpoint (HTTPS only)
    webhook_secret TEXT NOT NULL,             -- HMAC signing secret
    event_filters JSONB NOT NULL,             -- Array of event types or wildcard ["*"]
    is_active BOOLEAN NOT NULL DEFAULT true,  -- Whether client is active
    retry_max_attempts INTEGER NOT NULL DEFAULT 5, -- Maximum retry attempts
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    -- Enforce HTTP/HTTPS URLs only
    CONSTRAINT webhook_url_http_https CHECK (webhook_url LIKE 'http://%' OR webhook_url LIKE 'https://%')
);

-- Index for querying active clients
CREATE INDEX idx_webhook_clients_active ON webhook_clients(is_active) WHERE is_active = true;

-- Create enum type for webhook delivery status
CREATE TYPE webhook_delivery_status AS ENUM ('pending', 'success', 'failed');

-- Webhook deliveries table
-- Tracks all webhook delivery attempts for audit and debugging
CREATE TABLE webhook_deliveries (
    id BIGSERIAL PRIMARY KEY,
    client_id VARCHAR(36) NOT NULL REFERENCES webhook_clients(client_id) ON DELETE CASCADE,
    event_id VARCHAR(255) NOT NULL,           -- Unique event ID (ULID for time-sortable)
    event_type VARCHAR(50) NOT NULL,          -- e.g., "token.indexing.queryable", "token.indexing.viewable"
    payload JSONB NOT NULL,                   -- Full event payload
    workflow_id VARCHAR(255) NOT NULL,        -- Temporal workflow ID for tracking
    workflow_run_id VARCHAR(255),             -- Temporal run ID
    delivery_status webhook_delivery_status NOT NULL DEFAULT 'pending', -- pending, success, failed
    attempts INTEGER NOT NULL DEFAULT 0,      -- Number of delivery attempts
    last_attempt_at TIMESTAMPTZ,              -- Timestamp of last attempt
    response_status INTEGER,                  -- HTTP status code from webhook endpoint
    response_body TEXT,                       -- Response body (limited to 4KB)
    error_message TEXT,                       -- Error message if delivery failed
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Index for querying deliveries by status
CREATE INDEX idx_webhook_deliveries_status ON webhook_deliveries(delivery_status);

-- Index for looking up deliveries by event ID (prevent replays)
CREATE INDEX idx_webhook_deliveries_event_id ON webhook_deliveries(event_id);

-- Index for client-specific delivery history
CREATE INDEX idx_webhook_deliveries_client ON webhook_deliveries(client_id, created_at DESC);

-- Comments for documentation
COMMENT ON TABLE webhook_clients IS 'Registered webhook clients that receive event notifications';
COMMENT ON TABLE webhook_deliveries IS 'Audit log of all webhook delivery attempts';

COMMENT ON COLUMN webhook_clients.event_filters IS 'JSONB array of event types to receive, e.g., ["token.indexing.queryable", "token.indexing.viewable"] or ["*"] for all';
COMMENT ON COLUMN webhook_clients.webhook_secret IS 'Secret key for HMAC-SHA256 signature verification';
COMMENT ON COLUMN webhook_deliveries.event_id IS 'ULID-based unique event identifier for deduplication';
COMMENT ON COLUMN webhook_deliveries.payload IS 'Complete webhook event payload as JSON';

COMMENT ON TYPE webhook_delivery_status IS 'Status of a webhook delivery: pending, success, failed';