-- Migration 030: drop 35 unused indexes (~8.5 GB).
--
-- Each index below was confirmed unused on TWO independent axes:
--   1. pg_stat_user_indexes.idx_scan = 0 over the database's entire life
--      (pg_stat_database.stats_reset IS NULL — never reset), and
--   2. a source-code audit found no query in internal/{store,api,workflows,
--      media,metadata} that filters, orders, joins, or does jsonb containment
--      on the indexed column(s). Live provenance/token/media reads key off
--      other indexes (token_id and its composites, primary keys, the
--      source_url_hash/provider and 6-column provenance_events_unique
--      constraints), none of which are touched here.
--
-- Notable specifics from the audit:
--   - idx_provenance_events_raw and idx_provenance_events_raw_gin are IDENTICAL
--     duplicate GIN(raw) indexes; nothing does jsonb containment on `raw`
--     (the only raw access is the scalar ORDER BY (raw->>'tx_index')::bigint,
--     which a GIN cannot serve).
--   - The provenance_events from_address/to_address indexes are referenced only
--     by one-time historical backfills (001.sql, 011_backfill.sql), not by any
--     live code path; re-running those backfills would seq-scan (a one-time cost).
--   - idx_enrichment_sources_image_url exists only in prod (not in
--     init_pg_db.sql or any migration); IF EXISTS covers it.
--   - Kept (idx_scan=0 but a real query path exists): idx_tokens_viewable,
--     idx_media_render_probes_due, idx_webhook_clients_active, the four
--     *_url_hash indexes (URL-rewrite UPDATEs), and
--     release_members_release_id_mint_number_idx.
--
-- The matching CREATE INDEX statements were removed from db/init_pg_db.sql in
-- the same change, so fresh databases never create them.
--
-- HOW TO RUN — must be OUTSIDE an explicit transaction (autocommit):
--   psql "$DSN" -f db/migrations/030_drop_unused_indexes.sql
-- DROP INDEX CONCURRENTLY cannot run inside a BEGIN/COMMIT block; a migration
-- runner that auto-wraps files in a transaction must disable wrapping for this
-- file (same rule as 017_dedup.sql). CONCURRENTLY takes no ACCESS EXCLUSIVE
-- lock, so reads/writes are never blocked. IF EXISTS makes it re-runnable, and
-- every drop is reversible by recreating the index if a need appears.

DROP INDEX CONCURRENTLY IF EXISTS public.idx_provenance_events_raw;
DROP INDEX CONCURRENTLY IF EXISTS public.idx_provenance_events_raw_gin;
DROP INDEX CONCURRENTLY IF EXISTS public.idx_media_assets_variant_urls_gin;
DROP INDEX CONCURRENTLY IF EXISTS public.idx_provenance_events_token_id_from_address_timestamp;
DROP INDEX CONCURRENTLY IF EXISTS public.idx_provenance_events_tx_hash;
DROP INDEX CONCURRENTLY IF EXISTS public.idx_provenance_events_id_text;
DROP INDEX CONCURRENTLY IF EXISTS public.idx_token_metadata_latest_json_gin;
DROP INDEX CONCURRENTLY IF EXISTS public.idx_provenance_events_to_address;
DROP INDEX CONCURRENTLY IF EXISTS public.idx_token_metadata_origin_json_gin;
DROP INDEX CONCURRENTLY IF EXISTS public.idx_provenance_events_block_number;
DROP INDEX CONCURRENTLY IF EXISTS public.idx_provenance_events_timestamp;
DROP INDEX CONCURRENTLY IF EXISTS public.idx_enrichment_sources_vendor_json_gin;
DROP INDEX CONCURRENTLY IF EXISTS public.idx_provenance_events_from_address;
DROP INDEX CONCURRENTLY IF EXISTS public.idx_provenance_events_chain;
DROP INDEX CONCURRENTLY IF EXISTS public.idx_provenance_events_event_type;
DROP INDEX CONCURRENTLY IF EXISTS public.idx_media_assets_provider_metadata_gin;
DROP INDEX CONCURRENTLY IF EXISTS public.idx_balances_updated_at;
DROP INDEX CONCURRENTLY IF EXISTS public.idx_enrichment_sources_image_url;
DROP INDEX CONCURRENTLY IF EXISTS public.idx_media_assets_provider_asset_id;
DROP INDEX CONCURRENTLY IF EXISTS public.idx_enrichment_sources_vendor_hash;
DROP INDEX CONCURRENTLY IF EXISTS public.idx_token_metadata_last_refreshed_at;
DROP INDEX CONCURRENTLY IF EXISTS public.idx_webhook_deliveries_client;
DROP INDEX CONCURRENTLY IF EXISTS public.idx_media_assets_created_at;
DROP INDEX CONCURRENTLY IF EXISTS public.idx_webhook_deliveries_event_id;
DROP INDEX CONCURRENTLY IF EXISTS public.idx_token_metadata_artists;
DROP INDEX CONCURRENTLY IF EXISTS public.idx_enrichment_sources_artists;
DROP INDEX CONCURRENTLY IF EXISTS public.idx_key_value_store_updated_at;
DROP INDEX CONCURRENTLY IF EXISTS public.idx_media_assets_provider;
DROP INDEX CONCURRENTLY IF EXISTS public.idx_enrichment_sources_vendor;
DROP INDEX CONCURRENTLY IF EXISTS public.idx_token_metadata_enrichment_level;
DROP INDEX CONCURRENTLY IF EXISTS public.idx_token_metadata_publisher;
DROP INDEX CONCURRENTLY IF EXISTS public.idx_webhook_deliveries_status;
DROP INDEX CONCURRENTLY IF EXISTS public.idx_tokens_burned;
DROP INDEX CONCURRENTLY IF EXISTS public.idx_watched_addresses_last_queried_at;
DROP INDEX CONCURRENTLY IF EXISTS public.idx_watched_addresses_chain;
