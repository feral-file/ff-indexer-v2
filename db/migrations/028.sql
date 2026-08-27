-- Migration 028: a stall is no evidence — reset legacy debounce counters.
--
-- Until ff-indexer-v2#142 a render timeout ("stalled") counted toward the L1 gate
-- exactly like a blank frame. Re-probing production's would-gate stalls on unloaded
-- hardware with the indexer's own renderer showed 29/40 rendered in production's exact
-- configuration, at counters of 9–13: the counter was measuring the prober's load, not
-- the page. From #142 a stall carries the counter forward unchanged and never gates,
-- and the probe whose blank would reach failure_gate_threshold is a confirmation.
--
-- Counters accumulated under the old contract are therefore not blank evidence. That
-- includes rows whose FINAL verdict is 'blank': a stall followed by a blank left a
-- counter of 2 in which only one observation was a frame, so the predicate is every
-- non-zero counter, not the stalled verdict — a row's last label says nothing about
-- what its count is made of. Affected rows restart from a first look; the deciding look
-- then runs under the confirmation conditions (#142). One extra render per row.
--
-- Scope: every non-zero counter, gated rows included. A gated row's counter is
-- irrelevant while the gate is held (its next probe is a confirmation regardless, and
-- the gate persists until a successful render releases it), but release — including
-- the shadow-mode sweeper's release of every gate — keeps the counter, so a released
-- legacy gate would carry stall-inflated evidence into its next blank. Zeroing it
-- changes nothing while gated and makes the release a clean restart.
--
-- ROLLOUT — exact-once, with the indexer STOPPED (see DEVELOPMENT.md):
--   1. stop the indexer (every media/probe worker, old or new, including in-flight
--      RenderMediaProbe jobs — a probe that read a counter before this UPDATE writes it
--      back afterwards, whichever executor version it runs);
--   2. run this file once and confirm COMMIT;
--   3. start the #142 image.
-- It is pure data; no code depends on it, so it is not covered by the default
-- "migrations before code" rule. NOT re-runnable after cutover: once the new executor
-- is live, a non-zero counter is a genuine first blank awaiting its confirmation, and
-- running this again would erase that evidence.
--
-- LOCKING: a single UPDATE over the affected rows (thousands, not millions); row locks
-- only, no DDL on a populated table beyond a comment. The stop above is for
-- correctness, not for locking.

BEGIN;

UPDATE media_render_probes
SET consecutive_failures = 0
WHERE consecutive_failures > 0;

COMMENT ON COLUMN media_render_probes.consecutive_failures IS
    'Consecutive blank frames (debounce state; gates at render_probe.failure_gate_threshold). '
    'A stall (load failure/timeout) carries it forward unchanged and never gates — it is '
    'evidence about the prober''s budget and load, not the page. known_bad_fingerprint '
    'gates immediately without it. Legacy counters were reset by migration 028.';

COMMIT;
