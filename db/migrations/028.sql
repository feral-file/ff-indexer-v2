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
-- Scope: ungated rows only. A gated row's counter is irrelevant to healing (a
-- successful render releases regardless) and shadow mode holds no gates, so touching
-- them would change nothing.
--
-- ORDERING — unlike the default "migrations before code": run this AFTER the #142 image
-- is live (or with the media worker stopped). It is pure data; no code depends on it.
-- Run before the new image, a still-running pre-#142 worker keeps writing stall counts
-- into the window between the reset and the restart, and the new executor cannot tell
-- those from blank debounce state. Run after, nothing writes stall counts any more, so
-- the reset is final. Re-runnable: a second application is a no-op.
--
-- LOCKING: a single UPDATE over the affected rows (thousands, not millions); row locks
-- only, no DDL on a populated table beyond a comment. Safe under traffic.

BEGIN;

UPDATE media_render_probes
SET consecutive_failures = 0
WHERE health_gated = false
  AND consecutive_failures > 0;

COMMENT ON COLUMN media_render_probes.consecutive_failures IS
    'Consecutive blank frames (debounce state; gates at render_probe.failure_gate_threshold). '
    'A stall (load failure/timeout) carries it forward unchanged and never gates — it is '
    'evidence about the prober''s budget and load, not the page. known_bad_fingerprint '
    'gates immediately without it. Legacy counters were reset by migration 028.';

COMMIT;
