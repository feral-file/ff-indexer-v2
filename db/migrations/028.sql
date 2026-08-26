-- Migration 028: a stall is no evidence — reset legacy stalled debounce counters.
--
-- Until ff-indexer-v2#142 a render timeout ("stalled") counted toward the L1 gate
-- exactly like a blank frame. Re-probing production's would-gate stalls on unloaded
-- hardware with the indexer's own renderer showed 29/40 rendered in production's exact
-- configuration, at counters of 9–13: the counter was measuring the prober's load, not
-- the page. From #142 a stall carries the counter forward unchanged and never gates,
-- and any non-zero counter makes the next probe a confirmation whose blank gates.
-- Counters accumulated under the old contract are therefore not blank evidence and must
-- not be inherited as a first strike, nor keep those rows in the urgent re-probe tier.
--
-- Scope: ungated rows only. A gated row's counter is irrelevant to healing (a
-- successful render releases regardless) and shadow mode holds no gates, so nothing is
-- lost by leaving gated rows alone; touching them would change nothing.
--
-- LOCKING: a single UPDATE over the affected rows (thousands, not millions); row locks
-- only, no DDL on a populated table beyond a comment. Safe under traffic.

BEGIN;

UPDATE media_render_probes
SET consecutive_failures = 0
WHERE verdict = 'stalled'
  AND health_gated = false
  AND consecutive_failures > 0;

COMMENT ON COLUMN media_render_probes.consecutive_failures IS
    'Consecutive blank frames (debounce state; gates at render_probe.failure_gate_threshold). '
    'A stall (load failure/timeout) carries it forward unchanged and never gates — it is '
    'evidence about the prober''s budget and load, not the page. known_bad_fingerprint '
    'gates immediately without it. Legacy stalled counters were reset by migration 028.';

COMMIT;
