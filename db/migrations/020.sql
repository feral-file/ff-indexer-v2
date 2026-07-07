-- Migration 020: Add attempt_count to jobs for crash-loop protection.
--
-- Without this column, SweepOrphanedJobs unconditionally resets every running row back to
-- pending on worker startup. A job that crashes the process via SIGABRT (e.g. an SVG whose
-- resvg filter triggers a Rust assertion) would loop forever: run → SIGABRT → orphan →
-- sweep → pending → run → SIGABRT ...
--
-- With attempt_count, ClaimJobs increments the counter on every claim. SweepOrphanedJobs
-- can compare against a configured MaxAttempts threshold and transition exhausted jobs to
-- failed instead of resetting them to pending, breaking the loop permanently.
--
-- Default 0: existing rows start with no recorded attempts (treated as fresh by workers).

BEGIN;

ALTER TABLE jobs
    ADD COLUMN attempt_count INTEGER NOT NULL DEFAULT 0;

COMMENT ON COLUMN jobs.attempt_count IS
    'Number of times this job has been claimed (incremented by ClaimJobs). '
    'SweepOrphanedJobs uses this to mark jobs failed after MaxAttempts crashes '
    'instead of resetting them to pending indefinitely.';

COMMIT;
