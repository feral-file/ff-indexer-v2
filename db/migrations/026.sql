-- Migration 026: enforce one address_indexing_jobs row per queue job
-- (unique index on job_id).
--
-- CreateAddressIndexingJob's application-level idempotency check
-- (check-then-insert) races against the worker lifecycle: a trigger can observe
-- no tracking row for a deduplicated queue job, the worker can then create and
-- terminalize one, and the trigger's later insert still succeeds because the
-- schema's partial unique indexes only protect ACTIVE rows. The result is a new
-- active row for an already-terminal queue job — its terminal transition has
-- already run, so nothing ever finishes the duplicate and the address reads as
-- permanently busy. A database constraint closes the race window that
-- application checks cannot: the insert becomes ON CONFLICT DO NOTHING against
-- this index.
--
-- Reconciliation before the index: historical duplicates per job_id are
-- expected — before the application-level job_id check existed, every queue-job
-- retry created a fresh tracking row for the same job_id (the prior attempt's
-- row having gone terminal). Keep the newest row per job_id (max id: ids are
-- monotonic with insertion, and the newest row reflects the job's final
-- lifecycle state); delete the superseded attempt rows. One queue job
-- contributes one outcome to the address's history — per-attempt duplicates
-- would otherwise inflate the trigger throttle's failure streak.
--
-- ⚠️ Deployment ordering: run BEFORE deploying application code that relies on
-- ON CONFLICT against this index (the standard migrations-before-deploy rule).
--
-- The table is locked against writes for the (short) duration of the
-- transaction. Without the lock this migration races live traffic: the DELETE
-- dedups only the rows in its own snapshot, while CREATE UNIQUE INDEX builds
-- over every committed row — a worker inserting one more per-retry duplicate
-- between the two statements fails the index build (observed in production:
-- "Key (job_id)=... is duplicated" after DELETE 10696). EXCLUSIVE mode blocks
-- writers but not readers; blocked workers simply wait out the migration.
-- Safe to re-run after a failed attempt: the failed transaction rolled back.

BEGIN;

LOCK TABLE address_indexing_jobs IN EXCLUSIVE MODE;

DELETE FROM address_indexing_jobs a
USING address_indexing_jobs b
WHERE a.job_id = b.job_id
  AND a.id < b.id;

CREATE UNIQUE INDEX idx_address_indexing_jobs_job_id ON address_indexing_jobs (job_id);

COMMIT;
