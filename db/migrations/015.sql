-- Remove changes_journal audit log (replaced by token_events for collection sync; API no longer exposes journal)
DROP TABLE IF EXISTS changes_journal CASCADE;
DROP TYPE IF EXISTS subject_type;

-- Ownership periods were only used to gate acquired/released token_events; token_events + balances are sufficient
DROP TABLE IF EXISTS token_ownership_periods CASCADE;
