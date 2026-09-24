//go:build integration

package store

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// explainWithoutSeqScan returns the EXPLAIN plan for stmt with sequential scans disabled
// for the statement's transaction. On the tiny test tables the planner would otherwise pick
// a seq scan even when an index applies; with enable_seqscan off, a plan that still shows
// "Seq Scan" proves that no usable index exists for the predicate.
func explainWithoutSeqScan(t *testing.T, stmt string) string {
	t.Helper()
	tx := testDB.Begin()
	require.NoError(t, tx.Error)
	defer tx.Rollback()
	require.NoError(t, tx.Exec("SET LOCAL enable_seqscan = off").Error)
	var lines []string
	require.NoError(t, tx.Raw("EXPLAIN "+stmt).Scan(&lines).Error)
	return strings.Join(lines, "\n")
}

// TestHotPathIndexes pins the index contracts behind two production hot paths.
//
// Reason: on prod-01 (2026-09-24) the ownership-event replay delete in UpsertToken and the
// URL-rewrite UPDATEs in UpdateMediaURLAndPropagate were sequentially scanning 5M-row
// tables because no index matched their predicates (migration 031 adds one for
// token_events; the *_url_hash indexes are partial and only apply when the query also
// asserts "<url> IS NOT NULL"). This test fails if either contract regresses: the index
// is dropped, or a query loses the predicate that lets the partial index apply.
// Constraint: it checks plans, not timings, so it stays deterministic on small test data.
func TestHotPathIndexes(t *testing.T) {
	if testDB == nil {
		t.Skip("Test database not initialized")
	}

	t.Run("ownership replay delete uses idx_token_events_token_event", func(t *testing.T) {
		plan := explainWithoutSeqScan(t,
			"DELETE FROM token_events WHERE token_id = 1 AND event_type IN ('acquired', 'released')")
		require.Contains(t, plan, "idx_token_events_token_event", plan)
		require.NotContains(t, plan, "Seq Scan", plan)
	})

	urlHashCases := []struct {
		table, urlCol, index string
	}{
		{"token_metadata", "image_url", "idx_token_metadata_image_url_hash"},
		{"token_metadata", "animation_url", "idx_token_metadata_animation_url_hash"},
		{"enrichment_sources", "image_url", "idx_enrichment_sources_image_url_hash"},
		{"enrichment_sources", "animation_url", "idx_enrichment_sources_animation_url_hash"},
	}
	for _, c := range urlHashCases {
		t.Run(c.table+"."+c.urlCol+" rewrite uses its partial index", func(t *testing.T) {
			withPredicate := "UPDATE " + c.table + " SET " + c.urlCol + " = 'u' WHERE " +
				c.urlCol + "_hash = 'h' AND " + c.urlCol + " IS NOT NULL"
			plan := explainWithoutSeqScan(t, withPredicate)
			require.Contains(t, plan, c.index, plan)

			// Negative control: without the IS NOT NULL predicate the partial index cannot
			// apply and the planner has nothing but a sequential scan, even with seq scans
			// disabled. This is the shape the production queries had before the fix.
			withoutPredicate := "UPDATE " + c.table + " SET " + c.urlCol + " = 'u' WHERE " +
				c.urlCol + "_hash = 'h'"
			require.Contains(t, explainWithoutSeqScan(t, withoutPredicate), "Seq Scan")
		})
	}
}
