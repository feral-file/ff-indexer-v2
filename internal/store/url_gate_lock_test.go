package store

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/feral-file/ff-indexer-v2/internal/types"
)

// TestOrderedChangedURLGateHashes pins the deadlock-avoidance contract for
// metadata/enrichment media sync: every transaction must acquire its URL-gate advisory
// locks in one global order. Two concurrent updates whose image/animation pairs are
// reversed otherwise each hold their first lock while waiting for the other's, and
// postgres aborts one as a deadlock. Sortedness is the entire fix, so it is what this
// test asserts — along with the skip logic mirroring syncSingleMediaURL (unchanged and
// empty URLs must not add gate contention).
func TestOrderedChangedURLGateHashes(t *testing.T) {
	strPtr := func(s string) *string { return &s }
	urlP := "https://example.com/gate/p.html"
	urlQ := "https://example.com/gate/q.html"

	t.Run("reversed pairs produce the identical lock order", func(t *testing.T) {
		// Token X: image=P, animation=Q. Token Y: image=Q, animation=P. Role-order
		// locking gives [P,Q] and [Q,P] — the deadlock shape. Sorted hashes must agree.
		x := orderedChangedURLGateHashes(
			[2]*string{nil, strPtr(urlP)},
			[2]*string{nil, strPtr(urlQ)},
		)
		y := orderedChangedURLGateHashes(
			[2]*string{nil, strPtr(urlQ)},
			[2]*string{nil, strPtr(urlP)},
		)
		assert.Equal(t, x, y, "acquisition order must not depend on role order")
		assert.Len(t, x, 2)
		assert.True(t, sort.StringsAreSorted(x))
	})

	t.Run("unchanged, removed, and empty URLs add no locks", func(t *testing.T) {
		hashes := orderedChangedURLGateHashes(
			[2]*string{strPtr(urlP), strPtr(urlP)}, // unchanged: sync skips it
			[2]*string{strPtr(urlQ), nil},          // removed: only deletes, no gate read
			[2]*string{nil, nil},                   // absent on both sides
		)
		assert.Empty(t, hashes)
	})

	t.Run("the same URL in both roles locks once", func(t *testing.T) {
		hashes := orderedChangedURLGateHashes(
			[2]*string{nil, strPtr(urlP)},
			[2]*string{nil, strPtr(urlP)},
		)
		assert.Equal(t, []string{types.MD5Hash(urlP)}, hashes,
			"pg_advisory_xact_lock is reentrant, but the plan should not repeat itself")
	})

	t.Run("changed URL contributes its new hash, not its old", func(t *testing.T) {
		hashes := orderedChangedURLGateHashes(
			[2]*string{strPtr(urlP), strPtr(urlQ)},
		)
		assert.Equal(t, []string{types.MD5Hash(urlQ)}, hashes,
			"only the inserted row reads the gate; the old row is just deleted")
	})
}
