package workflows

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/feral-file/ff-indexer-v2/internal/domain"
)

func TestFindLastCompleteBlockIndexByTarget(t *testing.T) {
	tokens := []domain.TokenWithBlock{
		{TokenCID: domain.TokenCID("t0"), BlockNumber: 100},
		{TokenCID: domain.TokenCID("t1"), BlockNumber: 100},
		{TokenCID: domain.TokenCID("t2"), BlockNumber: 101},
		{TokenCID: domain.TokenCID("t3"), BlockNumber: 101},
		{TokenCID: domain.TokenCID("t4"), BlockNumber: 101},
		{TokenCID: domain.TokenCID("t5"), BlockNumber: 102},
	}

	tests := []struct {
		name        string
		targetCount int
		wantEnd     int
	}{
		{name: "empty slice", targetCount: 3, wantEnd: 0},
		{name: "non-positive target", targetCount: 0, wantEnd: 0},
		{name: "target in first block", targetCount: 1, wantEnd: 2},
		{name: "target ends at first block boundary", targetCount: 2, wantEnd: 2},
		{name: "target in middle block allows overflow", targetCount: 3, wantEnd: 5},
		{name: "target in middle block deeper allows overflow", targetCount: 4, wantEnd: 5},
		{name: "target equals len", targetCount: len(tokens), wantEnd: len(tokens)},
		{name: "target greater than len", targetCount: len(tokens) + 10, wantEnd: len(tokens)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got int
			if tt.name == "empty slice" {
				got = findLastCompleteBlockIndexByTarget(nil, tt.targetCount)
			} else {
				got = findLastCompleteBlockIndexByTarget(tokens, tt.targetCount)
			}
			require.Equal(t, tt.wantEnd, got)
		})
	}
}

func TestFindLastCompleteBlockIndexByQuota(t *testing.T) {
	// Tokens grouped by block; order doesn't matter for this helper as long as blocks are contiguous.
	ascending := []domain.TokenWithBlock{
		{TokenCID: domain.TokenCID("t0"), BlockNumber: 100},
		{TokenCID: domain.TokenCID("t1"), BlockNumber: 100},
		{TokenCID: domain.TokenCID("t2"), BlockNumber: 101},
		{TokenCID: domain.TokenCID("t3"), BlockNumber: 101},
		{TokenCID: domain.TokenCID("t4"), BlockNumber: 101},
		{TokenCID: domain.TokenCID("t5"), BlockNumber: 102},
	}
	descending := []domain.TokenWithBlock{
		{TokenCID: domain.TokenCID("t5"), BlockNumber: 102},
		{TokenCID: domain.TokenCID("t4"), BlockNumber: 101},
		{TokenCID: domain.TokenCID("t3"), BlockNumber: 101},
		{TokenCID: domain.TokenCID("t2"), BlockNumber: 101},
		{TokenCID: domain.TokenCID("t1"), BlockNumber: 100},
		{TokenCID: domain.TokenCID("t0"), BlockNumber: 100},
	}

	tests := []struct {
		name         string
		tokens       []domain.TokenWithBlock
		allowedCount int
		wantEnd      int
	}{
		{name: "empty slice", tokens: nil, allowedCount: 3, wantEnd: 0},
		{name: "non-positive allowed", tokens: ascending, allowedCount: 0, wantEnd: 0},
		{name: "allowed >= len", tokens: ascending, allowedCount: len(ascending), wantEnd: len(ascending)},
		{
			name:         "allowed cuts inside first block includes whole first block",
			tokens:       ascending,
			allowedCount: 1,
			wantEnd:      2,
		},
		{
			name:         "allowed cuts inside non-first block backs off to previous complete block",
			tokens:       ascending,
			allowedCount: 4, // boundary in block 101
			wantEnd:      2, // block 100 only
		},
		{
			name:         "descending order cuts inside a block backs off correctly",
			tokens:       descending,
			allowedCount: 2, // boundary in block 101 (cuts inside)
			wantEnd:      1, // only complete block 102
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := findLastCompleteBlockIndexByQuota(tt.tokens, tt.allowedCount)
			require.Equal(t, tt.wantEnd, got)
		})
	}
}

func TestChunkTokensByTargetBlockAligned(t *testing.T) {
	// Blocks:
	// - 100 has 2 tokens
	// - 101 has 3 tokens
	// - 102 has 4 tokens
	// - 103 has 1 token
	tokens := []domain.TokenWithBlock{
		{TokenCID: domain.TokenCID("t0"), BlockNumber: 100},
		{TokenCID: domain.TokenCID("t1"), BlockNumber: 100},
		{TokenCID: domain.TokenCID("t2"), BlockNumber: 101},
		{TokenCID: domain.TokenCID("t3"), BlockNumber: 101},
		{TokenCID: domain.TokenCID("t4"), BlockNumber: 101},
		{TokenCID: domain.TokenCID("t5"), BlockNumber: 102},
		{TokenCID: domain.TokenCID("t6"), BlockNumber: 102},
		{TokenCID: domain.TokenCID("t7"), BlockNumber: 102},
		{TokenCID: domain.TokenCID("t8"), BlockNumber: 102},
		{TokenCID: domain.TokenCID("t9"), BlockNumber: 103},
	}

	t.Run("first chunk uses firstBatchTarget, later chunks use subsequentBatchTarget, and never split blocks", func(t *testing.T) {
		chunks := chunkTokensByTargetBlockAligned(tokens, 5, 1)
		require.Len(t, chunks, 3)
		require.Len(t, chunks[0], 5) // 2 (block 100) + 3 (block 101)
		require.Len(t, chunks[1], 4) // whole block 102
		require.Len(t, chunks[2], 1) // whole block 103

		// Ensure no block number appears in multiple chunks.
		seen := make(map[uint64]int)
		for chunkIndex, chunk := range chunks {
			for _, tok := range chunk {
				if prev, ok := seen[tok.BlockNumber]; ok && prev != chunkIndex {
					t.Fatalf("block %d appears in multiple chunks: %d and %d", tok.BlockNumber, prev, chunkIndex)
				}
				seen[tok.BlockNumber] = chunkIndex
			}
		}
	})

	t.Run("first target smaller than first block still includes whole first block", func(t *testing.T) {
		chunks := chunkTokensByTargetBlockAligned(tokens, 1, 1)
		require.GreaterOrEqual(t, len(chunks), 1)
		require.Len(t, chunks[0], 2)
	})

	t.Run("non-positive targets fall back to defaults and still make progress", func(t *testing.T) {
		// defaults are first=20, subsequent=1 -> with only 10 tokens, first chunk includes all.
		chunks := chunkTokensByTargetBlockAligned(tokens, 0, 0)
		require.Len(t, chunks, 1)
		require.Len(t, chunks[0], len(tokens))
	})
}

func TestChunkTokensByTargetBlockAligned_RequiresGroupedTokens(t *testing.T) {
	// This is a documentation test: the helper assumes tokens from the same block are contiguous.
	// We don't enforce this in code, but we validate that callers should sort/group first.
	tokens := []domain.TokenWithBlock{
		{TokenCID: domain.TokenCID("t0"), BlockNumber: 100},
		{TokenCID: domain.TokenCID("t1"), BlockNumber: 101},
		{TokenCID: domain.TokenCID("t2"), BlockNumber: 100}, // out-of-order block 100
	}

	chunks := chunkTokensByTargetBlockAligned(tokens, 1, 1)

	// The function will still return chunks, but block-alignment isn't guaranteed with ungrouped input.
	// We assert only that it makes forward progress and returns a partition of the input.
	var count int
	for _, c := range chunks {
		count += len(c)
	}
	require.Equal(t, len(tokens), count, fmt.Sprintf("expected all tokens to be included, got %d", count))
}
