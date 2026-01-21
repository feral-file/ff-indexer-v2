package ethereum

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/block"
	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/logger"
)

func TestGetTokenCIDsByOwnerAndBlockRange_Integration(t *testing.T) {
	rpcURL := os.Getenv("ETHEREUM_RPC_URL")
	if rpcURL == "" {
		t.Skip("Skipping integration test: ETHEREUM_RPC_URL not set")
	}
	require.NoError(t, logger.Initialize(logger.Config{Debug: true}))

	testCases := []struct {
		name         string
		owner        string
		fromBlock    uint64
		toBlock      uint64
		expectedFile string
	}{
		{
			name:         "case_1",
			owner:        "0x457ee5f723C7606c12a7264b52e285906F91eEA6",
			fromBlock:    15_000_000,
			toBlock:      19_000_000,
			expectedFile: filepath.Join("test_assets", "0x457ee5f723C7606c12a7264b52e285906F91eEA6.json"),
		},
		{
			name:         "case_2",
			owner:        "0x99fc8AD516FBCC9bA3123D56e63A35d05AA9EFB8",
			fromBlock:    15_000_000,
			toBlock:      19_000_000,
			expectedFile: filepath.Join("test_assets", "0x99fc8AD516FBCC9bA3123D56e63A35d05AA9EFB8.json"),
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	dialer := adapter.NewEthClientDialer()
	ethClient, err := dialer.Dial(ctx, rpcURL)
	require.NoError(t, err)
	t.Cleanup(func() { ethClient.Close() })

	clock := adapter.NewClock()
	blockProvider := block.NewBlockProvider(
		NewEthereumBlockFetcher(ethClient),
		block.Config{
			TTL:               5 * time.Second,
			StaleWindow:       30 * time.Second,
			BlockTimestampTTL: 0,
		},
		clock,
	)

	client := NewClient(domain.ChainEthereumMainnet, ethClient, clock, blockProvider)

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			if tc.owner == "" || tc.expectedFile == "" {
				t.Skip("Skipping integration test case: fill owner/from/to/expectedFile")
			}
			require.LessOrEqual(t, tc.fromBlock, tc.toBlock)

			got, err := client.GetTokenCIDsByOwnerAndBlockRange(ctx, tc.owner, tc.fromBlock, tc.toBlock)
			require.NoError(t, err)
			t.Logf("token count: %d", len(got))
			sortTokensByCIDAndBlock(got)

			expectedData, err := os.ReadFile(tc.expectedFile)
			require.NoError(t, err)
			var expected []domain.TokenWithBlock
			require.NoError(t, json.Unmarshal(expectedData, &expected))
			sortTokensByCIDAndBlock(expected)

			require.Equal(t, len(expected), len(got))
			require.Equal(t, expected, got)
		})
	}
}

func sortTokensByCIDAndBlock(tokens []domain.TokenWithBlock) {
	sort.Slice(tokens, func(i, j int) bool {
		if tokens[i].TokenCID != tokens[j].TokenCID {
			return tokens[i].TokenCID < tokens[j].TokenCID
		}
		return tokens[i].BlockNumber < tokens[j].BlockNumber
	})
}
