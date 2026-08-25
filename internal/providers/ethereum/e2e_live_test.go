//go:build e2elive

// Live end-to-end checks for the head-driven ingestion path against a real
// Ethereum endpoint. Not part of make check: run explicitly with
//
//	E2E_ETH_WS=wss://... go test -tags e2elive -run TestE2E -v -count=1 ./internal/providers/ethereum/
//
// Reason: unit tests prove the subscriber's control flow with mocks; these
// prove the two claims that only a real node can — that FetchIngestionLogs
// returns exactly what the former eth_subscribe("logs") filter would have
// delivered for the same blocks, and that newHeads-driven fetches produce an
// ordered, gap-free, non-duplicated event stream from a live chain.
package ethereum_test

import (
	"context"
	"fmt"
	"math/big"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/stretchr/testify/require"

	"github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/block"
	"github.com/feral-file/ff-indexer-v2/internal/domain"
	ethprovider "github.com/feral-file/ff-indexer-v2/internal/providers/ethereum"
	"github.com/feral-file/ff-indexer-v2/internal/providers/ethereum/helpers"
)

type liveFixture struct {
	raw    adapter.EthClient
	client ethprovider.EthereumClient
	head   uint64
}

func newLiveFixture(t *testing.T) *liveFixture {
	t.Helper()
	wsURL := os.Getenv("E2E_ETH_WS")
	if wsURL == "" {
		t.Skip("E2E_ETH_WS not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	raw, err := adapter.NewEthClientDialer().Dial(ctx, wsURL)
	require.NoError(t, err)
	t.Cleanup(raw.Close)

	clock := adapter.NewClock()
	provider := block.NewBlockProvider(ethprovider.NewEthereumBlockFetcher(raw),
		block.Config{TTL: 12 * time.Second, StaleWindow: 60 * time.Second}, clock)
	client, err := ethprovider.NewGuardedClient(domain.ChainEthereumMainnet, raw, clock, provider,
		ethprovider.ClientGuards{GetLogsSpanCap: 10_000, GetLogsCallBudget: 100})
	require.NoError(t, err)

	head, err := client.GetLatestBlock(ctx)
	require.NoError(t, err)
	return &liveFixture{raw: raw, client: client, head: head}
}

type logKey struct {
	block uint64
	tx    common.Hash
	index uint
}

func keysOf(logs []types.Log) []logKey {
	keys := make([]logKey, 0, len(logs))
	for _, l := range logs {
		keys = append(keys, logKey{l.BlockNumber, l.TxHash, l.Index})
	}
	return keys
}

// TestE2E_FetchMatchesFormerSubscriptionFilter: the old subscriber built one
// filter (standard + custom topic0s, no address) and let the node push every
// match. The same filter through eth_getLogs must return the identical log set
// for a mined range — including the ERC-20-shaped 3-topic Transfers the
// adapters discard — proving the change moved transport, not selection.
func TestE2E_FetchMatchesFormerSubscriptionFilter(t *testing.T) {
	f := newLiveFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	from, to := f.head-6, f.head-1 // mined, past any tip churn
	newPath, err := f.client.FetchIngestionLogs(ctx, from, to)
	require.NoError(t, err)

	formerQuery := ethereum.FilterQuery{
		FromBlock: new(big.Int).SetUint64(from),
		ToBlock:   new(big.Int).SetUint64(to),
		Topics: [][]common.Hash{append(helpers.StandardEventSignatures(),
			f.client.ContractAdapterRegistry().GetCustomEventSignaturesForChain(domain.ChainEthereumMainnet)...)},
	}
	oldPath, err := f.raw.FilterLogs(ctx, formerQuery)
	require.NoError(t, err)

	require.NotEmpty(t, newPath, "a 6-block mainnet range always has NFT-topic logs")
	require.Equal(t, keysOf(oldPath), keysOf(newPath), "same filter must select the same logs in the same order")
	require.True(t, sort.SliceIsSorted(newPath, func(i, j int) bool {
		if newPath[i].BlockNumber != newPath[j].BlockNumber {
			return newPath[i].BlockNumber < newPath[j].BlockNumber
		}
		return newPath[i].Index < newPath[j].Index
	}))

	// Determinism: the replay path (restart in the enqueue→cursor window,
	// reorg re-fetch) must see the same logs again.
	again, err := f.client.FetchIngestionLogs(ctx, from, to)
	require.NoError(t, err)
	require.Equal(t, keysOf(newPath), keysOf(again))

	threeTopic := 0
	for _, l := range newPath {
		if l.Topics[0] == helpers.TransferEventSignature && len(l.Topics) == 3 {
			threeTopic++
		}
	}
	t.Logf("blocks %d-%d: %d logs, %d ERC-20-shaped Transfers still selected (filter unchanged)", from, to, len(newPath), threeTopic)
}

// TestE2E_LiveSubscriberOrderedStream runs the real subscriber from a few
// blocks behind head: the first head must fill the gap, live heads must keep
// extending the stream, and events must arrive strictly ordered without
// duplicates and with block timestamps resolved.
func TestE2E_LiveSubscriberOrderedStream(t *testing.T) {
	f := newLiveFixture(t)
	sub, err := ethprovider.NewSubscriber(ethprovider.Config{
		ChainID: domain.ChainEthereumMainnet, MaxCatchupBlocks: 100,
	}, f.client)
	require.NoError(t, err)

	fromBlock := f.head - 3
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	var events []*domain.BlockchainEvent
	seen := map[string]struct{}{}
	handler := func(e *domain.BlockchainEvent) error {
		key := fmt.Sprintf("%s:%d", e.TxHash, e.LogIndex)
		if _, dup := seen[key]; dup {
			return fmt.Errorf("duplicate event %s at block %d", key, e.BlockNumber)
		}
		seen[key] = struct{}{}
		events = append(events, e)
		// Stop once two heads beyond the catch-up boundary have produced events.
		if e.BlockNumber >= f.head+2 {
			cancel()
		}
		return nil
	}
	err = sub.SubscribeEvents(ctx, fromBlock, handler)
	require.ErrorIs(t, err, context.Canceled, "subscriber must only stop because we canceled")

	require.NotEmpty(t, events)
	require.GreaterOrEqual(t, events[0].BlockNumber, fromBlock, "nothing below fromBlock")
	require.LessOrEqual(t, events[0].BlockNumber, f.head, "catch-up must start inside the gap")
	for i := 1; i < len(events); i++ {
		prev, cur := events[i-1], events[i]
		require.LessOrEqual(t, prev.BlockNumber, cur.BlockNumber, "blocks must be non-decreasing at event %d", i)
		if prev.BlockNumber == cur.BlockNumber {
			require.Less(t, prev.LogIndex, cur.LogIndex, "log index must increase within block %d", cur.BlockNumber)
		}
	}
	for _, e := range events {
		require.False(t, e.Timestamp.IsZero(), "block timestamp must be resolved for %s", e.TxHash)
		require.Equal(t, domain.ChainEthereumMainnet, e.Chain)
	}
	last := events[len(events)-1].BlockNumber
	t.Logf("fromBlock=%d head-at-start=%d events=%d blocks %d..%d (%d live blocks beyond start head)",
		fromBlock, f.head, len(events), events[0].BlockNumber, last, last-f.head)
}
