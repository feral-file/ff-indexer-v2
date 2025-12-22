package ethereum

import (
	"context"
	"fmt"
	"math/big"
	"time"

	"github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/block"
)

// ethereumBlockFetcher implements block.BlockFetcher for Ethereum
type ethereumBlockFetcher struct {
	client adapter.EthClient
}

func NewEthereumBlockFetcher(client adapter.EthClient) block.BlockFetcher {
	return &ethereumBlockFetcher{client: client}
}

// FetchLatestBlock fetches the latest block number from Ethereum
func (f *ethereumBlockFetcher) FetchLatestBlock(ctx context.Context) (uint64, error) {
	header, err := f.client.HeaderByNumber(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("failed to get latest block: %w", err)
	}
	return header.Number.Uint64(), nil
}

// FetchBlockTimestamp fetches the timestamp for a given block number from Ethereum
func (f *ethereumBlockFetcher) FetchBlockTimestamp(ctx context.Context, blockNumber uint64) (time.Time, error) {
	block, err := f.client.BlockByNumber(ctx, new(big.Int).SetUint64(blockNumber))
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to get block %d: %w", blockNumber, err)
	}
	return time.Unix(int64(block.Time()), 0), nil //nolint:gosec,G115
}
