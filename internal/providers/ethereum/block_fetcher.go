package ethereum

import (
	"context"
	"fmt"

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
