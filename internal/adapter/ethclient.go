package adapter

import (
	"context"
	"math/big"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
)

// EthClient defines an interface for Ethereum client operations to enable mocking
//
//go:generate mockgen -source=ethclient.go -destination=../mocks/ethclient.go -package=mocks -mock_names=EthClient=MockEthClient
type EthClient interface {
	// SubscribeFilterLogs subscribes to filter logs
	SubscribeFilterLogs(ctx context.Context, query ethereum.FilterQuery, ch chan<- types.Log) (ethereum.Subscription, error)

	// FilterLogs retrieves logs that match the filter query
	FilterLogs(ctx context.Context, query ethereum.FilterQuery) ([]types.Log, error)

	// BlockByNumber returns a block by number
	BlockByNumber(ctx context.Context, number *big.Int) (*types.Block, error)

	// HeaderByNumber returns a header by number
	HeaderByNumber(ctx context.Context, number *big.Int) (*types.Header, error)

	// CallContract calls a contract function
	CallContract(ctx context.Context, msg ethereum.CallMsg, blockNumber *big.Int) ([]byte, error)

	// Close closes the connection
	Close()
}

// EthClientDialer defines an interface for dialing Ethereum clients
//
//go:generate mockgen -source=ethclient.go -destination=../mocks/ethclient.go -package=mocks -mock_names=EthClientDialer=MockEthClientDialer
type EthClientDialer interface {
	Dial(ctx context.Context, rawurl string) (EthClient, error)
}

// RealEthClientDialer implements EthClientDialer using the standard ethclient package
type RealEthClientDialer struct{}

// NewEthClientDialer creates a new real Ethereum client dialer
func NewEthClientDialer() EthClientDialer {
	return &RealEthClientDialer{}
}

func (a *RealEthClientDialer) Dial(ctx context.Context, rawurl string) (EthClient, error) {
	return ethclient.DialContext(ctx, rawurl)
}
