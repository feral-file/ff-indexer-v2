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
	SubscribeFilterLogs(ctx context.Context, query ethereum.FilterQuery, ch chan<- types.Log) (ethereum.Subscription, error)
	BlockByNumber(ctx context.Context, number *big.Int) (*types.Block, error)
	HeaderByNumber(ctx context.Context, number *big.Int) (*types.Header, error)
	Close()
}

// EthClientDialer defines an interface for dialing Ethereum clients
//
//go:generate mockgen -source=ethclient.go -destination=../mocks/ethclient.go -package=mocks -mock_names=EthClientDialer=MockEthClientDialer
type EthClientDialer interface {
	Dial(rawurl string) (EthClient, error)
}

// RealEthClientDialer implements EthClientDialer using the standard ethclient package
type RealEthClientDialer struct{}

// NewEthClientDialer creates a new real Ethereum client dialer
func NewEthClientDialer() EthClientDialer {
	return &RealEthClientDialer{}
}

func (a *RealEthClientDialer) Dial(rawurl string) (EthClient, error) {
	return ethclient.Dial(rawurl)
}
