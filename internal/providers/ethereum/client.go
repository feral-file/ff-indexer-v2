package ethereum

import (
	"context"
	"fmt"
	"math/big"
	"strings"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"

	"github.com/feral-file/ff-indexer-v2/internal/adapter"
)

type EthereumClient interface {
	// SubscribeFilterLogs subscribes to filter logs
	SubscribeFilterLogs(ctx context.Context, query ethereum.FilterQuery, ch chan<- types.Log) (ethereum.Subscription, error)

	// BlockByNumber returns a block by number
	BlockByNumber(ctx context.Context, number *big.Int) (*types.Block, error)

	// HeaderByNumber returns a header by number
	HeaderByNumber(ctx context.Context, number *big.Int) (*types.Header, error)

	// ERC721TokenURI fetches the tokenURI from an ERC721 contract
	ERC721TokenURI(ctx context.Context, contractAddress string, tokenNumber string) (string, error)

	// ERC1155URI fetches the uri from an ERC1155 contract
	ERC1155URI(ctx context.Context, contractAddress, tokenNumber string) (string, error)

	// ERC1155BalanceOf fetches the balance of a specific token ID for an owner from an ERC1155 contract
	ERC1155BalanceOf(ctx context.Context, contractAddress, ownerAddress, tokenNumber string) (string, error)

	// Close closes the connection
	Close()
}

type ethereumClient struct {
	client adapter.EthClient
}

func NewClient(client adapter.EthClient) EthereumClient {
	return &ethereumClient{client: client}
}

// SubscribeFilterLogs subscribes to filter logs
func (c *ethereumClient) SubscribeFilterLogs(ctx context.Context, query ethereum.FilterQuery, ch chan<- types.Log) (ethereum.Subscription, error) {
	return c.client.SubscribeFilterLogs(ctx, query, ch)
}

// BlockByNumber returns a block by number
func (c *ethereumClient) BlockByNumber(ctx context.Context, number *big.Int) (*types.Block, error) {
	return c.client.BlockByNumber(ctx, number)
}

// HeaderByNumber returns a header by number
func (c *ethereumClient) HeaderByNumber(ctx context.Context, number *big.Int) (*types.Header, error) {
	return c.client.HeaderByNumber(ctx, number)
}

// ERC721TokenURI fetches the tokenURI from an ERC721 contract
func (c *ethereumClient) ERC721TokenURI(ctx context.Context, contractAddress string, tokenNumber string) (string, error) {
	// ERC721 tokenURI function signature: tokenURI(uint256) returns (string)
	tokenURIABI, err := abi.JSON(strings.NewReader(`[{"constant":true,"inputs":[{"name":"tokenId","type":"uint256"}],"name":"tokenURI","outputs":[{"name":"","type":"string"}],"payable":false,"stateMutability":"view","type":"function"}]`))
	if err != nil {
		return "", fmt.Errorf("failed to parse ABI: %w", err)
	}

	tokenID, ok := new(big.Int).SetString(tokenNumber, 10)
	if !ok {
		return "", fmt.Errorf("invalid token number: %s", tokenNumber)
	}

	data, err := tokenURIABI.Pack("tokenURI", tokenID)
	if err != nil {
		return "", fmt.Errorf("failed to pack data: %w", err)
	}

	contractAddr := common.HexToAddress(contractAddress)
	result, err := c.client.CallContract(ctx, ethereum.CallMsg{
		To:   &contractAddr,
		Data: data,
	}, nil)
	if err != nil {
		return "", fmt.Errorf("failed to call contract: %w", err)
	}

	var uri string
	if err := tokenURIABI.UnpackIntoInterface(&uri, "tokenURI", result); err != nil {
		return "", fmt.Errorf("failed to unpack result: %w", err)
	}

	return uri, nil
}

// ERC1155URI fetches the uri from an ERC1155 contract
func (c *ethereumClient) ERC1155URI(ctx context.Context, contractAddress, tokenNumber string) (string, error) {
	// ERC1155 uri function signature: uri(uint256) returns (string)
	uriABI, err := abi.JSON(strings.NewReader(`[{"constant":true,"inputs":[{"name":"id","type":"uint256"}],"name":"uri","outputs":[{"name":"","type":"string"}],"payable":false,"stateMutability":"view","type":"function"}]`))
	if err != nil {
		return "", fmt.Errorf("failed to parse ABI: %w", err)
	}

	tokenID, ok := new(big.Int).SetString(tokenNumber, 10)
	if !ok {
		return "", fmt.Errorf("invalid token number: %s", tokenNumber)
	}

	data, err := uriABI.Pack("uri", tokenID)
	if err != nil {
		return "", fmt.Errorf("failed to pack data: %w", err)
	}

	contractAddr := common.HexToAddress(contractAddress)
	result, err := c.client.CallContract(ctx, ethereum.CallMsg{
		To:   &contractAddr,
		Data: data,
	}, nil)
	if err != nil {
		return "", fmt.Errorf("failed to call contract: %w", err)
	}

	var uri string
	if err := uriABI.UnpackIntoInterface(&uri, "uri", result); err != nil {
		return "", fmt.Errorf("failed to unpack result: %w", err)
	}

	return uri, nil
}

// ERC1155BalanceOf fetches the balance of a specific token ID for an owner from an ERC1155 contract
func (c *ethereumClient) ERC1155BalanceOf(ctx context.Context, contractAddress, ownerAddress, tokenNumber string) (string, error) {
	// ERC1155 balanceOf function signature: balanceOf(address,uint256) returns (uint256)
	balanceOfABI, err := abi.JSON(strings.NewReader(`[{"constant":true,"inputs":[{"name":"account","type":"address"},{"name":"id","type":"uint256"}],"name":"balanceOf","outputs":[{"name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"}]`))
	if err != nil {
		return "", fmt.Errorf("failed to parse ABI: %w", err)
	}

	tokenID, ok := new(big.Int).SetString(tokenNumber, 10)
	if !ok {
		return "", fmt.Errorf("invalid token number: %s", tokenNumber)
	}

	owner := common.HexToAddress(ownerAddress)
	data, err := balanceOfABI.Pack("balanceOf", owner, tokenID)
	if err != nil {
		return "", fmt.Errorf("failed to pack data: %w", err)
	}

	contractAddr := common.HexToAddress(contractAddress)
	result, err := c.client.CallContract(ctx, ethereum.CallMsg{
		To:   &contractAddr,
		Data: data,
	}, nil)
	if err != nil {
		return "", fmt.Errorf("failed to call contract: %w", err)
	}

	var balance *big.Int
	if err := balanceOfABI.UnpackIntoInterface(&balance, "balanceOf", result); err != nil {
		return "", fmt.Errorf("failed to unpack result: %w", err)
	}

	return balance.String(), nil
}

// Close closes the connection
func (c *ethereumClient) Close() {
	c.client.Close()
}
