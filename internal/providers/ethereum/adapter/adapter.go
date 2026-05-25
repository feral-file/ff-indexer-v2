// Package adapter provides configuration-driven contract adapters for legacy and standard tokens.
package adapter

import (
	"context"
	"fmt"
	"math/big"
	"strings"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"

	ethadapter "github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/domain"
)

// ContractAdapter defines contract-specific token operations used by the Ethereum client.
type ContractAdapter interface {
	TokenExists(ctx context.Context, contractAddress, tokenNumber string) (bool, error)
	TokenOwner(ctx context.Context, contractAddress, tokenNumber string) (string, error)
	TokenURI(ctx context.Context, contractAddress, tokenNumber string) (string, error)
	SupportsProvenance() bool
}

// SuccessCondition describes how to interpret a contract call result for existence checks.
type SuccessCondition string

const (
	// SuccessNoRevert treats a successful call as proof the token exists.
	SuccessNoRevert SuccessCondition = "no_revert"
	// SuccessAddressNonZero treats a non-zero address return value as proof the token exists.
	SuccessAddressNonZero SuccessCondition = "address_nonzero"
)

// MetadataSource describes where token metadata should be fetched from.
type MetadataSource string

const (
	// MetadataSourceOnChain fetches metadata via an on-chain URI method.
	MetadataSourceOnChain MetadataSource = "on_chain"
	// MetadataSourceVendorOnly skips on-chain metadata and relies on vendor enrichment.
	MetadataSourceVendorOnly MetadataSource = "vendor_only"
)

// MethodCall describes a declarative contract method invocation.
type MethodCall struct {
	Method           string
	ABI              abi.ABI
	Params           []string
	SuccessCondition SuccessCondition
}

// GenericAdapter executes configured contract calls for legacy or custom contracts.
type GenericAdapter struct {
	existence  *MethodCall
	owner      *MethodCall
	metadata   ContractMetadataConfig
	ethClient  ethadapter.EthClient
	provenance bool
}

// ContractMetadataConfig holds metadata routing configuration for a contract adapter.
type ContractMetadataConfig struct {
	Source MetadataSource
	Method *MethodCall
}

// NewGenericAdapter builds a GenericAdapter from parsed contract configuration.
func NewGenericAdapter(
	existence, owner *MethodCall,
	metadata ContractMetadataConfig,
	ethClient ethadapter.EthClient,
	supportsProvenance bool,
) *GenericAdapter {
	return &GenericAdapter{
		existence:  existence,
		owner:      owner,
		metadata:   metadata,
		ethClient:  ethClient,
		provenance: supportsProvenance,
	}
}

// TokenExists checks token existence using the configured existence method.
func (a *GenericAdapter) TokenExists(ctx context.Context, contractAddress, tokenNumber string) (bool, error) {
	if a.existence == nil {
		return false, fmt.Errorf("existence method not configured")
	}

	result, err := a.callMethod(ctx, contractAddress, tokenNumber, a.existence)
	if err != nil {
		if isExecutionRevert(err) {
			return false, nil
		}
		return false, err
	}

	return evaluateSuccess(result, a.existence.SuccessCondition)
}

// TokenOwner resolves the current owner using the configured owner method.
//
// When existence uses address_nonzero, a zero owner address means the token does not exist
// rather than a burn. This avoids misclassifying invalid legacy token IDs as burned during
// owner-specific indexing paths that skip a separate existence check.
func (a *GenericAdapter) TokenOwner(ctx context.Context, contractAddress, tokenNumber string) (string, error) {
	if a.owner == nil {
		return "", fmt.Errorf("owner method not configured")
	}

	result, err := a.callMethod(ctx, contractAddress, tokenNumber, a.owner)
	if err != nil {
		return "", err
	}

	owner, err := unpackAddress(a.owner.ABI, a.owner.Method, result)
	if err != nil {
		return "", err
	}

	if owner == common.HexToAddress(domain.ETHEREUM_ZERO_ADDRESS) &&
		a.existence != nil &&
		a.existence.SuccessCondition == SuccessAddressNonZero {
		return "", domain.ErrTokenNotFoundOnChain
	}

	return owner.Hex(), nil
}

// TokenURI returns on-chain metadata when configured; vendor-only contracts return empty.
func (a *GenericAdapter) TokenURI(ctx context.Context, contractAddress, tokenNumber string) (string, error) {
	switch a.metadata.Source {
	case MetadataSourceVendorOnly, "":
		return "", nil
	case MetadataSourceOnChain:
		if a.metadata.Method == nil {
			return "", fmt.Errorf("on-chain metadata method not configured")
		}
		result, err := a.callMethod(ctx, contractAddress, tokenNumber, a.metadata.Method)
		if err != nil {
			return "", err
		}
		var uri string
		if err := a.metadata.Method.ABI.UnpackIntoInterface(&uri, a.metadata.Method.Method, result); err != nil {
			return "", fmt.Errorf("failed to unpack metadata URI: %w", err)
		}
		return uri, nil
	default:
		return "", fmt.Errorf("unsupported metadata source: %s", a.metadata.Source)
	}
}

// SupportsProvenance reports whether full on-chain provenance indexing is supported.
func (a *GenericAdapter) SupportsProvenance() bool {
	return a.provenance
}

func (a *GenericAdapter) callMethod(
	ctx context.Context,
	contractAddress, tokenNumber string,
	call *MethodCall,
) ([]byte, error) {
	args, err := resolveParams(call.Params, tokenNumber)
	if err != nil {
		return nil, err
	}

	data, err := call.ABI.Pack(call.Method, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to pack %s: %w", call.Method, err)
	}

	contractAddr := common.HexToAddress(contractAddress)
	result, err := a.ethClient.CallContract(ctx, ethereum.CallMsg{
		To:   &contractAddr,
		Data: data,
	}, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to call %s: %w", call.Method, err)
	}
	if len(result) == 0 {
		return nil, fmt.Errorf("empty result from %s", call.Method)
	}

	return result, nil
}

func resolveParams(params []string, tokenNumber string) ([]any, error) {
	tokenID, ok := new(big.Int).SetString(tokenNumber, 10)
	if !ok {
		return nil, fmt.Errorf("invalid token number: %s", tokenNumber)
	}

	resolved := make([]any, len(params))
	for i, param := range params {
		switch param {
		case "${tokenId}":
			resolved[i] = tokenID
		default:
			return nil, fmt.Errorf("unsupported parameter placeholder: %s", param)
		}
	}

	return resolved, nil
}

func evaluateSuccess(result []byte, condition SuccessCondition) (bool, error) {
	switch condition {
	case SuccessAddressNonZero:
		if len(result) < common.AddressLength {
			return false, nil
		}
		addr := common.BytesToAddress(result[len(result)-common.AddressLength:])
		return addr != common.HexToAddress(domain.ETHEREUM_ZERO_ADDRESS), nil
	case SuccessNoRevert, "":
		return true, nil
	default:
		return false, fmt.Errorf("unsupported success condition: %s", condition)
	}
}

func unpackAddress(contractABI abi.ABI, method string, result []byte) (common.Address, error) {
	values, err := contractABI.Unpack(method, result)
	if err != nil {
		return common.Address{}, fmt.Errorf("failed to unpack %s result: %w", method, err)
	}
	if len(values) == 0 {
		return common.Address{}, fmt.Errorf("no return values from %s", method)
	}

	addr, ok := values[0].(common.Address)
	if !ok {
		return common.Address{}, fmt.Errorf("expected address from %s", method)
	}

	return addr, nil
}

func isExecutionRevert(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "execution reverted") ||
		strings.Contains(msg, "nonexistent token") ||
		strings.Contains(msg, "invalid opcode")
}
