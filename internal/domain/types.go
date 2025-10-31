package domain

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"
)

// Blockchain represents the blockchain name
type Blockchain string

const (
	BlockchainEthereum Blockchain = "ethereum"
	BlockchainTezos    Blockchain = "tezos"
)

// Chain represents the blockchain network identifier using CAIP-2 format
type Chain string

const (
	ChainEthereumMainnet Chain = "eip155:1"
	ChainEthereumSepolia Chain = "eip155:11155111"
	ChainTezosMainnet    Chain = "tezos:mainnet"
	ChainTezosGhostnet   Chain = "tezos:ghostnet"
)

// IsValidChain checks if a chain is valid
func IsValidChain(chain Chain) bool {
	return chain == ChainEthereumMainnet ||
		chain == ChainEthereumSepolia ||
		chain == ChainTezosMainnet ||
		chain == ChainTezosGhostnet
}

// ChainStandard represents blockchain token standards
type ChainStandard string

const (
	StandardERC721  ChainStandard = "erc721"
	StandardERC1155 ChainStandard = "erc1155"
	StandardFA2     ChainStandard = "fa2"
)

// EventType represents the type of blockchain event
type EventType string

const (
	EventTypeTransfer            EventType = "transfer"
	EventTypeMint                EventType = "mint"
	EventTypeBurn                EventType = "burn"
	EventTypeMetadataUpdate      EventType = "metadata_update"
	EventTypeMetadataUpdateRange EventType = "metadata_update_range"
)

// TokenCID represents the canonical token identifier in format: chain:standard:contract:tokenNumber (e.g., "eip155:1:erc721:0xabc...:1234")
type TokenCID string

// TokenWithBlock represents a token identifier with its associated block number
// This is used for tracking when tokens were involved in transfers during block range sweeps
type TokenWithBlock struct {
	TokenCID    TokenCID `json:"token_cid"`
	BlockNumber uint64   `json:"block_number"`
}

// BlockchainEvent represents a normalized blockchain event
// This is the standard format published to NATS
type BlockchainEvent struct {
	Chain           Chain         `json:"chain"`                     // e.g., "eip155:1", "tezos:mainnet"
	Standard        ChainStandard `json:"standard"`                  // e.g., "erc721", "erc1155", "fa2"
	ContractAddress string        `json:"contract_address"`          // contract address
	TokenNumber     string        `json:"token_number"`              // token ID (or start token ID for range events)
	ToTokenNumber   string        `json:"to_token_number,omitempty"` // end token ID (only for metadata_update_range events)
	EventType       EventType     `json:"event_type"`                // transfer, mint, burn, metadata_update, metadata_update_range
	FromAddress     *string       `json:"from_address"`              // sender address (empty for mint)
	ToAddress       *string       `json:"to_address"`                // recipient address (empty for burn)
	Quantity        string        `json:"quantity"`                  // amount transferred (1 for ERC721/FA2, N for ERC1155)
	TxHash          string        `json:"tx_hash"`                   // transaction hash
	BlockNumber     uint64        `json:"block_number"`              // block number
	BlockHash       *string       `json:"block_hash,omitempty"`      // block hash (optional, nil if not available)
	Timestamp       time.Time     `json:"timestamp"`                 // block timestamp
	TxIndex         uint64        `json:"tx_index"`                  // transaction index in the block (for ordering)
}

func (e *BlockchainEvent) Valid() bool {
	// Validate token CID
	if !e.TokenCID().Valid() {
		return false
	}

	// Validate quantity
	quantity, err := strconv.ParseUint(e.Quantity, 10, 64)
	if err != nil {
		return false
	}
	if quantity == 0 {
		return false
	}

	// Validate different fields based on event type
	switch e.EventType {
	case EventTypeTransfer:
		// Validate from and to addresses
		if e.FromAddress == nil || e.ToAddress == nil {
			return false
		}
		// Ensure neither address is zero/empty (those should be mint/burn)
		if *e.FromAddress == "" || *e.FromAddress == ETHEREUM_ZERO_ADDRESS {
			return false
		}
		if *e.ToAddress == "" || *e.ToAddress == ETHEREUM_ZERO_ADDRESS {
			return false
		}
	case EventTypeMint:
		// Validate from address (must be nil, empty, or zero)
		if e.FromAddress != nil && *e.FromAddress != "" && *e.FromAddress != ETHEREUM_ZERO_ADDRESS {
			return false
		}
		// Validate to address (must exist and not be zero/empty)
		if e.ToAddress == nil || *e.ToAddress == "" || *e.ToAddress == ETHEREUM_ZERO_ADDRESS {
			return false
		}
	case EventTypeBurn:
		// Validate from address (must exist and not be zero/empty)
		if e.FromAddress == nil || *e.FromAddress == "" || *e.FromAddress == ETHEREUM_ZERO_ADDRESS {
			return false
		}
		// Validate to address (must be nil, empty, or zero)
		if e.ToAddress != nil && *e.ToAddress != "" && *e.ToAddress != ETHEREUM_ZERO_ADDRESS {
			return false
		}
	case EventTypeMetadataUpdate:
		// Validate from and to addresses
		if e.FromAddress != nil || e.ToAddress != nil {
			return false
		}
	case EventTypeMetadataUpdateRange:
		// Validate from and to addresses
		if e.FromAddress != nil || e.ToAddress != nil {
			return false
		}

		// Validate token number
		if e.TokenNumber == "" || e.ToTokenNumber == "" {
			return false
		}

		// Validate token numbers
		if !validTokenNumber(e.ToTokenNumber) {
			return false
		}
	default:
		return false
	}

	return true
}

// CurrentOwner returns the current owner of the token
func (e *BlockchainEvent) CurrentOwner() *string {
	// For FA2 and ERC1155, the current owner is nil since it's a multi-owner token
	if e.Standard == StandardFA2 || e.Standard == StandardERC1155 {
		return nil
	}
	return e.ToAddress
}

// TokenCID generates the canonical token ID
func (e *BlockchainEvent) TokenCID() TokenCID {
	return TokenCID(fmt.Sprintf("%s:%s:%s:%s", e.Chain, e.Standard, e.ContractAddress, e.TokenNumber))
}

// String returns the string representation of the TokenCID
func (t TokenCID) String() string {
	return string(t)
}

// Parse parses the TokenCID into chain, standard, contract address, and token number
func (t TokenCID) Parse() (Chain, ChainStandard, string, string) {
	parts := strings.Split(string(t), ":")
	return Chain(fmt.Sprintf("%s:%s", parts[0], parts[1])), ChainStandard(parts[2]), parts[3], parts[4]
}

// Valid checks if the TokenCID is valid
func (t TokenCID) Valid() bool {
	chain, standard, contractAddress, tokenNumber := t.Parse()

	// Validate token number
	if !validTokenNumber(tokenNumber) {
		return false
	}

	// Validate chain, standard, and contract address
	switch chain {
	case ChainEthereumMainnet, ChainEthereumSepolia:
		// Validate supported standards
		if standard != StandardERC721 && standard != StandardERC1155 {
			return false
		}

		// Validate contract address
		if !common.IsHexAddress(contractAddress) {
			return false
		}
	case ChainTezosMainnet, ChainTezosGhostnet:
		// Validate supported standards
		if standard != StandardFA2 {
			return false
		}

		// Validate contract address
		if !strings.HasPrefix(contractAddress, "KT1") {
			return false
		}
	default:
		return false
	}

	return true
}

// NewTokenCID creates a new TokenCID
func NewTokenCID(chain Chain, standard ChainStandard, contractAddress string, tokenNumber string) TokenCID {
	return TokenCID(fmt.Sprintf("%s:%s:%s:%s", chain, standard, contractAddress, tokenNumber))
}

// determineTransferEventType determines the event type based on from/to addresses
func TransferEventType(from *string, to *string) EventType {
	if from == nil || *from == "" || *from == ETHEREUM_ZERO_ADDRESS {
		return EventTypeMint
	}
	if to == nil || *to == "" || *to == ETHEREUM_ZERO_ADDRESS {
		return EventTypeBurn
	}
	return EventTypeTransfer
}

// AddressToBlockchain converts an address to the blockchain it belongs to
func AddressToBlockchain(address string) Blockchain {
	if strings.HasPrefix(address, "0x") {
		return BlockchainEthereum
	}
	return BlockchainTezos
}

// NormalizeAddresses normalizes a list of addresses to the format used by the blockchain
func NormalizeAddresses(addresses []string) []string {
	for i, address := range addresses {
		addresses[i] = NormalizeAddress(address)
	}
	return addresses
}

// NormalizeAddress normalizes an address to the format used by the blockchain
func NormalizeAddress(address string) string {
	if strings.HasPrefix(address, "0x") {
		return common.HexToAddress(address).String()
	}
	return address
}

// validTokenNumber checks if a token number is valid
func validTokenNumber(tokenNumber string) bool {
	return regexp.MustCompile(`^[0-9]*$`).MatchString(tokenNumber)
}
