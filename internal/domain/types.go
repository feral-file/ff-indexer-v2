package domain

import (
	"fmt"
	"strings"
	"time"
)

// Chain represents the blockchain network identifier using CAIP-2 format
type Chain string

const (
	ChainEthereumMainnet Chain = "eip155:1"
	ChainEthereumSepolia Chain = "eip155:11155111"
	ChainTezosMainnet    Chain = "tezos:mainnet"
	ChainTezosNarwhal    Chain = "tezos:narwhal"
)

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

// TokenCID represents the canonical token identifier in format: chain/standard:contract/tokenNumber (e.g., "eip155:1/erc721:0xabc.../1234")
type TokenCID string

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
	LogIndex        uint64        `json:"log_index"`                 // log index in the block (for ordering)
}

// TokenCID generates the canonical token ID
func (e *BlockchainEvent) TokenCID() TokenCID {
	return TokenCID(fmt.Sprintf("%s/%s/%s/%s", e.Chain, e.Standard, e.ContractAddress, e.TokenNumber))
}

// String returns the string representation of the TokenCID
func (t TokenCID) String() string {
	return string(t)
}

// Parse parses the TokenCID into chain, standard, contract address, and token number
func (t TokenCID) Parse() (Chain, ChainStandard, string, string) {
	parts := strings.Split(string(t), "/")
	return Chain(parts[0]), ChainStandard(parts[1]), parts[2], parts[3]
}
