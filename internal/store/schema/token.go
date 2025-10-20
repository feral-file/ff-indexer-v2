package schema

import (
	"time"

	"github.com/feral-file/ff-indexer-v2/internal/domain"
)

// Token represents the tokens table - the primary entity for tracking tokens across all supported blockchains
type Token struct {
	// ID is the internal database primary key
	ID int64 `gorm:"column:id;primaryKey;autoIncrement"`
	// TokenCID is the canonical token identifier in format: chain/standard:contract/tokenNumber (e.g., "eip155:1/erc721:0xabc.../1234")
	TokenCID string `gorm:"column:token_cid;not null;uniqueIndex;type:text"`
	// Chain identifies the blockchain network (e.g., "eip155:1" for Ethereum mainnet, "tezos:mainnet")
	Chain domain.Chain `gorm:"column:chain;not null;type:text;index:idx_tokens_chain_contract_number,priority:1"`
	// Standard identifies the token contract type (erc721, erc1155, fa2)
	Standard domain.ChainStandard `gorm:"column:standard;not null;type:text"`
	// ContractAddress is the blockchain address of the smart contract
	ContractAddress string `gorm:"column:contract_address;not null;type:text;index:idx_tokens_chain_contract_number,priority:2"`
	// TokenNumber is the token ID within the contract (string to support very large numbers)
	TokenNumber string `gorm:"column:token_number;not null;type:text;index:idx_tokens_chain_contract_number,priority:3"`
	// CurrentOwner is the current owner's blockchain address (nil for multi-owner ERC1155/FA2 tokens)
	CurrentOwner *string `gorm:"column:current_owner;type:text"`
	// Burned indicates whether the token has been permanently destroyed
	Burned bool `gorm:"column:burned;not null;default:false"`
	// LastActivityTime records the timestamp of the most recent on-chain activity for this token
	LastActivityTime time.Time `gorm:"column:last_activity_time;not null;default:now();type:timestamptz"`
	// CreatedAt is the timestamp when this record was first indexed
	CreatedAt time.Time `gorm:"column:created_at;not null;default:now();type:timestamptz"`
	// UpdatedAt is the timestamp when this record was last updated
	UpdatedAt time.Time `gorm:"column:updated_at;not null;default:now();type:timestamptz"`

	// Associations
	Balances          []Balance          `gorm:"foreignKey:TokenID;constraint:OnDelete:CASCADE"`
	Metadata          *TokenMetadata     `gorm:"foreignKey:TokenID;constraint:OnDelete:CASCADE"`
	EnrichmentSources []EnrichmentSource `gorm:"foreignKey:TokenID;constraint:OnDelete:CASCADE"`
	MediaAssets       []MediaAsset       `gorm:"foreignKey:TokenID;constraint:OnDelete:CASCADE"`
	ProvenanceEvents  []ProvenanceEvent  `gorm:"foreignKey:TokenID;constraint:OnDelete:CASCADE"`
	ChangesJournals   []ChangesJournal   `gorm:"foreignKey:TokenID;constraint:OnDelete:CASCADE"`
}

// TableName specifies the table name for the Token model
func (Token) TableName() string {
	return "tokens"
}
