package store

import (
	"context"
	"time"

	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/store/schema"
)

// CreateTokenInput represents the input for creating a token
type CreateTokenInput struct {
	TokenCID         string
	Chain            domain.Chain
	Standard         domain.ChainStandard
	ContractAddress  string
	TokenNumber      string
	CurrentOwner     *string
	Burned           bool
	LastActivityTime time.Time
}

// CreateBalanceInput represents the input for creating a balance record
type CreateBalanceInput struct {
	OwnerAddress string
	Quantity     string
}

// CreateProvenanceEventInput represents the input for creating a provenance event
type CreateProvenanceEventInput struct {
	Chain       domain.Chain
	EventType   schema.ProvenanceEventType
	FromAddress *string
	ToAddress   *string
	Quantity    *string
	TxHash      *string
	BlockNumber *uint64
	BlockHash   *string
	Raw         []byte
	Timestamp   time.Time
}

// CreateTokenMintInput represents the complete input for creating a token mint with all related data
type CreateTokenMintInput struct {
	Token           CreateTokenInput
	Balance         *CreateBalanceInput // Optional, only if there's an owner
	ProvenanceEvent CreateProvenanceEventInput
	TokenCID        string    // For change journal
	ChangedAt       time.Time // For change journal
}

// CreateTokenMetadataInput represents the input for creating or updating token metadata
type CreateTokenMetadataInput struct {
	TokenID         int64
	OriginJSON      []byte
	LatestJSON      []byte
	LatestHash      *string
	EnrichmentLevel schema.EnrichmentLevel
	LastRefreshedAt *time.Time
	ImageURL        *string
	AnimationURL    *string
	Name            *string
	Artists         []string
}

// Store defines the interface for database operations
type Store interface {
	// GetTokenByTokenCID retrieves a token by its canonical ID
	GetTokenByTokenCID(ctx context.Context, tokenCID string) (*schema.Token, error)
	// IsAnyAddressWatched checks if any of the given addresses are being watched on a specific chain
	IsAnyAddressWatched(ctx context.Context, chain domain.Chain, addresses []string) (bool, error)
	// GetBlockCursor retrieves the last processed block number for a chain
	GetBlockCursor(ctx context.Context, chain string) (uint64, error)
	// SetBlockCursor stores the last processed block number for a chain
	SetBlockCursor(ctx context.Context, chain string, blockNumber uint64) error
	// CreateTokenMint creates a new token with associated balance, change journal, and provenance event in a single transaction
	CreateTokenMint(ctx context.Context, input CreateTokenMintInput) error
	// GetTokenMetadata retrieves the metadata for a token by its ID
	GetTokenMetadata(ctx context.Context, tokenID int64) (*schema.TokenMetadata, error)
	// UpsertTokenMetadata creates or updates token metadata
	UpsertTokenMetadata(ctx context.Context, input CreateTokenMetadataInput) error
}
