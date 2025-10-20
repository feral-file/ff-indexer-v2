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

// UpdateBalanceInput represents the input for updating a balance record
type UpdateBalanceInput struct {
	OwnerAddress string
	Delta        string // Delta to add/subtract (can be negative)
}

// CreateOrUpdateTokenTransferInput represents the complete input for creating or updating a token transfer with all related data
type CreateOrUpdateTokenTransferInput struct {
	Token                 CreateTokenInput
	SenderBalanceUpdate   *UpdateBalanceInput // nil if sender is zero address (mint)
	ReceiverBalanceUpdate *UpdateBalanceInput // nil if receiver is zero address (burn)
	ProvenanceEvent       CreateProvenanceEventInput
	TokenCID              string    // For change journal
	ChangedAt             time.Time // For change journal
}

// CreateOrUpdateTokenTransferResult contains the result of a token transfer operation
type CreateOrUpdateTokenTransferResult struct {
	TokenID         int64
	WasNewlyCreated bool // true if token was created, false if it was updated
}

// CreateTokenBurnInput represents the complete input for updating a token burn with all related data
type CreateTokenBurnInput struct {
	TokenCID            string
	SenderBalanceUpdate *UpdateBalanceInput // Update sender balance (decrease by quantity)
	ProvenanceEvent     CreateProvenanceEventInput
	ChangedAt           time.Time // For change journal
	LastActivityTime    time.Time // For token update
}

// CreateMetadataUpdateInput represents the complete input for creating a metadata update record
type CreateMetadataUpdateInput struct {
	TokenCID        string
	ProvenanceEvent CreateProvenanceEventInput
	ChangedAt       time.Time // For change journal
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
	// CreateOrUpdateTokenTransfer creates or updates a token with associated balance updates, change journal, and provenance event in a single transaction
	CreateOrUpdateTokenTransfer(ctx context.Context, input CreateOrUpdateTokenTransferInput) (*CreateOrUpdateTokenTransferResult, error)
	// UpdateTokenBurn updates a token as burned with associated balance update, change journal, and provenance event in a single transaction
	UpdateTokenBurn(ctx context.Context, input CreateTokenBurnInput) error
	// CreateMetadataUpdate creates a provenance event and change journal entry for a metadata update
	CreateMetadataUpdate(ctx context.Context, input CreateMetadataUpdateInput) error
	// GetTokenMetadata retrieves the metadata for a token by its ID
	GetTokenMetadata(ctx context.Context, tokenID int64) (*schema.TokenMetadata, error)
	// UpsertTokenMetadata creates or updates token metadata
	UpsertTokenMetadata(ctx context.Context, input CreateTokenMetadataInput) error
}
