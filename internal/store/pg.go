package store

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"gorm.io/gorm"

	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/store/schema"
)

type pgStore struct {
	db *gorm.DB
}

// NewPGStore creates a new PostgreSQL store instance
func NewPGStore(db *gorm.DB) Store {
	return &pgStore{db: db}
}

// GetTokenByTokenCID retrieves a token by its canonical ID
func (s *pgStore) GetTokenByTokenCID(ctx context.Context, tokenCID string) (*schema.Token, error) {
	var token schema.Token
	err := s.db.WithContext(ctx).Where("token_cid = ?", tokenCID).First(&token).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get token: %w", err)
	}

	return &token, nil
}

// IsAnyAddressWatched checks if any of the given addresses are being watched on a specific chain
func (s *pgStore) IsAnyAddressWatched(ctx context.Context, chain domain.Chain, addresses []string) (bool, error) {
	if len(addresses) == 0 {
		return false, nil
	}

	var count int64
	err := s.db.WithContext(ctx).
		Model(&schema.WatchedAddresses{}).
		Where("chain = ? AND address IN ? AND watching = ?", string(chain), addresses, true).
		Count(&count).Error
	if err != nil {
		return false, fmt.Errorf("failed to check watched addresses: %w", err)
	}

	return count > 0, nil
}

// GetBlockCursor retrieves the last processed block number for a chain
func (s *pgStore) GetBlockCursor(ctx context.Context, chain string) (uint64, error) {
	key := fmt.Sprintf("block_cursor:%s", chain)

	var kv schema.KeyValueStore
	err := s.db.WithContext(ctx).Where("key = ?", key).First(&kv).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return 0, nil
		}
		return 0, fmt.Errorf("failed to get block cursor: %w", err)
	}

	blockNumber, err := strconv.ParseUint(kv.Value, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("failed to parse block cursor: %w", err)
	}

	return blockNumber, nil
}

// SetBlockCursor stores the last processed block number for a chain
func (s *pgStore) SetBlockCursor(ctx context.Context, chain string, blockNumber uint64) error {
	key := fmt.Sprintf("block_cursor:%s", chain)
	value := strconv.FormatUint(blockNumber, 10)

	kv := schema.KeyValueStore{
		Key:   key,
		Value: value,
	}

	err := s.db.WithContext(ctx).Save(&kv).Error
	if err != nil {
		return fmt.Errorf("failed to set block cursor: %w", err)
	}

	return nil
}

// CreateTokenMint creates a new token with associated balance, change journal, and provenance event in a single transaction
func (s *pgStore) CreateTokenMint(ctx context.Context, input CreateTokenMintInput) error {
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// 1. Create the token
		token := schema.Token{
			TokenCID:         input.Token.TokenCID,
			Chain:            input.Token.Chain,
			Standard:         input.Token.Standard,
			ContractAddress:  input.Token.ContractAddress,
			TokenNumber:      input.Token.TokenNumber,
			CurrentOwner:     input.Token.CurrentOwner,
			Burned:           input.Token.Burned,
			LastActivityTime: input.Token.LastActivityTime,
		}

		if err := tx.Create(&token).Error; err != nil {
			return fmt.Errorf("failed to create token: %w", err)
		}

		// 2. Create the balance record (if provided)
		if input.Balance != nil {
			balance := schema.Balance{
				TokenID:      token.ID,
				OwnerAddress: input.Balance.OwnerAddress,
				Quantity:     input.Balance.Quantity,
			}

			if err := tx.Create(&balance).Error; err != nil {
				return fmt.Errorf("failed to create balance: %w", err)
			}
		}

		// 3. Create the provenance event
		provenanceEvent := schema.ProvenanceEvent{
			TokenID:     token.ID,
			Chain:       input.ProvenanceEvent.Chain,
			EventType:   input.ProvenanceEvent.EventType,
			FromAddress: input.ProvenanceEvent.FromAddress,
			ToAddress:   input.ProvenanceEvent.ToAddress,
			Quantity:    input.ProvenanceEvent.Quantity,
			TxHash:      input.ProvenanceEvent.TxHash,
			BlockNumber: input.ProvenanceEvent.BlockNumber,
			BlockHash:   input.ProvenanceEvent.BlockHash,
			Timestamp:   input.ProvenanceEvent.Timestamp,
		}

		if err := tx.Create(&provenanceEvent).Error; err != nil {
			return fmt.Errorf("failed to create provenance event: %w", err)
		}

		// 4. Create the change journal entry
		changeJournal := schema.ChangesJournal{
			SubjectType: schema.SubjectTypeToken,
			SubjectID:   input.TokenCID,
			ChangedAt:   input.ChangedAt,
		}

		if err := tx.Create(&changeJournal).Error; err != nil {
			return fmt.Errorf("failed to create change journal: %w", err)
		}

		return nil
	})
}

// GetTokenMetadata retrieves the metadata for a token by its ID
func (s *pgStore) GetTokenMetadata(ctx context.Context, tokenID int64) (*schema.TokenMetadata, error) {
	var metadata schema.TokenMetadata
	err := s.db.WithContext(ctx).Where("token_id = ?", tokenID).First(&metadata).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get token metadata: %w", err)
	}

	return &metadata, nil
}

// UpsertTokenMetadata creates or updates token metadata
func (s *pgStore) UpsertTokenMetadata(ctx context.Context, input CreateTokenMetadataInput) error {
	metadata := schema.TokenMetadata{
		TokenID:         input.TokenID,
		OriginJSON:      input.OriginJSON,
		LatestJSON:      input.LatestJSON,
		LatestHash:      input.LatestHash,
		EnrichmentLevel: input.EnrichmentLevel,
		LastRefreshedAt: input.LastRefreshedAt,
		ImageURL:        input.ImageURL,
		AnimationURL:    input.AnimationURL,
		Name:            input.Name,
		Artists:         input.Artists,
	}

	err := s.db.WithContext(ctx).Save(&metadata).Error
	if err != nil {
		return fmt.Errorf("failed to upsert token metadata: %w", err)
	}

	return nil
}
