package store

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

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
			Raw:         input.ProvenanceEvent.Raw,
			Timestamp:   input.ProvenanceEvent.Timestamp,
		}

		if err := tx.Create(&provenanceEvent).Error; err != nil {
			return fmt.Errorf("failed to create provenance event: %w", err)
		}

		// 4. Create the change journal entry
		// For mint events: subject_type = 'token', subject_id = provenance_event_id
		changeJournal := schema.ChangesJournal{
			TokenID:     token.ID,
			SubjectType: schema.SubjectTypeToken,
			SubjectID:   fmt.Sprintf("%d", provenanceEvent.ID),
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

// CreateOrUpdateTokenTransfer creates or updates a token with associated balance updates, change journal, and provenance event in a single transaction
func (s *pgStore) CreateOrUpdateTokenTransfer(ctx context.Context, input CreateOrUpdateTokenTransferInput) (*CreateOrUpdateTokenTransferResult, error) {
	result := &CreateOrUpdateTokenTransferResult{}

	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// 1. Check if token exists
		var existingToken schema.Token
		err := tx.Where("token_cid = ?", input.Token.TokenCID).First(&existingToken).Error

		var token schema.Token
		if err != nil {
			if !errors.Is(err, gorm.ErrRecordNotFound) {
				return fmt.Errorf("failed to check if token exists: %w", err)
			}

			// Token doesn't exist, create it
			token = schema.Token{
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

			result.WasNewlyCreated = true
		} else {
			// Token exists, update it (but only if last_activity_time is newer or equal)
			// The database trigger will enforce the last_activity_time constraint
			token = existingToken
			token.CurrentOwner = input.Token.CurrentOwner
			token.Burned = input.Token.Burned
			token.LastActivityTime = input.Token.LastActivityTime

			if err := tx.Save(&token).Error; err != nil {
				return fmt.Errorf("failed to update token: %w", err)
			}

			result.WasNewlyCreated = false
		}

		result.TokenID = token.ID

		// 2. Update sender's balance (decrease)
		if input.SenderBalanceUpdate != nil {
			var senderBalance schema.Balance
			// Try to lock the row for update to ensure atomicity
			err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
				Where("token_id = ? AND owner_address = ?", token.ID, input.SenderBalanceUpdate.OwnerAddress).
				First(&senderBalance).Error

			if err != nil {
				if errors.Is(err, gorm.ErrRecordNotFound) {
					// Balance doesn't exist - create it with the delta as initial quantity
					// This can happen if events are processed out of order or the initial mint wasn't captured
					senderBalance = schema.Balance{
						TokenID:      token.ID,
						OwnerAddress: input.SenderBalanceUpdate.OwnerAddress,
						Quantity:     input.SenderBalanceUpdate.Delta,
					}

					if err := tx.Create(&senderBalance).Error; err != nil {
						return fmt.Errorf("failed to create sender balance: %w", err)
					}
				} else {
					return fmt.Errorf("failed to lock sender balance: %w", err)
				}
			} else {
				// Balance exists, update it
				// Use raw SQL to perform numeric subtraction
				if err := tx.Model(&senderBalance).
					Update("quantity", gorm.Expr("quantity - ?", input.SenderBalanceUpdate.Delta)).Error; err != nil {
					return fmt.Errorf("failed to update sender balance: %w", err)
				}

				// Refresh to get new quantity
				if err := tx.First(&senderBalance, senderBalance.ID).Error; err != nil {
					return fmt.Errorf("failed to refresh sender balance: %w", err)
				}

				// Delete balance if it reaches zero
				if senderBalance.Quantity == "0" {
					if err := tx.Delete(&senderBalance).Error; err != nil {
						return fmt.Errorf("failed to delete zero balance: %w", err)
					}
				}
			}
		}

		// 3. Update receiver's balance (increase)
		if input.ReceiverBalanceUpdate != nil {
			var receiverBalance schema.Balance
			// Lock the row for update to ensure atomicity
			err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
				Where("token_id = ? AND owner_address = ?", token.ID, input.ReceiverBalanceUpdate.OwnerAddress).
				First(&receiverBalance).Error

			if err != nil {
				if errors.Is(err, gorm.ErrRecordNotFound) {
					// Balance doesn't exist, create it
					receiverBalance = schema.Balance{
						TokenID:      token.ID,
						OwnerAddress: input.ReceiverBalanceUpdate.OwnerAddress,
						Quantity:     input.ReceiverBalanceUpdate.Delta,
					}

					if err := tx.Create(&receiverBalance).Error; err != nil {
						return fmt.Errorf("failed to create receiver balance: %w", err)
					}
				} else {
					return fmt.Errorf("failed to lock receiver balance: %w", err)
				}
			} else {
				// Balance exists, update it
				if err := tx.Model(&receiverBalance).
					Update("quantity", gorm.Expr("quantity + ?", input.ReceiverBalanceUpdate.Delta)).Error; err != nil {
					return fmt.Errorf("failed to update receiver balance: %w", err)
				}
			}
		}

		// 4. Create the provenance event
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
			Raw:         input.ProvenanceEvent.Raw,
			Timestamp:   input.ProvenanceEvent.Timestamp,
		}

		if err := tx.Create(&provenanceEvent).Error; err != nil {
			return fmt.Errorf("failed to create provenance event: %w", err)
		}

		// 5. Create the change journal entry
		// For transfer events: subject_type = 'owner', subject_id = provenance_event_id
		changeJournal := schema.ChangesJournal{
			TokenID:     token.ID,
			SubjectType: schema.SubjectTypeOwner,
			SubjectID:   fmt.Sprintf("%d", provenanceEvent.ID),
			ChangedAt:   input.ChangedAt,
		}

		if err := tx.Create(&changeJournal).Error; err != nil {
			return fmt.Errorf("failed to create change journal: %w", err)
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return result, nil
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
