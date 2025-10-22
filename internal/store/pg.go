package store

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"go.uber.org/zap"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	logger "github.com/bitmark-inc/autonomy-logger"
	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/store/schema"
	"github.com/feral-file/ff-indexer-v2/internal/types"
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

		// 2. Create the balance record
		balance := schema.Balance{
			TokenID:      token.ID,
			OwnerAddress: input.Balance.OwnerAddress,
			Quantity:     input.Balance.Quantity,
		}
		if err := tx.Create(&balance).Error; err != nil {
			return fmt.Errorf("failed to create balance: %w", err)
		}

		// 3. Create the provenance event with conflict handling
		provenanceEvent := schema.ProvenanceEvent{
			TokenID:     token.ID,
			Chain:       input.ProvenanceEvent.Chain,
			EventType:   input.ProvenanceEvent.EventType,
			FromAddress: input.ProvenanceEvent.FromAddress,
			ToAddress:   input.ProvenanceEvent.ToAddress,
			Quantity:    &input.ProvenanceEvent.Quantity,
			TxHash:      &input.ProvenanceEvent.TxHash,
			BlockNumber: &input.ProvenanceEvent.BlockNumber,
			BlockHash:   input.ProvenanceEvent.BlockHash,
			Raw:         input.ProvenanceEvent.Raw,
			Timestamp:   input.ProvenanceEvent.Timestamp,
		}

		// Use ON CONFLICT DO NOTHING to skip duplicates based on (chain, tx_hash)
		if err := tx.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "chain"}, {Name: "tx_hash"}},
			DoNothing: true,
		}).Clauses(clause.Returning{Columns: []clause.Column{}}).
			Create(&provenanceEvent).Error; err != nil {
			return fmt.Errorf("failed to create provenance event: %w", err)
		}

		// If the event was a duplicate, we need to fetch it to get the ID
		if provenanceEvent.ID == 0 {
			if err := tx.Where("chain = ? AND tx_hash = ?", input.ProvenanceEvent.Chain, input.ProvenanceEvent.TxHash).
				First(&provenanceEvent).Error; err != nil {
				return fmt.Errorf("failed to fetch existing provenance event: %w", err)
			}
		}

		// 4. Create the change journal entry
		// For mint events: subject_type = 'token', subject_id = provenance_event_id
		changeJournal := schema.ChangesJournal{
			TokenID:     token.ID,
			SubjectType: schema.SubjectTypeToken,
			SubjectID:   fmt.Sprintf("%d", provenanceEvent.ID),
			ChangedAt:   input.ProvenanceEvent.Timestamp,
		}

		if err := tx.Clauses(clause.OnConflict{
			DoNothing: true,
		}).
			Clauses(clause.Returning{Columns: []clause.Column{}}).
			Create(&changeJournal).Error; err != nil {
			return fmt.Errorf("failed to create change journal: %w", err)
		}

		return nil
	})
}

// GetTokenWithMetadataByTokenCID retrieves a token with its metadata by canonical ID using JOIN
func (s *pgStore) GetTokenWithMetadataByTokenCID(ctx context.Context, tokenCID string) (*TokensWithMetadataResult, error) {
	var result struct {
		schema.Token
		schema.TokenMetadata
		MetadataExists bool `gorm:"column:metadata_exists"`
	}

	err := s.db.WithContext(ctx).
		Table("tokens").
		Select("tokens.*, token_metadata.*, CASE WHEN token_metadata.token_id IS NOT NULL THEN true ELSE false END as metadata_exists").
		Joins("LEFT JOIN token_metadata ON tokens.id = token_metadata.token_id").
		Where("tokens.token_cid = ?", tokenCID).
		First(&result).Error

	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get token with metadata: %w", err)
	}

	tokenResult := &TokensWithMetadataResult{
		Token: &result.Token,
	}

	if result.MetadataExists {
		tokenResult.Metadata = &result.TokenMetadata
	}

	return tokenResult, nil
}

// GetTokensByFilter retrieves tokens with their metadata based on filters
func (s *pgStore) GetTokensByFilter(ctx context.Context, filter TokenQueryFilter) ([]*TokensWithMetadataResult, uint64, error) {
	query := s.db.WithContext(ctx).Model(&schema.Token{})

	// Apply filters
	if len(filter.Owners) > 0 {
		// Join with balances to filter by owners
		query = query.Joins("LEFT JOIN balances ON balances.token_id = tokens.id").
			Where("balances.owner_address IN ? OR tokens.current_owner IN ?", filter.Owners, filter.Owners).
			Distinct("tokens.id")
	}

	if len(filter.Chains) > 0 {
		query = query.Where("chain IN ?", filter.Chains)
	}

	if len(filter.ContractAddresses) > 0 {
		query = query.Where("contract_address IN ?", filter.ContractAddresses)
	}

	if len(filter.TokenNumbers) > 0 {
		query = query.Where("token_number IN ?", filter.TokenNumbers)
	}

	// Count total before pagination
	var total int64
	countQuery := query
	if err := countQuery.Count(&total).Error; err != nil {
		return nil, 0, fmt.Errorf("failed to count tokens: %w", err)
	}

	// Apply pagination
	query = query.Order("id ASC").Limit(filter.Limit).Offset(filter.Offset)

	var tokens []schema.Token
	if err := query.Find(&tokens).Error; err != nil {
		return nil, 0, fmt.Errorf("failed to get tokens: %w", err)
	}

	// Fetch metadata for all tokens
	if len(tokens) == 0 {
		return []*TokensWithMetadataResult{}, uint64(total), nil //nolint:gosec,G115
	}

	tokenIDs := make([]uint64, len(tokens))
	for i, token := range tokens {
		tokenIDs[i] = token.ID
	}

	var metadataList []schema.TokenMetadata
	if err := s.db.WithContext(ctx).Where("token_id IN ?", tokenIDs).Find(&metadataList).Error; err != nil {
		return nil, 0, fmt.Errorf("failed to get token metadata: %w", err)
	}

	// Create a map of metadata by token ID
	metadataMap := make(map[uint64]*schema.TokenMetadata)
	for i := range metadataList {
		metadataMap[metadataList[i].TokenID] = &metadataList[i]
	}

	// Combine tokens with their metadata
	results := make([]*TokensWithMetadataResult, len(tokens))
	for i := range tokens {
		results[i] = &TokensWithMetadataResult{
			Token:    &tokens[i],
			Metadata: metadataMap[tokens[i].ID],
		}
	}

	return results, uint64(total), nil //nolint:gosec,G115
}

// GetTokenOwners retrieves owners (balances) for a token
func (s *pgStore) GetTokenOwners(ctx context.Context, tokenID uint64, limit int, offset int) ([]schema.Balance, uint64, error) {
	query := s.db.WithContext(ctx).Model(&schema.Balance{}).Where("token_id = ?", tokenID)

	// Count total
	var total int64
	if err := query.Count(&total).Error; err != nil {
		return nil, 0, fmt.Errorf("failed to count owners: %w", err)
	}

	// Apply pagination
	query = query.Order("id ASC").Limit(limit).Offset(offset)

	var balances []schema.Balance
	if err := query.Find(&balances).Error; err != nil {
		return nil, 0, fmt.Errorf("failed to get owners: %w", err)
	}

	return balances, uint64(total), nil //nolint:gosec,G115
}

// GetTokenProvenanceEvents retrieves provenance events for a token
func (s *pgStore) GetTokenProvenanceEvents(ctx context.Context, tokenID uint64, limit int, offset int, orderDesc bool) ([]schema.ProvenanceEvent, uint64, error) {
	query := s.db.WithContext(ctx).Model(&schema.ProvenanceEvent{}).Where("token_id = ?", tokenID)

	// Count total
	var total int64
	if err := query.Count(&total).Error; err != nil {
		return nil, 0, fmt.Errorf("failed to count provenance events: %w", err)
	}

	// Apply ordering by timestamp (with ID as tiebreaker)
	if orderDesc {
		query = query.Order("timestamp DESC, id DESC")
	} else {
		query = query.Order("timestamp ASC, id ASC")
	}

	// Apply pagination
	query = query.Limit(limit).Offset(offset)

	var events []schema.ProvenanceEvent
	if err := query.Find(&events).Error; err != nil {
		return nil, 0, fmt.Errorf("failed to get provenance events: %w", err)
	}

	return events, uint64(total), nil //nolint:gosec,G115
}

// GetTokenMetadataByTokenCID retrieves token metadata by token CID
func (s *pgStore) GetTokenMetadataByTokenCID(ctx context.Context, tokenCID string) (*schema.TokenMetadata, error) {
	var metadata schema.TokenMetadata
	err := s.db.WithContext(ctx).
		Joins("JOIN tokens ON tokens.id = token_metadata.token_id").
		Where("tokens.token_cid = ?", tokenCID).
		First(&metadata).Error
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

// UpdateTokenBurn updates a token as burned with associated balance update, change journal, and provenance event in a single transaction
// This method assumes the token and balance records already exist
func (s *pgStore) UpdateTokenBurn(ctx context.Context, input CreateTokenBurnInput) error {
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// 1. Get the token to ensure it exists
		var token schema.Token
		err := tx.Where("token_cid = ?", input.TokenCID).First(&token).Error
		if err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return domain.ErrTokenNotFound
			}
			return fmt.Errorf("failed to get token: %w", err)
		}

		// 2. Update the token: set burned = true, current_owner = nil, last_activity_time
		token.Burned = true
		token.CurrentOwner = nil
		token.LastActivityTime = input.LastActivityTime

		if err := tx.Save(&token).Error; err != nil {
			return fmt.Errorf("failed to update token burn: %w", err)
		}

		// 3. Update sender's balance (decrease by quantity, delete if reaches 0)
		if input.SenderBalanceUpdate != nil {
			// Use raw SQL to perform numeric subtraction
			tx := tx.Model(&schema.Balance{}).
				Where("token_id = ? AND owner_address = ?", token.ID, input.SenderBalanceUpdate.OwnerAddress).
				Update("quantity", gorm.Expr("quantity - ?", input.SenderBalanceUpdate.Delta))

			if tx.Error != nil {
				return fmt.Errorf("failed to update sender balance: %w", tx.Error)
			}

			// Delete balance if it reaches zero
			if tx.RowsAffected > 0 {
				var senderBalance schema.Balance
				if err := tx.Where("token_id = ? AND owner_address = ?", token.ID, input.SenderBalanceUpdate.OwnerAddress).
					First(&senderBalance).Error; err != nil {
					if errors.Is(err, gorm.ErrRecordNotFound) {
						logger.Warn("Sender balance not found", zap.String("token_cid", token.TokenCID), zap.String("owner_address", input.SenderBalanceUpdate.OwnerAddress))
					} else {
						return fmt.Errorf("failed to find sender balance: %w", err)
					}
				}

				if senderBalance.Quantity == "0" {
					if err := tx.Delete(&senderBalance).Error; err != nil {
						return fmt.Errorf("failed to delete zero balance: %w", err)
					}
				}
			}
		}

		// 4. Create the provenance event with conflict handling
		provenanceEvent := schema.ProvenanceEvent{
			TokenID:     token.ID,
			Chain:       input.ProvenanceEvent.Chain,
			EventType:   input.ProvenanceEvent.EventType,
			FromAddress: input.ProvenanceEvent.FromAddress,
			ToAddress:   input.ProvenanceEvent.ToAddress,
			Quantity:    &input.ProvenanceEvent.Quantity,
			TxHash:      &input.ProvenanceEvent.TxHash,
			BlockNumber: &input.ProvenanceEvent.BlockNumber,
			BlockHash:   input.ProvenanceEvent.BlockHash,
			Raw:         input.ProvenanceEvent.Raw,
			Timestamp:   input.ProvenanceEvent.Timestamp,
		}

		// Use ON CONFLICT DO NOTHING to skip duplicates based on (chain, tx_hash)
		if err := tx.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "chain"}, {Name: "tx_hash"}},
			DoNothing: true,
		}).Clauses(clause.Returning{Columns: []clause.Column{}}).
			Create(&provenanceEvent).Error; err != nil {
			return fmt.Errorf("failed to create provenance event: %w", err)
		}

		// If the event was a duplicate, we need to fetch it to get the ID
		if provenanceEvent.ID == 0 {
			if err := tx.Where("chain = ? AND tx_hash = ?", input.ProvenanceEvent.Chain, input.ProvenanceEvent.TxHash).
				First(&provenanceEvent).Error; err != nil {
				return fmt.Errorf("failed to fetch existing provenance event: %w", err)
			}
		}

		// 5. Create the change journal entry
		// For burn events: subject_type = 'token', subject_id = provenance_event_id
		changeJournal := schema.ChangesJournal{
			TokenID:     token.ID,
			SubjectType: schema.SubjectTypeToken,
			SubjectID:   fmt.Sprintf("%d", provenanceEvent.ID),
			ChangedAt:   input.ChangedAt,
		}

		if err := tx.Clauses(clause.OnConflict{
			DoNothing: true,
		}).
			Clauses(clause.Returning{Columns: []clause.Column{}}).
			Create(&changeJournal).Error; err != nil {
			return fmt.Errorf("failed to create change journal: %w", err)
		}

		return nil
	})
}

// CreateMetadataUpdate creates a provenance event and change journal entry for a metadata update
func (s *pgStore) CreateMetadataUpdate(ctx context.Context, input CreateMetadataUpdateInput) error {
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// 1. Get the token to ensure it exists
		var token schema.Token
		err := tx.Where("token_cid = ?", input.TokenCID).First(&token).Error
		if err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return domain.ErrTokenNotFound
			}
			return fmt.Errorf("failed to get token: %w", err)
		}

		// 2. Create the provenance event with conflict handling
		provenanceEvent := schema.ProvenanceEvent{
			TokenID:     token.ID,
			Chain:       input.ProvenanceEvent.Chain,
			EventType:   input.ProvenanceEvent.EventType,
			FromAddress: input.ProvenanceEvent.FromAddress,
			ToAddress:   input.ProvenanceEvent.ToAddress,
			Quantity:    &input.ProvenanceEvent.Quantity,
			TxHash:      &input.ProvenanceEvent.TxHash,
			BlockNumber: &input.ProvenanceEvent.BlockNumber,
			BlockHash:   input.ProvenanceEvent.BlockHash,
			Raw:         input.ProvenanceEvent.Raw,
			Timestamp:   input.ProvenanceEvent.Timestamp,
		}

		// Use ON CONFLICT DO NOTHING to skip duplicates based on (chain, tx_hash)
		if err := tx.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "chain"}, {Name: "tx_hash"}},
			DoNothing: true,
		}).Clauses(clause.Returning{Columns: []clause.Column{}}).
			Create(&provenanceEvent).Error; err != nil {
			return fmt.Errorf("failed to create provenance event: %w", err)
		}

		// If the event was a duplicate, we need to fetch it to get the ID
		if provenanceEvent.ID == 0 {
			if err := tx.Where("chain = ? AND tx_hash = ?", input.ProvenanceEvent.Chain, input.ProvenanceEvent.TxHash).
				First(&provenanceEvent).Error; err != nil {
				return fmt.Errorf("failed to fetch existing provenance event: %w", err)
			}
		}

		// 3. Create the change journal entry
		// For metadata updates: subject_type = 'metadata', subject_id = provenance_event_id
		changeJournal := schema.ChangesJournal{
			TokenID:     token.ID,
			SubjectType: schema.SubjectTypeMetadata,
			SubjectID:   fmt.Sprintf("%d", provenanceEvent.ID),
			ChangedAt:   input.ChangedAt,
		}

		if err := tx.Clauses(clause.OnConflict{
			DoNothing: true,
		}).
			Clauses(clause.Returning{Columns: []clause.Column{}}).
			Create(&changeJournal).Error; err != nil {
			return fmt.Errorf("failed to create change journal: %w", err)
		}

		return nil
	})
}

// UpdateTokenTransfer updates a token transfer (assumes token exists)
func (s *pgStore) UpdateTokenTransfer(ctx context.Context, input UpdateTokenTransferInput) error {
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// 1. Get the token to ensure it exists
		var token schema.Token
		err := tx.Where("token_cid = ?", input.TokenCID).First(&token).Error
		if err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return domain.ErrTokenNotFound
			}
			return fmt.Errorf("failed to get token: %w", err)
		}

		// 2. Update the token
		token.CurrentOwner = input.CurrentOwner
		token.LastActivityTime = input.LastActivityTime

		if err := tx.Save(&token).Error; err != nil {
			return fmt.Errorf("failed to update token: %w", err)
		}

		// 3. Update sender's balance (decrease)
		if input.SenderBalanceUpdate != nil {
			// Use raw SQL to perform numeric subtraction
			tx := tx.Model(&schema.Balance{}).
				Where("token_id = ? AND owner_address = ?", token.ID, input.SenderBalanceUpdate.OwnerAddress).
				Update("quantity", gorm.Expr("quantity - ?", input.SenderBalanceUpdate.Delta))

			if tx.Error != nil {
				return fmt.Errorf("failed to update sender balance: %w", tx.Error)
			}

			// Delete balance if it reaches zero
			if tx.RowsAffected > 0 {
				var senderBalance schema.Balance
				if err := tx.Where("token_id = ? AND owner_address = ?", token.ID, input.SenderBalanceUpdate.OwnerAddress).First(&senderBalance).Error; err != nil {
					if errors.Is(err, gorm.ErrRecordNotFound) {
						logger.Warn("Sender balance not found", zap.String("token_cid", token.TokenCID), zap.String("owner_address", input.SenderBalanceUpdate.OwnerAddress))
					} else {
						return fmt.Errorf("failed to find sender balance: %w", err)
					}
				}

				if senderBalance.Quantity == "0" {
					if err := tx.Delete(&senderBalance).Error; err != nil {
						return fmt.Errorf("failed to delete zero balance: %w", err)
					}
				}
			}

		}

		// 4. Update receiver's balance (increase)
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

		// 5. Create the provenance event with conflict handling
		provenanceEvent := schema.ProvenanceEvent{
			TokenID:     token.ID,
			Chain:       input.ProvenanceEvent.Chain,
			EventType:   input.ProvenanceEvent.EventType,
			FromAddress: input.ProvenanceEvent.FromAddress,
			ToAddress:   input.ProvenanceEvent.ToAddress,
			Quantity:    &input.ProvenanceEvent.Quantity,
			TxHash:      &input.ProvenanceEvent.TxHash,
			BlockNumber: &input.ProvenanceEvent.BlockNumber,
			BlockHash:   input.ProvenanceEvent.BlockHash,
			Raw:         input.ProvenanceEvent.Raw,
			Timestamp:   input.ProvenanceEvent.Timestamp,
		}

		// Use ON CONFLICT DO NOTHING to skip duplicates based on (chain, tx_hash)
		if err := tx.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "chain"}, {Name: "tx_hash"}},
			DoNothing: true,
		}).Clauses(clause.Returning{Columns: []clause.Column{}}).
			Create(&provenanceEvent).Error; err != nil {
			return fmt.Errorf("failed to create provenance event: %w", err)
		}

		// If the event was a duplicate, we need to fetch it to get the ID
		if provenanceEvent.ID == 0 {
			if err := tx.Where("chain = ? AND tx_hash = ?", input.ProvenanceEvent.Chain, input.ProvenanceEvent.TxHash).
				First(&provenanceEvent).Error; err != nil {
				return fmt.Errorf("failed to fetch existing provenance event: %w", err)
			}
		}

		// 6. Create the change journal entry
		// For transfer events: subject_type = 'owner', subject_id = provenance_event_id
		changeJournal := schema.ChangesJournal{
			TokenID:     token.ID,
			SubjectType: schema.SubjectTypeOwner,
			SubjectID:   fmt.Sprintf("%d", provenanceEvent.ID),
			ChangedAt:   input.ChangedAt,
		}

		if err := tx.Clauses(clause.OnConflict{
			DoNothing: true,
		}).
			Clauses(clause.Returning{Columns: []clause.Column{}}).
			Create(&changeJournal).Error; err != nil {
			return fmt.Errorf("failed to create change journal: %w", err)
		}

		return nil
	})
}

// CreateTokenWithProvenances creates or updates a token with all its provenance data (balances and events)
func (s *pgStore) CreateTokenWithProvenances(ctx context.Context, input CreateTokenWithProvenancesInput) error {
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// 1. Upsert the token
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

		// Use ON CONFLICT to update if token already exists
		if err := tx.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "token_cid"}},
			DoUpdates: clause.AssignmentColumns([]string{"current_owner", "burned", "last_activity_time"}),
		}).Create(&token).Error; err != nil {
			return fmt.Errorf("failed to upsert token: %w", err)
		}

		// 2. Upsert all balances in batch (using ON CONFLICT to handle existing records)
		if len(input.Balances) > 0 {
			balances := make([]schema.Balance, 0, len(input.Balances))
			for _, balanceInput := range input.Balances {
				balances = append(balances, schema.Balance{
					TokenID:      token.ID,
					OwnerAddress: balanceInput.OwnerAddress,
					Quantity:     balanceInput.Quantity,
				})
			}

			// Use Clauses with OnConflict to upsert
			if err := tx.Clauses(clause.OnConflict{
				Columns:   []clause.Column{{Name: "token_id"}, {Name: "owner_address"}},
				DoUpdates: clause.AssignmentColumns([]string{"quantity"}),
			}).Create(&balances).Error; err != nil {
				return fmt.Errorf("failed to upsert balances: %w", err)
			}
		}

		// 3. Batch insert all provenance events (skip duplicates using ON CONFLICT)
		if len(input.Events) > 0 {
			provenanceEvents := make([]schema.ProvenanceEvent, 0, len(input.Events))
			for i := range input.Events {
				eventInput := input.Events[i]

				provenanceEvents = append(provenanceEvents, schema.ProvenanceEvent{
					TokenID:     token.ID,
					Chain:       eventInput.Chain,
					EventType:   eventInput.EventType,
					FromAddress: eventInput.FromAddress,
					ToAddress:   eventInput.ToAddress,
					Quantity:    &eventInput.Quantity,
					TxHash:      &eventInput.TxHash,
					BlockNumber: &eventInput.BlockNumber,
					BlockHash:   eventInput.BlockHash,
					Raw:         eventInput.Raw,
					Timestamp:   eventInput.Timestamp,
				})
			}

			// Use ON CONFLICT DO NOTHING to skip duplicates based on (chain, tx_hash)
			if err := tx.Clauses(clause.OnConflict{
				Columns:   []clause.Column{{Name: "chain"}, {Name: "tx_hash"}},
				DoNothing: true,
			}).Clauses(clause.Returning{Columns: []clause.Column{}}).
				Create(&provenanceEvents).Error; err != nil {
				return fmt.Errorf("failed to create provenance events: %w", err)
			}

			// 4. Query back the events to get their IDs (for both newly inserted and existing events)
			// Use tx_hash as the primary identifier for batch query
			txHashes := make([]string, 0, len(input.Events))
			for _, eventInput := range input.Events {
				txHashes = append(txHashes, eventInput.TxHash)
			}

			var queriedEvents []schema.ProvenanceEvent
			if len(txHashes) > 0 {
				if err := tx.Where("token_id = ? AND tx_hash IN ?", token.ID, txHashes).
					Find(&queriedEvents).Error; err != nil {
					return fmt.Errorf("failed to query provenance events: %w", err)
				}
			}

			// 5. Batch insert changes_journal entries for all events
			// The unique constraint (token_id, subject_type, subject_id) will handle deduplication
			if len(queriedEvents) > 0 {
				changeJournals := make([]schema.ChangesJournal, 0, len(queriedEvents))
				for _, evt := range queriedEvents {
					subjectType := types.ProvenanceEventTypeToSubjectType(evt.EventType)
					changeJournals = append(changeJournals, schema.ChangesJournal{
						TokenID:     token.ID,
						SubjectType: subjectType,
						SubjectID:   fmt.Sprintf("%d", evt.ID),
						ChangedAt:   evt.Timestamp,
					})
				}

				// Use ON CONFLICT DO NOTHING to skip duplicates
				if err := tx.Clauses(clause.OnConflict{
					DoNothing: true,
				}).
					Clauses(clause.Returning{Columns: []clause.Column{}}).
					Create(&changeJournals).Error; err != nil {
					return fmt.Errorf("failed to create changes_journal entries: %w", err)
				}
			}
		}

		return nil
	})
}
