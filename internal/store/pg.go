package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"go.uber.org/zap"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/plugin/dbresolver"

	"github.com/feral-file/ff-indexer-v2/internal/logger"

	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/store/schema"
	"github.com/feral-file/ff-indexer-v2/internal/types"
)

type pgStore struct {
	db *gorm.DB
}

func hasDBResolver(db *gorm.DB) bool {
	return db != nil && db.Callback().Query().Get("gorm:db_resolver") != nil
}

// NewPGStore creates a new PostgreSQL store instance
func NewPGStore(db *gorm.DB) Store {
	return &pgStore{db: db}
}

// ConfigureConnectionPool configures the connection pool settings for a GORM database connection.
// It accesses the underlying *sql.DB and sets the pool configuration.
// If any of the pool settings are 0 or empty, reasonable defaults are used:
//   - MaxOpenConns: 20 (if 0)
//   - MaxIdleConns: 5 (if 0)
//   - ConnMaxLifetime: 5 minutes (if 0)
//   - ConnMaxIdleTime: 10 minutes (if 0)
func ConfigureConnectionPool(db *gorm.DB, maxOpenConns, maxIdleConns int, connMaxLifetime, connMaxIdleTime time.Duration) error {
	sqlDB, err := db.DB()
	if err != nil {
		return fmt.Errorf("failed to get underlying sql.DB: %w", err)
	}

	maxOpenConns, maxIdleConns, connMaxLifetime, connMaxIdleTime =
		NormalizeConnectionPoolSettings(maxOpenConns, maxIdleConns, connMaxLifetime, connMaxIdleTime)

	sqlDB.SetMaxOpenConns(maxOpenConns)
	sqlDB.SetMaxIdleConns(maxIdleConns)
	sqlDB.SetConnMaxLifetime(connMaxLifetime)
	sqlDB.SetConnMaxIdleTime(connMaxIdleTime)

	return nil
}

// NormalizeConnectionPoolSettings applies defaults and clamps pool settings into safe values.
//
// Defaults (when zero):
//   - MaxOpenConns: 20
//   - MaxIdleConns: 5
//   - ConnMaxLifetime: 5 minutes
//   - ConnMaxIdleTime: 10 minutes
//
// Notes:
//   - database/sql treats MaxOpenConns=0 as "unlimited"
//   - database/sql treats MaxIdleConns=0 as "no idle connections"
func NormalizeConnectionPoolSettings(maxOpenConns, maxIdleConns int, connMaxLifetime, connMaxIdleTime time.Duration) (int, int, time.Duration, time.Duration) {
	// Set defaults if not provided
	if maxOpenConns == 0 {
		maxOpenConns = 20
	}
	if maxIdleConns == 0 {
		maxIdleConns = 5
	}
	if connMaxLifetime == 0 {
		connMaxLifetime = 5 * time.Minute
	}
	if connMaxIdleTime == 0 {
		connMaxIdleTime = 10 * time.Minute
	}

	// Ensure MaxIdleConns doesn't exceed MaxOpenConns
	if maxIdleConns > maxOpenConns {
		maxIdleConns = maxOpenConns
	}

	return maxOpenConns, maxIdleConns, connMaxLifetime, connMaxIdleTime
}

// calculateSafeBatchSize computes the optimal batch size for bulk inserts to avoid
// PostgreSQL's "extended protocol limited to 65535 parameters" error.
//
// PostgreSQL's extended protocol has a hard limit of 65535 parameters per query.
// When doing batch inserts with GORM, each record consumes multiple parameters
// (one per field being inserted), and ON CONFLICT clauses may add additional parameters.
//
// Parameters:
//   - totalRecords: total number of records to insert
//   - fieldsPerRecord: number of fields/parameters per record
//
// Returns the safe batch size that won't exceed the parameter limit.
//
// Example with headroom of 1000:
//   - Balance struct: 3 fields → (65,535 - 1,000) / 3 = 21,511 records/batch
//   - ProvenanceEvent struct: 11 fields → (65,535 - 1,000) / 11 = 5,866 records/batch
//   - ChangesJournal struct: 4 fields → (65,535 - 1,000) / 4 = 16,133 records/batch
//
// The function uses a total headroom to account for batch-level overhead:
//   - GORM-added timestamp fields (created_at, updated_at) across all records
//   - ON CONFLICT clause parameters (can be significant with multi-column conflicts)
//   - Query metadata and internal GORM bookkeeping
//
// Total headroom is more accurate than per-record overhead because some costs
// are fixed per batch, not scaled per record.
func calculateSafeBatchSize(totalRecords int, fieldsPerRecord int) int {
	const maxParams = 65535
	const totalHeadroom = 1000 // Total parameter headroom for batch-level overhead

	// Reserve headroom from total available parameters
	availableParams := maxParams - totalHeadroom
	safeBatchSize := max(availableParams/fieldsPerRecord, 1)

	if safeBatchSize > totalRecords {
		return totalRecords
	}

	return safeBatchSize
}

// GetTokenByID retrieves a token by its internal ID
func (s *pgStore) GetTokenByID(ctx context.Context, tokenID uint64) (*schema.Token, error) {
	var token schema.Token
	err := s.db.WithContext(ctx).Where("id = ?", tokenID).First(&token).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get token: %w", err)
	}
	return &token, nil
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

// GetTokensByCIDs retrieves multiple tokens by their canonical IDs
func (s *pgStore) GetTokensByCIDs(ctx context.Context, tokenCIDs []string) ([]*schema.Token, error) {
	if len(tokenCIDs) == 0 {
		return []*schema.Token{}, nil
	}

	var tokens []*schema.Token
	err := s.db.WithContext(ctx).
		Where("token_cid IN ?", tokenCIDs).
		Find(&tokens).Error
	if err != nil {
		return nil, fmt.Errorf("failed to get tokens by CIDs: %w", err)
	}

	return tokens, nil
}

// GetTokensByIDs retrieves multiple tokens by their internal IDs
func (s *pgStore) GetTokensByIDs(ctx context.Context, tokenIDs []uint64) ([]*schema.Token, error) {
	if len(tokenIDs) == 0 {
		return []*schema.Token{}, nil
	}

	var tokens []*schema.Token
	err := s.db.WithContext(ctx).
		Where("id IN ?", tokenIDs).
		Find(&tokens).Error
	if err != nil {
		return nil, fmt.Errorf("failed to get tokens by IDs: %w", err)
	}

	return tokens, nil
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
		// 1. Create or get the token (handle multi-edition tokens like FA2/ERC1155)
		token := schema.Token{
			TokenCID:        input.Token.TokenCID,
			Chain:           input.Token.Chain,
			Standard:        input.Token.Standard,
			ContractAddress: input.Token.ContractAddress,
			TokenNumber:     input.Token.TokenNumber,
			CurrentOwner:    input.Token.CurrentOwner,
			Burned:          input.Token.Burned,
		}

		// Use ON CONFLICT DO NOTHING for token_cid (unique constraint)
		// This allows subsequent mints for FA2/ERC1155 tokens
		if err := tx.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "token_cid"}},
			DoNothing: true,
		}).Clauses(clause.Returning{Columns: []clause.Column{}}).
			Create(&token).Error; err != nil {
			return fmt.Errorf("failed to create token: %w", err)
		}

		// If token.ID is 0, it means the token already existed, so fetch it
		if token.ID == 0 {
			if err := tx.Where("token_cid = ?", input.Token.TokenCID).First(&token).Error; err != nil {
				return fmt.Errorf("failed to get existing token: %w", err)
			}
		}

		// 2. Upsert the balance record (create or update)
		balance := schema.Balance{
			TokenID:      token.ID,
			OwnerAddress: input.Balance.OwnerAddress,
			Quantity:     input.Balance.Quantity,
		}
		// Use ON CONFLICT DO UPDATE to update the quantity if balance already exists
		if err := tx.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "token_id"}, {Name: "owner_address"}},
			DoUpdates: clause.AssignmentColumns([]string{"quantity"}),
		}).Create(&balance).Error; err != nil {
			return fmt.Errorf("failed to upsert balance: %w", err)
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

		// Use ON CONFLICT DO NOTHING to skip duplicates based on comprehensive unique index
		// (chain, tx_hash, token_id, from_address, to_address, event_type)
		if err := tx.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "chain"}, {Name: "tx_hash"}, {Name: "token_id"}, {Name: "from_address"}, {Name: "to_address"}, {Name: "event_type"}},
			DoNothing: true,
		}).Clauses(clause.Returning{Columns: []clause.Column{}}).
			Create(&provenanceEvent).Error; err != nil {
			return fmt.Errorf("failed to create provenance event: %w", err)
		}

		// If the event was a duplicate (ID == 0), just return success
		// This is expected for re-processing the same transaction
		if provenanceEvent.ID == 0 {
			return nil
		}

		// 4. Update ownership periods based on the provenance event
		if err := s.updateOwnershipPeriods(ctx, tx, &provenanceEvent, input.Token.Standard); err != nil {
			return fmt.Errorf("failed to update ownership periods: %w", err)
		}

		// 5. Create the change journal entry
		// For mint events: subject_type = 'token', subject_id = provenance_event_id
		// Populate meta with provenance information
		meta := schema.ProvenanceChangeMeta{
			TokenID:     token.ID,
			Chain:       token.Chain,
			Standard:    token.Standard,
			Contract:    token.ContractAddress,
			TokenNumber: token.TokenNumber,
			From:        input.ProvenanceEvent.FromAddress,
			To:          input.ProvenanceEvent.ToAddress,
			Quantity:    input.ProvenanceEvent.Quantity,
			TxHash:      input.ProvenanceEvent.TxHash,
		}
		metaJSON, err := json.Marshal(meta)
		if err != nil {
			return fmt.Errorf("failed to marshal change journal meta: %w", err)
		}

		changeJournal := schema.ChangesJournal{
			SubjectType: schema.SubjectTypeToken,
			SubjectID:   fmt.Sprintf("%d", provenanceEvent.ID),
			ChangedAt:   input.ProvenanceEvent.Timestamp,
			Meta:        metaJSON,
		}

		if err := tx.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "subject_type"}, {Name: "subject_id"}, {Name: "changed_at"}},
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
	query := s.db.WithContext(ctx).Model(&schema.Token{}).
		Select("DISTINCT ON (tokens.id, latest_pe.timestamp) tokens.*")

	// By default, exclude tokens without metadata AND enrichment (broken metadata)
	// A token is broken only if it has NEITHER metadata NOR enrichment
	// Unless IncludeBroken is explicitly set to true
	if !filter.IncludeBroken {
		query = query.
			Joins("LEFT JOIN token_metadata ON tokens.id = token_metadata.token_id").
			Joins("LEFT JOIN enrichment_sources ON tokens.id = enrichment_sources.token_id").
			Where("token_metadata.token_id IS NOT NULL OR enrichment_sources.token_id IS NOT NULL")
	}

	// By default, exclude tokens with broken media URLs
	// Unless IncludeBrokenMedia is explicitly set to true
	// A token's media is broken if:
	//   - All animation URLs (if any exist) are broken, OR
	//   - No animation URLs exist AND all image URLs (if any exist) are broken
	//   - Token has no media URLs at all
	if !filter.IncludeBrokenMedia {
		query = query.Where(`
			-- Include token if it has at least one healthy animation URL
			EXISTS (
				SELECT 1 FROM token_media_health tmh
				WHERE tmh.token_id = tokens.id
					AND tmh.health_status = ?
					AND tmh.media_source IN ?
			)
			OR
			-- OR if no animation URLs exist AND has at least one healthy image URL
			(
				NOT EXISTS (
					SELECT 1 FROM token_media_health tmh
					WHERE tmh.token_id = tokens.id AND tmh.media_source IN ?
				)
				AND EXISTS (
					SELECT 1 FROM token_media_health tmh
					WHERE tmh.token_id = tokens.id
						AND tmh.health_status = ?
						AND tmh.media_source IN ?
				)
			)
		`, schema.MediaHealthStatusHealthy,
			[]schema.MediaHealthSource{schema.MediaHealthSourceMetadataAnimation, schema.MediaHealthSourceEnrichmentAnimation},
			[]schema.MediaHealthSource{schema.MediaHealthSourceMetadataAnimation, schema.MediaHealthSourceEnrichmentAnimation},
			schema.MediaHealthStatusHealthy,
			[]schema.MediaHealthSource{schema.MediaHealthSourceMetadataImage, schema.MediaHealthSourceEnrichmentImage})
	}

	// Apply filters
	if len(filter.Owners) > 0 {
		// Filter by current owners only (balances or current_owner)
		query = query.
			Joins("LEFT JOIN balances ON balances.token_id = tokens.id").
			Where("balances.owner_address IN ? OR tokens.current_owner IN ?", filter.Owners, filter.Owners)
	}

	if len(filter.TokenIDs) > 0 {
		query = query.Where("tokens.id IN ?", filter.TokenIDs)
	}

	if len(filter.Chains) > 0 {
		query = query.Where("tokens.chain IN ?", filter.Chains)
	}

	if len(filter.ContractAddresses) > 0 {
		query = query.Where("tokens.contract_address IN ?", filter.ContractAddresses)
	}

	if len(filter.TokenNumbers) > 0 {
		query = query.Where("tokens.token_number IN ?", filter.TokenNumbers)
	}

	if len(filter.TokenCIDs) > 0 {
		query = query.Where("tokens.token_cid IN ?", filter.TokenCIDs)
	}

	// Count total before pagination
	var total int64
	countQuery := query
	if err := countQuery.Count(&total).Error; err != nil {
		return nil, 0, fmt.Errorf("failed to count tokens: %w", err)
	}

	// Join with latest provenance event to sort by timestamp
	// When filtering by owners, only consider provenance events related to those owners
	if len(filter.Owners) > 0 {
		query = query.Joins(`LEFT JOIN LATERAL (
			SELECT timestamp
			FROM provenance_events
			WHERE provenance_events.token_id = tokens.id
			AND (provenance_events.from_address IN ? OR provenance_events.to_address IN ?)
			ORDER BY timestamp DESC, (raw->>'tx_index')::bigint DESC
			LIMIT 1
		) latest_pe ON true`, filter.Owners, filter.Owners)
	} else {
		query = query.Joins(`LEFT JOIN LATERAL (
			SELECT timestamp
			FROM provenance_events
			WHERE provenance_events.token_id = tokens.id
			ORDER BY timestamp DESC, (raw->>'tx_index')::bigint DESC
			LIMIT 1
		) latest_pe ON true`)
	}

	// Apply pagination with sorting by latest provenance event timestamp descending
	query = query.Order("latest_pe.timestamp DESC NULLS LAST").Order("tokens.id DESC").Limit(filter.Limit).Offset(int(filter.Offset)) //nolint:gosec,G115

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
func (s *pgStore) GetTokenOwners(ctx context.Context, tokenID uint64, limit int, offset uint64) ([]schema.Balance, uint64, error) {
	query := s.db.WithContext(ctx).Model(&schema.Balance{}).Where("token_id = ?", tokenID)

	// Count total
	var total int64
	if err := query.Count(&total).Error; err != nil {
		return nil, 0, fmt.Errorf("failed to count owners: %w", err)
	}

	// Apply pagination
	query = query.Order("id ASC").Limit(limit).Offset(int(offset)) //nolint:gosec,G115

	var balances []schema.Balance
	if err := query.Find(&balances).Error; err != nil {
		return nil, 0, fmt.Errorf("failed to get owners: %w", err)
	}

	return balances, uint64(total), nil //nolint:gosec,G115
}

// GetTokenOwnersBulk retrieves owners (balances) for multiple tokens
func (s *pgStore) GetTokenOwnersBulk(ctx context.Context, tokenIDs []uint64, limit int) (map[uint64][]schema.Balance, map[uint64]uint64, error) {
	if len(tokenIDs) == 0 {
		return make(map[uint64][]schema.Balance), make(map[uint64]uint64), nil
	}

	// Use a window function to limit results per token and get total counts
	type balanceWithTotal struct {
		schema.Balance
		TotalCount uint64 `gorm:"column:total_count"`
	}

	var balancesWithTotals []balanceWithTotal
	err := s.db.WithContext(ctx).Raw(`
		SELECT * FROM (
			SELECT 
				*, 
				ROW_NUMBER() OVER (PARTITION BY token_id ORDER BY id ASC) as rn,
				COUNT(*) OVER (PARTITION BY token_id) as total_count
			FROM balances
			WHERE token_id IN ?
		) sub
		WHERE rn <= ?
	`, tokenIDs, limit).Scan(&balancesWithTotals).Error

	if err != nil {
		return nil, nil, fmt.Errorf("failed to get owners bulk: %w", err)
	}

	// Group by token ID and collect totals
	result := make(map[uint64][]schema.Balance)
	totals := make(map[uint64]uint64)
	for _, bwt := range balancesWithTotals {
		result[bwt.TokenID] = append(result[bwt.TokenID], bwt.Balance)
		totals[bwt.TokenID] = bwt.TotalCount
	}

	return result, totals, nil
}

// GetTokenProvenanceEvents retrieves provenance events for a token
func (s *pgStore) GetTokenProvenanceEvents(ctx context.Context, tokenID uint64, limit int, offset uint64, orderDesc bool) ([]schema.ProvenanceEvent, uint64, error) {
	query := s.db.WithContext(ctx).Model(&schema.ProvenanceEvent{}).Where("token_id = ?", tokenID)

	// Count total
	var total int64
	if err := query.Count(&total).Error; err != nil {
		return nil, 0, fmt.Errorf("failed to count provenance events: %w", err)
	}

	// Apply ordering by timestamp (with tx_index as tiebreaker)
	// Use bigint to avoid overflow with large transaction indexes
	if orderDesc {
		query = query.Order("timestamp DESC, (raw->>'tx_index')::bigint DESC")
	} else {
		query = query.Order("timestamp ASC, (raw->>'tx_index')::bigint ASC")
	}

	// Apply pagination
	query = query.Limit(limit).Offset(int(offset)) //nolint:gosec,G115

	var events []schema.ProvenanceEvent
	if err := query.Find(&events).Error; err != nil {
		return nil, 0, fmt.Errorf("failed to get provenance events: %w", err)
	}

	return events, uint64(total), nil //nolint:gosec,G115
}

// GetTokenProvenanceEventsBulk retrieves provenance events for multiple tokens
func (s *pgStore) GetTokenProvenanceEventsBulk(ctx context.Context, tokenIDs []uint64, limit int) (map[uint64][]schema.ProvenanceEvent, map[uint64]uint64, error) {
	if len(tokenIDs) == 0 {
		return make(map[uint64][]schema.ProvenanceEvent), make(map[uint64]uint64), nil
	}

	// Use a window function to limit results per token and get total counts, ordered by timestamp DESC (most recent first)
	type eventWithTotal struct {
		schema.ProvenanceEvent
		TotalCount uint64 `gorm:"column:total_count"`
	}

	var eventsWithTotals []eventWithTotal
	err := s.db.WithContext(ctx).Raw(`
		SELECT * FROM (
			SELECT 
				*, 
				ROW_NUMBER() OVER (PARTITION BY token_id ORDER BY timestamp DESC, (raw->>'tx_index')::bigint DESC) as rn,
				COUNT(*) OVER (PARTITION BY token_id) as total_count
			FROM provenance_events
			WHERE token_id IN ?
		) sub
		WHERE rn <= ?
		ORDER BY timestamp DESC, (raw->>'tx_index')::bigint DESC
	`, tokenIDs, limit).Scan(&eventsWithTotals).Error

	if err != nil {
		return nil, nil, fmt.Errorf("failed to get provenance events bulk: %w", err)
	}

	// Group by token ID and collect totals
	result := make(map[uint64][]schema.ProvenanceEvent)
	totals := make(map[uint64]uint64)
	for _, ewt := range eventsWithTotals {
		result[ewt.TokenID] = append(result[ewt.TokenID], ewt.ProvenanceEvent)
		totals[ewt.TokenID] = ewt.TotalCount
	}

	return result, totals, nil
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
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// 1. Get the old metadata with row-level lock (if it exists)
		// Use SELECT ... FOR UPDATE to prevent concurrent updates
		var oldMetadata *schema.TokenMetadata
		err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("token_id = ?", input.TokenID).
			First(&oldMetadata).Error
		if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
			return fmt.Errorf("failed to lock old metadata: %w", err)
		}

		// 2. Upsert the metadata
		metadata := schema.TokenMetadata{
			TokenID:         input.TokenID,
			OriginJSON:      input.OriginJSON,
			LatestJSON:      input.LatestJSON,
			LatestHash:      input.LatestHash,
			EnrichmentLevel: input.EnrichmentLevel,
			LastRefreshedAt: &input.LastRefreshedAt,
			ImageURL:        input.ImageURL,
			AnimationURL:    input.AnimationURL,
			Name:            input.Name,
			Artists:         input.Artists,
			Description:     input.Description,
			Publisher:       input.Publisher,
			MimeType:        input.MimeType,
		}

		err = tx.Save(&metadata).Error
		if err != nil {
			return fmt.Errorf("failed to upsert token metadata: %w", err)
		}

		// 3. Create or update the change journal entry with both old and new metadata
		// subject_id = token_id (which is the PK of token_metadata table)
		// Build the meta with old (optional) and new (required) metadata fields
		metaChanges := schema.MetadataChangeMeta{
			TokenID: input.TokenID,
			New: schema.MetadataFields{
				AnimationURL: metadata.AnimationURL,
				ImageURL:     metadata.ImageURL,
				Artists:      metadata.Artists,
				Publisher:    metadata.Publisher,
				Name:         metadata.Name,
				Description:  metadata.Description,
				MimeType:     metadata.MimeType,
			},
		}

		// Add old metadata if it existed
		if oldMetadata != nil {
			metaChanges.Old = schema.MetadataFields{
				AnimationURL: oldMetadata.AnimationURL,
				ImageURL:     oldMetadata.ImageURL,
				Artists:      oldMetadata.Artists,
				Publisher:    oldMetadata.Publisher,
				Name:         oldMetadata.Name,
				Description:  oldMetadata.Description,
				MimeType:     oldMetadata.MimeType,
			}
		}

		metaJSON, err := json.Marshal(metaChanges)
		if err != nil {
			return fmt.Errorf("failed to marshal change journal meta: %w", err)
		}

		changeJournal := schema.ChangesJournal{
			SubjectType: schema.SubjectTypeMetadata,
			SubjectID:   fmt.Sprintf("%d", input.TokenID), // token_metadata.token_id (PK)
			ChangedAt:   input.LastRefreshedAt,
			Meta:        metaJSON,
		}

		// Use ON CONFLICT DO NOTHING to skip duplicates based on unique constraint
		// This allows tracking multiple metadata changes over time (changed_at is part of the unique key)
		if err := tx.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "subject_type"}, {Name: "subject_id"}, {Name: "changed_at"}},
			DoNothing: true,
		}).
			Clauses(clause.Returning{Columns: []clause.Column{}}).
			Create(&changeJournal).Error; err != nil {
			return fmt.Errorf("failed to create change journal: %w", err)
		}

		// 4. Sync media health records
		var oldImageURL, oldAnimationURL *string
		if oldMetadata != nil {
			oldImageURL = oldMetadata.ImageURL
			oldAnimationURL = oldMetadata.AnimationURL
		}
		// Handle image URL changes
		if err := s.syncSingleMediaURL(tx, input.TokenID, oldImageURL, input.ImageURL, schema.MediaHealthSourceMetadataImage); err != nil {
			return fmt.Errorf("failed to sync image URL: %w", err)
		}

		// Handle animation URL changes
		if err := s.syncSingleMediaURL(tx, input.TokenID, oldAnimationURL, input.AnimationURL, schema.MediaHealthSourceMetadataAnimation); err != nil {
			return fmt.Errorf("failed to sync animation URL: %w", err)
		}

		return nil
	})
}

// GetEnrichmentSourceByTokenID retrieves an enrichment source by token ID
func (s *pgStore) GetEnrichmentSourceByTokenID(ctx context.Context, tokenID uint64) (*schema.EnrichmentSource, error) {
	var enrichmentSource schema.EnrichmentSource
	err := s.db.WithContext(ctx).Where("token_id = ?", tokenID).First(&enrichmentSource).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get enrichment source: %w", err)
	}
	return &enrichmentSource, nil
}

// GetEnrichmentSourcesByTokenIDs retrieves enrichment sources for multiple tokens
func (s *pgStore) GetEnrichmentSourcesByTokenIDs(ctx context.Context, tokenIDs []uint64) (map[uint64]*schema.EnrichmentSource, error) {
	if len(tokenIDs) == 0 {
		return make(map[uint64]*schema.EnrichmentSource), nil
	}

	var enrichmentSources []schema.EnrichmentSource
	err := s.db.WithContext(ctx).Where("token_id IN ?", tokenIDs).Find(&enrichmentSources).Error
	if err != nil {
		return nil, fmt.Errorf("failed to get enrichment sources bulk: %w", err)
	}

	// Map by token ID
	result := make(map[uint64]*schema.EnrichmentSource)
	for i := range enrichmentSources {
		result[enrichmentSources[i].TokenID] = &enrichmentSources[i]
	}

	return result, nil
}

// GetEnrichmentSourceByTokenCID retrieves an enrichment source by token CID
func (s *pgStore) GetEnrichmentSourceByTokenCID(ctx context.Context, tokenCID string) (*schema.EnrichmentSource, error) {
	var enrichmentSource schema.EnrichmentSource
	err := s.db.WithContext(ctx).
		Joins("JOIN tokens ON tokens.id = enrichment_sources.token_id").
		Where("tokens.token_cid = ?", tokenCID).
		First(&enrichmentSource).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get enrichment source: %w", err)
	}
	return &enrichmentSource, nil
}

// UpsertEnrichmentSource creates or updates an enrichment source and updates enrichment_level in token_metadata
func (s *pgStore) UpsertEnrichmentSource(ctx context.Context, input CreateEnrichmentSourceInput) error {
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// 1. Get the old enrichment source (if it exists) for change tracking
		var oldEnrichmentSource *schema.EnrichmentSource
		err := tx.Where("token_id = ?", input.TokenID).First(&oldEnrichmentSource).Error
		if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
			return fmt.Errorf("failed to get old enrichment source: %w", err)
		}

		// 2. Upsert enrichment source
		enrichmentSource := schema.EnrichmentSource{
			TokenID:      input.TokenID,
			Vendor:       input.Vendor,
			VendorJSON:   input.VendorJSON,
			VendorHash:   input.VendorHash,
			ImageURL:     input.ImageURL,
			AnimationURL: input.AnimationURL,
			Name:         input.Name,
			Description:  input.Description,
			Artists:      input.Artists,
			MimeType:     input.MimeType,
		}

		if err := tx.Save(&enrichmentSource).Error; err != nil {
			return fmt.Errorf("failed to upsert enrichment source: %w", err)
		}

		// 3. Update enrichment_level in token_metadata to 'vendor'
		if err := tx.Model(&schema.TokenMetadata{}).
			Where("token_id = ?", input.TokenID).
			Update("enrichment_level", schema.EnrichmentLevelVendor).Error; err != nil {
			return fmt.Errorf("failed to update enrichment level: %w", err)
		}

		// 4. Create change journal entry
		metaChanges := schema.EnrichmentSourceChangeMeta{
			TokenID: input.TokenID,
			New: schema.EnrichmentSourceFields{
				Vendor:       string(enrichmentSource.Vendor),
				VendorHash:   enrichmentSource.VendorHash,
				AnimationURL: enrichmentSource.AnimationURL,
				ImageURL:     enrichmentSource.ImageURL,
				Name:         enrichmentSource.Name,
				Description:  enrichmentSource.Description,
				Artists:      enrichmentSource.Artists,
				MimeType:     enrichmentSource.MimeType,
			},
		}

		// Add old enrichment source if it existed
		if oldEnrichmentSource != nil {
			metaChanges.Old = schema.EnrichmentSourceFields{
				Vendor:       string(oldEnrichmentSource.Vendor),
				VendorHash:   oldEnrichmentSource.VendorHash,
				AnimationURL: oldEnrichmentSource.AnimationURL,
				ImageURL:     oldEnrichmentSource.ImageURL,
				Name:         oldEnrichmentSource.Name,
				Description:  oldEnrichmentSource.Description,
				Artists:      oldEnrichmentSource.Artists,
				MimeType:     oldEnrichmentSource.MimeType,
			}
		}

		metaJSON, err := json.Marshal(metaChanges)
		if err != nil {
			return fmt.Errorf("failed to marshal change journal meta: %w", err)
		}

		changeJournal := schema.ChangesJournal{
			SubjectType: schema.SubjectTypeEnrichSource,
			SubjectID:   fmt.Sprintf("%d", input.TokenID), // enrichment_sources.token_id (PK)
			ChangedAt:   time.Now(),
			Meta:        metaJSON,
		}

		// Use ON CONFLICT DO NOTHING to skip duplicates based on unique constraint
		if err := tx.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "subject_type"}, {Name: "subject_id"}, {Name: "changed_at"}},
			DoNothing: true,
		}).
			Clauses(clause.Returning{Columns: []clause.Column{}}).
			Create(&changeJournal).Error; err != nil {
			return fmt.Errorf("failed to create change journal: %w", err)
		}

		// 5. Sync media health records
		var oldImageURL, oldAnimationURL *string
		if oldEnrichmentSource != nil {
			oldImageURL = oldEnrichmentSource.ImageURL
			oldAnimationURL = oldEnrichmentSource.AnimationURL
		}

		// Handle image URL changes
		if err := s.syncSingleMediaURL(tx, input.TokenID, oldImageURL, input.ImageURL, schema.MediaHealthSourceEnrichmentImage); err != nil {
			return fmt.Errorf("failed to sync image URL: %w", err)
		}

		// Handle animation URL changes
		if err := s.syncSingleMediaURL(tx, input.TokenID, oldAnimationURL, input.AnimationURL, schema.MediaHealthSourceEnrichmentAnimation); err != nil {
			return fmt.Errorf("failed to sync animation URL: %w", err)
		}

		return nil
	})
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

		// 2. Update the token: set burned = true, current_owner = nil
		token.Burned = true
		token.CurrentOwner = nil

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
						logger.WarnCtx(ctx, "Sender balance not found", zap.String("token_cid", token.TokenCID), zap.String("owner_address", input.SenderBalanceUpdate.OwnerAddress))
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

		// Use ON CONFLICT DO NOTHING to skip duplicates based on comprehensive unique index
		// (chain, tx_hash, token_id, from_address, to_address, event_type)
		if err := tx.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "chain"}, {Name: "tx_hash"}, {Name: "token_id"}, {Name: "from_address"}, {Name: "to_address"}, {Name: "event_type"}},
			DoNothing: true,
		}).Clauses(clause.Returning{Columns: []clause.Column{}}).
			Create(&provenanceEvent).Error; err != nil {
			return fmt.Errorf("failed to create provenance event: %w", err)
		}

		// If the event was a duplicate (ID == 0), rollback the transaction
		// This prevents linking multiple operations to the same provenance event
		if provenanceEvent.ID == 0 {
			return fmt.Errorf("duplicate provenance event detected for chain=%s, tx_hash=%s",
				input.ProvenanceEvent.Chain, input.ProvenanceEvent.TxHash)
		}

		// 5. Update ownership periods based on the provenance event
		if err := s.updateOwnershipPeriods(ctx, tx, &provenanceEvent, token.Standard); err != nil {
			return fmt.Errorf("failed to update ownership periods: %w", err)
		}

		// 6. Create the change journal entry
		// For burn events: subject_type = 'token', subject_id = provenance_event_id
		// Populate meta with provenance information
		meta := schema.ProvenanceChangeMeta{
			TokenID:     token.ID,
			Chain:       token.Chain,
			Standard:    token.Standard,
			Contract:    token.ContractAddress,
			TokenNumber: token.TokenNumber,
			From:        input.ProvenanceEvent.FromAddress,
			To:          input.ProvenanceEvent.ToAddress,
			Quantity:    input.ProvenanceEvent.Quantity,
			TxHash:      input.ProvenanceEvent.TxHash,
		}
		metaJSON, err := json.Marshal(meta)
		if err != nil {
			return fmt.Errorf("failed to marshal change journal meta: %w", err)
		}

		changeJournal := schema.ChangesJournal{
			SubjectType: schema.SubjectTypeToken,
			SubjectID:   fmt.Sprintf("%d", provenanceEvent.ID),
			ChangedAt:   input.ProvenanceEvent.Timestamp,
			Meta:        metaJSON,
		}

		if err := tx.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "subject_type"}, {Name: "subject_id"}, {Name: "changed_at"}},
			DoNothing: true,
		}).
			Clauses(clause.Returning{Columns: []clause.Column{}}).
			Create(&changeJournal).Error; err != nil {
			return fmt.Errorf("failed to create change journal: %w", err)
		}

		return nil
	})
}

// CreateMetadataUpdate creates a provenance event for a metadata update
// Note: The change journal entry is created separately in UpsertTokenMetadata where we have access to the actual metadata changes
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

		// Use ON CONFLICT DO NOTHING to skip duplicates based on comprehensive unique index
		// (chain, tx_hash, token_id, from_address, to_address, event_type)
		if err := tx.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "chain"}, {Name: "tx_hash"}, {Name: "token_id"}, {Name: "from_address"}, {Name: "to_address"}, {Name: "event_type"}},
			DoNothing: true,
		}).Clauses(clause.Returning{Columns: []clause.Column{}}).
			Create(&provenanceEvent).Error; err != nil {
			return fmt.Errorf("failed to create provenance event: %w", err)
		}

		// If the event was a duplicate (ID == 0), rollback the transaction
		// This prevents linking multiple operations to the same provenance event
		if provenanceEvent.ID == 0 {
			return fmt.Errorf("duplicate provenance event detected for chain=%s, tx_hash=%s",
				input.ProvenanceEvent.Chain, input.ProvenanceEvent.TxHash)
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
						logger.WarnCtx(ctx, "Sender balance not found", zap.String("token_cid", token.TokenCID), zap.String("owner_address", input.SenderBalanceUpdate.OwnerAddress))
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

		// Use ON CONFLICT DO NOTHING to skip duplicates based on comprehensive unique index
		// (chain, tx_hash, token_id, from_address, to_address, event_type)
		if err := tx.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "chain"}, {Name: "tx_hash"}, {Name: "token_id"}, {Name: "from_address"}, {Name: "to_address"}, {Name: "event_type"}},
			DoNothing: true,
		}).Clauses(clause.Returning{Columns: []clause.Column{}}).
			Create(&provenanceEvent).Error; err != nil {
			return fmt.Errorf("failed to create provenance event: %w", err)
		}

		// If the event was a duplicate (ID == 0), rollback the transaction
		// This prevents linking multiple operations to the same provenance event
		if provenanceEvent.ID == 0 {
			return fmt.Errorf("duplicate provenance event detected for chain=%s, tx_hash=%s",
				input.ProvenanceEvent.Chain, input.ProvenanceEvent.TxHash)
		}

		// 6. Update ownership periods based on the provenance event
		if err := s.updateOwnershipPeriods(ctx, tx, &provenanceEvent, token.Standard); err != nil {
			return fmt.Errorf("failed to update ownership periods: %w", err)
		}

		// 7. Create the change journal entry
		// For transfer events: subject_type depends on token standard
		// - ERC721 (single token): subject_type = 'owner'
		// - ERC1155/FA2 (multi-token): subject_type = 'balance'
		// Populate meta with provenance information
		meta := schema.ProvenanceChangeMeta{
			TokenID:     token.ID,
			Chain:       token.Chain,
			Standard:    token.Standard,
			Contract:    token.ContractAddress,
			TokenNumber: token.TokenNumber,
			From:        input.ProvenanceEvent.FromAddress,
			To:          input.ProvenanceEvent.ToAddress,
			Quantity:    input.ProvenanceEvent.Quantity,
			TxHash:      input.ProvenanceEvent.TxHash,
		}
		metaJSON, err := json.Marshal(meta)
		if err != nil {
			return fmt.Errorf("failed to marshal change journal meta: %w", err)
		}

		subjectType := types.ProvenanceEventTypeToSubjectType(input.ProvenanceEvent.EventType, token.Standard)
		changeJournal := schema.ChangesJournal{
			SubjectType: subjectType,
			SubjectID:   fmt.Sprintf("%d", provenanceEvent.ID),
			ChangedAt:   input.ProvenanceEvent.Timestamp,
			Meta:        metaJSON,
		}

		if err := tx.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "subject_type"}, {Name: "subject_id"}, {Name: "changed_at"}},
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
			TokenCID:        input.Token.TokenCID,
			Chain:           input.Token.Chain,
			Standard:        input.Token.Standard,
			ContractAddress: input.Token.ContractAddress,
			TokenNumber:     input.Token.TokenNumber,
			CurrentOwner:    input.Token.CurrentOwner,
			Burned:          input.Token.Burned,
		}

		// Use ON CONFLICT to update if token already exists
		if err := tx.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "token_cid"}},
			DoUpdates: clause.AssignmentColumns([]string{"current_owner", "burned"}),
		}).Clauses(clause.Returning{}).Create(&token).Error; err != nil {
			return fmt.Errorf("failed to upsert token: %w", err)
		}

		// 2. Delete existing related records to ensure data freshness
		// Delete changes_journal entries first
		// Only delete journal entries related to provenance data (token, owner, balance)
		// Other journal types (e.g., metadata, enrichment_sources, media_assets) should be preserved as they may not be tied to on-chain events
		if len(input.Events) > 0 {
			// First, get existing provenance event IDs
			var existingProvenances []schema.ProvenanceEvent
			if err := tx.Where("token_id = ?", token.ID).Find(&existingProvenances).Error; err != nil {
				return fmt.Errorf("failed to get existing provenance events: %w", err)
			}

			// Delete changes_journal entries linked to these provenance events
			if len(existingProvenances) > 0 {
				existingEventIDs := make([]string, len(existingProvenances))
				for i, pe := range existingProvenances {
					existingEventIDs[i] = strconv.FormatUint(pe.ID, 10)
				}

				// subject_id contains the provenance_event_id for token/owner/balance types
				if err := tx.Where("subject_type IN ? AND subject_id IN ?",
					[]schema.SubjectType{schema.SubjectTypeToken, schema.SubjectTypeOwner, schema.SubjectTypeBalance},
					existingEventIDs).
					Delete(&schema.ChangesJournal{}).Error; err != nil {
					return fmt.Errorf("failed to delete existing changes_journal entries: %w", err)
				}
			}

			// Delete existing provenance events
			if err := tx.Where("token_id = ?", token.ID).Delete(&schema.ProvenanceEvent{}).Error; err != nil {
				return fmt.Errorf("failed to delete existing provenance events: %w", err)
			}

			// Delete existing ownership periods since we're re-creating all provenance data
			if err := tx.Where("token_id = ?", token.ID).Delete(&schema.TokenOwnershipPeriod{}).Error; err != nil {
				return fmt.Errorf("failed to delete existing ownership periods: %w", err)
			}
		}

		if len(input.Balances) > 0 {
			// Delete existing balances
			if err := tx.Where("token_id = ?", token.ID).Delete(&schema.Balance{}).Error; err != nil {
				return fmt.Errorf("failed to delete existing balances: %w", err)
			}
		}

		// 3. Insert all balances in batch
		if len(input.Balances) > 0 {
			balances := make([]schema.Balance, 0, len(input.Balances))
			for _, balanceInput := range input.Balances {
				balances = append(balances, schema.Balance{
					TokenID:      token.ID,
					OwnerAddress: balanceInput.OwnerAddress,
					Quantity:     balanceInput.Quantity,
				})
			}

			// Balance has 3 fields: token_id, owner_address, quantity
			batchSize := calculateSafeBatchSize(len(balances), 3)

			if err := tx.Clauses(clause.OnConflict{
				Columns:   []clause.Column{{Name: "token_id"}, {Name: "owner_address"}},
				DoUpdates: clause.AssignmentColumns([]string{"quantity"}),
			}).CreateInBatches(balances, batchSize).Error; err != nil {
				return fmt.Errorf("failed to create balances: %w", err)
			}
		}

		// 4. Batch insert all provenance events
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

			// ProvenanceEvent has 11 fields: token_id, chain, event_type, from_address, to_address,
			// quantity, tx_hash, block_number, block_hash, timestamp, raw
			batchSize := calculateSafeBatchSize(len(provenanceEvents), 11)

			// Use ON CONFLICT DO NOTHING to skip duplicates based on comprehensive unique index
			// (chain, tx_hash, token_id, from_address, to_address, event_type)
			if err := tx.Clauses(clause.OnConflict{
				Columns:   []clause.Column{{Name: "chain"}, {Name: "tx_hash"}, {Name: "token_id"}, {Name: "from_address"}, {Name: "to_address"}, {Name: "event_type"}},
				DoNothing: true,
			}).CreateInBatches(provenanceEvents, batchSize).Error; err != nil {
				return fmt.Errorf("failed to create provenance events: %w", err)
			}

			// 5. Update ownership periods for all provenance events
			for i := range provenanceEvents {
				// Skip events that weren't inserted (due to ON CONFLICT DO NOTHING)
				if provenanceEvents[i].ID == 0 {
					continue
				}

				if err := s.updateOwnershipPeriods(ctx, tx, &provenanceEvents[i], token.Standard); err != nil {
					return fmt.Errorf("failed to update ownership periods for event %d: %w", i, err)
				}
			}

			// 6. Batch insert changes_journal entries for all events
			changeJournals := make([]schema.ChangesJournal, 0, len(provenanceEvents))
			for _, evt := range provenanceEvents {
				// Skip events that weren't inserted (due to ON CONFLICT DO NOTHING)
				// These event could have ID == 0 and already have change journals from when they were first inserted
				if evt.ID == 0 {
					continue
				}

				// Skip events that are not on-chain events (metadata updates considered as off-chain for change journal)
				if evt.EventType != schema.ProvenanceEventTypeMint &&
					evt.EventType != schema.ProvenanceEventTypeBurn &&
					evt.EventType != schema.ProvenanceEventTypeTransfer {
					continue
				}

				// Populate meta with provenance information
				meta := schema.ProvenanceChangeMeta{
					TokenID:     token.ID,
					Chain:       token.Chain,
					Standard:    token.Standard,
					Contract:    token.ContractAddress,
					TokenNumber: token.TokenNumber,
					From:        evt.FromAddress,
					To:          evt.ToAddress,
					Quantity:    *evt.Quantity,
				}
				if evt.TxHash != nil {
					meta.TxHash = *evt.TxHash
				}
				metaJSON, err := json.Marshal(meta)
				if err != nil {
					return fmt.Errorf("failed to marshal change journal meta: %w", err)
				}

				subjectType := types.ProvenanceEventTypeToSubjectType(evt.EventType, token.Standard)
				changeJournals = append(changeJournals, schema.ChangesJournal{
					SubjectType: subjectType,
					SubjectID:   fmt.Sprintf("%d", evt.ID),
					ChangedAt:   evt.Timestamp,
					Meta:        metaJSON,
				})
			}

			// ChangesJournal has 4 fields: subject_type, subject_id, changed_at, meta
			batchSize = calculateSafeBatchSize(len(changeJournals), 4)

			// Use ON CONFLICT DO NOTHING with the unique constraint columns
			// Unique constraint: (subject_type, subject_id, changed_at)
			if err := tx.Clauses(clause.OnConflict{
				Columns:   []clause.Column{{Name: "subject_type"}, {Name: "subject_id"}, {Name: "changed_at"}},
				DoNothing: true,
			}).CreateInBatches(changeJournals, batchSize).Error; err != nil {
				return fmt.Errorf("failed to create changes_journal entries: %w", err)
			}
		}

		return nil
	})
}

// UpsertTokenBalanceForOwner upserts a token balance for a specific owner with owner-related provenance events
// Unlike CreateTokenWithProvenances, this method does NOT delete other owners' balances or events
// This is used for owner-specific indexing where we only want to update one owner's data
func (s *pgStore) UpsertTokenBalanceForOwner(ctx context.Context, input UpsertTokenBalanceForOwnerInput) error {
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// 1. Upsert the token
		token := schema.Token{
			TokenCID:        input.Token.TokenCID,
			Chain:           input.Token.Chain,
			Standard:        input.Token.Standard,
			ContractAddress: input.Token.ContractAddress,
			TokenNumber:     input.Token.TokenNumber,
			CurrentOwner:    input.Token.CurrentOwner,
			Burned:          input.Token.Burned,
		}

		// Use ON CONFLICT to update if token already exists
		// For owner-specific indexing, we only update current_owner if it's explicitly set
		var err error
		if input.Token.CurrentOwner != nil {
			// With DoUpdates, we can use Returning to get the ID back
			err = tx.Clauses(clause.OnConflict{
				Columns:   []clause.Column{{Name: "token_cid"}},
				DoUpdates: clause.AssignmentColumns([]string{"current_owner"}),
			}).Clauses(clause.Returning{}).Create(&token).Error
		} else {
			// With DoNothing, GORM doesn't return the row, so we need to fetch it separately
			err = tx.Clauses(clause.OnConflict{
				Columns:   []clause.Column{{Name: "token_cid"}},
				DoNothing: true,
			}).Create(&token).Error

			// Fetch the token ID if it wasn't set (conflict case with DoNothing)
			if err == nil && token.ID == 0 {
				err = tx.Where("token_cid = ?", input.Token.TokenCID).First(&token).Error
			}
		}
		if err != nil {
			return fmt.Errorf("failed to upsert token: %w", err)
		}

		// 2. Upsert the specific owner's balance (DO NOT delete other owners)
		if !types.IsPositiveNumeric(input.Quantity) {
			return fmt.Errorf("quantity is not a positive numeric value: %s", input.Quantity)
		}

		balance := schema.Balance{
			TokenID:      token.ID,
			OwnerAddress: input.OwnerAddress,
			Quantity:     input.Quantity,
		}

		if err := tx.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "token_id"}, {Name: "owner_address"}},
			DoUpdates: clause.AssignmentColumns([]string{"quantity"}),
		}).Create(&balance).Error; err != nil {
			return fmt.Errorf("failed to upsert balance: %w", err)
		}

		// 3. Insert provenance events (with ON CONFLICT DO NOTHING to avoid duplicates)
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

			// ProvenanceEvent has 11 fields: token_id, chain, event_type, from_address, to_address,
			// quantity, tx_hash, block_number, block_hash, timestamp, raw
			batchSize := calculateSafeBatchSize(len(provenanceEvents), 11)

			// Use ON CONFLICT DO NOTHING to skip duplicates based on comprehensive unique index
			// (chain, tx_hash, token_id, from_address, to_address, event_type)
			if err := tx.Clauses(clause.OnConflict{
				Columns:   []clause.Column{{Name: "chain"}, {Name: "tx_hash"}, {Name: "token_id"}, {Name: "from_address"}, {Name: "to_address"}, {Name: "event_type"}},
				DoNothing: true,
			}).CreateInBatches(provenanceEvents, batchSize).Error; err != nil {
				return fmt.Errorf("failed to create provenance events: %w", err)
			}

			// 4. Update ownership periods for newly inserted provenance events
			for i := range provenanceEvents {
				// Skip events that weren't inserted (due to ON CONFLICT DO NOTHING)
				if provenanceEvents[i].ID == 0 {
					continue
				}

				if err := s.updateOwnershipPeriods(ctx, tx, &provenanceEvents[i], token.Standard); err != nil {
					return fmt.Errorf("failed to update ownership periods for event %d: %w", i, err)
				}
			}

			// 5. Create changes_journal entries for newly inserted events
			changeJournals := make([]schema.ChangesJournal, 0, len(provenanceEvents))
			for _, evt := range provenanceEvents {
				// Skip events that weren't inserted (due to ON CONFLICT DO NOTHING)
				if evt.ID == 0 {
					continue
				}

				// Skip non-provenance events (metadata updates)
				if evt.EventType != schema.ProvenanceEventTypeMint &&
					evt.EventType != schema.ProvenanceEventTypeBurn &&
					evt.EventType != schema.ProvenanceEventTypeTransfer {
					continue
				}

				// Populate meta with provenance information
				meta := schema.ProvenanceChangeMeta{
					TokenID:     token.ID,
					Chain:       token.Chain,
					Standard:    token.Standard,
					Contract:    token.ContractAddress,
					TokenNumber: token.TokenNumber,
					From:        evt.FromAddress,
					To:          evt.ToAddress,
					Quantity:    *evt.Quantity,
				}
				if evt.TxHash != nil {
					meta.TxHash = *evt.TxHash
				}
				metaJSON, err := json.Marshal(meta)
				if err != nil {
					return fmt.Errorf("failed to marshal change journal meta: %w", err)
				}

				subjectType := types.ProvenanceEventTypeToSubjectType(evt.EventType, token.Standard)
				changeJournals = append(changeJournals, schema.ChangesJournal{
					SubjectType: subjectType,
					SubjectID:   fmt.Sprintf("%d", evt.ID),
					ChangedAt:   evt.Timestamp,
					Meta:        metaJSON,
				})
			}

			// Use ON CONFLICT DO NOTHING with the unique constraint columns
			if len(changeJournals) > 0 {
				// ChangesJournal has 4 fields: subject_type, subject_id, changed_at, meta
				batchSize := calculateSafeBatchSize(len(changeJournals), 4)

				if err := tx.Clauses(clause.OnConflict{
					Columns:   []clause.Column{{Name: "subject_type"}, {Name: "subject_id"}, {Name: "changed_at"}},
					DoNothing: true,
				}).CreateInBatches(changeJournals, batchSize).Error; err != nil {
					return fmt.Errorf("failed to create changes_journal entries: %w", err)
				}
			}
		}

		return nil
	})
}

// GetChanges retrieves changes with optional filters and pagination
func (s *pgStore) GetChanges(ctx context.Context, filter ChangesQueryFilter) ([]*schema.ChangesJournal, uint64, error) {
	// Build the base query for changes
	query := s.db.WithContext(ctx).Model(&schema.ChangesJournal{})

	// Apply anchor filter (ID-based cursor) - takes precedence over since
	if filter.Anchor != nil {
		// Anchor is a cursor - show records after this ID (ascending order)
		query = query.Where("id > ?", *filter.Anchor)
	} else if filter.Since != nil {
		// Deprecated: timestamp filter - kept for backward compatibility
		// Note: This may cause inconsistent results due to different timestamp semantics across subject types
		query = query.Where("changed_at >= ?", *filter.Since)
	}

	// Apply subject type filter
	if len(filter.SubjectTypes) > 0 {
		query = query.Where("subject_type IN ?", filter.SubjectTypes)
	}

	// Apply subject ID filter
	if len(filter.SubjectIDs) > 0 {
		query = query.Where("subject_id IN ?", filter.SubjectIDs)
	}

	// Filter by token_id
	if len(filter.TokenIDs) > 0 {
		// Build query to match changes for these tokens
		// Different subject types reference tokens differently
		query = query.Where(`
			(
				-- Provenance changes (token/owner/balance): subject_id is provenance_event_id
				subject_type IN (?, ?, ?) AND subject_id IN (
					SELECT CAST(id AS TEXT) FROM provenance_events WHERE token_id IN ?
				)
			) OR (
				-- Metadata, enrich_source, and token_viewability changes: subject_id is token_id
				subject_type IN (?, ?, ?) AND subject_id::BIGINT IN ?
			)
		`,
			schema.SubjectTypeToken, schema.SubjectTypeOwner, schema.SubjectTypeBalance, filter.TokenIDs,
			schema.SubjectTypeMetadata, schema.SubjectTypeEnrichSource, schema.SubjectTypeTokenViewability, filter.TokenIDs,
		)
	}

	// Filter by token_cid - need to resolve to token_ids and check subject_id based on subject_type
	if len(filter.TokenCIDs) > 0 {
		// Get token IDs for the given token CIDs
		var tokens []schema.Token
		if err := s.db.WithContext(ctx).Where("token_cid IN ?", filter.TokenCIDs).Find(&tokens).Error; err != nil {
			return nil, 0, fmt.Errorf("failed to get tokens for token_cids: %w", err)
		}

		if len(tokens) == 0 {
			// No matching tokens, return empty result
			return []*schema.ChangesJournal{}, 0, nil
		}

		tokenIDs := make([]uint64, len(tokens))
		for i, token := range tokens {
			tokenIDs[i] = token.ID
		}

		// Build query to match changes for these tokens
		// Different subject types reference tokens differently
		query = query.Where(`
			(
				-- Provenance changes (token/owner/balance): subject_id is provenance_event_id
				subject_type IN (?, ?, ?) AND subject_id IN (
					SELECT CAST(id AS TEXT) FROM provenance_events WHERE token_id IN ?
				)
			) OR (
				-- Metadata, enrich_source, and token_viewability changes: subject_id is token_id
				subject_type IN (?, ?, ?) AND subject_id::BIGINT IN ?
			)
		`,
			schema.SubjectTypeToken, schema.SubjectTypeOwner, schema.SubjectTypeBalance, tokenIDs,
			schema.SubjectTypeMetadata, schema.SubjectTypeEnrichSource, schema.SubjectTypeTokenViewability, tokenIDs,
		)
	}

	// Filter by addresses - include both provenance-linked changes and metadata/enrich_source changes
	if len(filter.Addresses) > 0 {
		// For provenance changes, match by addresses in provenance_events
		// For metadata/enrich_source, use the ownership periods table for fast lookups
		query = query.Where(`
			(
				-- Include provenance-related changes (owner/balance/token) linked to specific provenance events
				subject_type IN (?, ?, ?) AND EXISTS (
					SELECT 1 FROM provenance_events pe
					WHERE pe.id::text = changes_journal.subject_id
					AND (pe.from_address IN ? OR pe.to_address IN ?)
				)
			) OR (
				-- Include metadata/enrich_source changes that occurred during ownership periods
				-- Use the pre-computed ownership periods table for fast lookups
				subject_type IN (?, ?, ?) AND EXISTS (
					SELECT 1 FROM token_ownership_periods op
					WHERE op.token_id::text = changes_journal.subject_id
					AND op.owner_address IN ?
					AND changes_journal.changed_at >= op.acquired_at
					AND (op.released_at IS NULL OR changes_journal.changed_at < op.released_at)
				)
			)
		`,
			schema.SubjectTypeToken, schema.SubjectTypeOwner, schema.SubjectTypeBalance,
			filter.Addresses, filter.Addresses,
			schema.SubjectTypeMetadata, schema.SubjectTypeEnrichSource, schema.SubjectTypeTokenViewability,
			filter.Addresses,
		)
	}

	// Count total matching records
	var total int64
	if err := query.Count(&total).Error; err != nil {
		return nil, 0, fmt.Errorf("failed to count changes: %w", err)
	}

	// Apply ordering
	// For backward compatibility: when using deprecated 'since' parameter, order by changed_at with configurable direction
	// Otherwise, always order by ID ascending for sequential audit log
	if filter.Anchor == nil && filter.Since != nil {
		if filter.OrderDesc {
			query = query.Order("changed_at DESC, id DESC")
		} else {
			query = query.Order("changed_at ASC, id ASC")
		}
	} else {
		query = query.Order("changed_at ASC, id ASC")
	}

	// Apply pagination
	// Offset is only applied when using deprecated 'since' parameter (offset-based pagination)
	// When using 'anchor' for cursor-based pagination, offset is ignored (cursor continues from anchor ID)
	if filter.Offset > 0 && filter.Since != nil && filter.Anchor == nil {
		query = query.Offset(int(filter.Offset)) //nolint:gosec,G115
	}
	if filter.Limit > 0 {
		query = query.Limit(filter.Limit)
	}

	// Execute the query
	var changes []schema.ChangesJournal
	if err := query.Find(&changes).Error; err != nil {
		return nil, 0, fmt.Errorf("failed to query changes: %w", err)
	}

	// Convert to pointers
	var results []*schema.ChangesJournal
	for i := range changes {
		results = append(results, &changes[i])
	}

	return results, uint64(total), nil //nolint:gosec,G115
}

// GetProvenanceEventByID retrieves a provenance event by ID
func (s *pgStore) GetProvenanceEventByID(ctx context.Context, id uint64) (*schema.ProvenanceEvent, error) {
	var event schema.ProvenanceEvent
	err := s.db.WithContext(ctx).Where("id = ?", id).First(&event).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get provenance event: %w", err)
	}
	return &event, nil
}

// updateOwnershipPeriods updates ownership periods after a provenance event
// This is an internal method called within the same transaction as the provenance event creation
func (s *pgStore) updateOwnershipPeriods(ctx context.Context, tx *gorm.DB, event *schema.ProvenanceEvent, tokenStandard domain.ChainStandard) error {
	// Skip if not a transfer, mint, or burn event (metadata updates don't affect ownership)
	if event.EventType != schema.ProvenanceEventTypeTransfer &&
		event.EventType != schema.ProvenanceEventTypeMint &&
		event.EventType != schema.ProvenanceEventTypeBurn {
		return nil
	}

	// Handle based on token standard
	if tokenStandard == domain.StandardERC721 {
		return s.updateERC721OwnershipPeriods(ctx, tx, event)
	}
	// ERC1155, FA2 - check balances
	return s.updateMultiEditionOwnershipPeriods(ctx, tx, event)
}

// updateERC721OwnershipPeriods handles ownership periods for ERC721 tokens (single owner)
func (s *pgStore) updateERC721OwnershipPeriods(ctx context.Context, tx *gorm.DB, event *schema.ProvenanceEvent) error {
	// For ERC721, it's simple: one owner at a time

	// If event is not a mint, close their ownership period
	if event.EventType != schema.ProvenanceEventTypeMint {
		result := tx.WithContext(ctx).
			Model(&schema.TokenOwnershipPeriod{}).
			Where("token_id = ? AND owner_address = ? AND released_at IS NULL",
				event.TokenID, *event.FromAddress).
			Update("released_at", event.Timestamp)

		if result.Error != nil {
			return fmt.Errorf("failed to close ownership period for from_address: %w", result.Error)
		}
	}

	// If event is not a burn, create new ownership period
	if event.EventType != schema.ProvenanceEventTypeBurn {
		ownershipPeriod := &schema.TokenOwnershipPeriod{
			TokenID:      event.TokenID,
			OwnerAddress: *event.ToAddress,
			AcquiredAt:   event.Timestamp,
			ReleasedAt:   nil,
		}

		// Handle conflict: if an active period already exists, do nothing
		// The partial unique index ensures only one active period exists per token-owner
		if err := tx.WithContext(ctx).
			Clauses(clause.OnConflict{
				DoNothing: true,
			}).
			Clauses(clause.Returning{Columns: []clause.Column{}}).
			Create(ownershipPeriod).Error; err != nil {
			return fmt.Errorf("failed to create ownership period for to_address: %w", err)
		}
	}

	return nil
}

// updateMultiEditionOwnershipPeriods handles ownership periods for ERC1155/FA2 tokens (multiple owners, quantities)
func (s *pgStore) updateMultiEditionOwnershipPeriods(ctx context.Context, tx *gorm.DB, event *schema.ProvenanceEvent) error {
	// For ERC1155/FA2, check actual balances after the event

	// Check from_address balance (if exists and not zero address)
	if event.FromAddress != nil && *event.FromAddress != "" && *event.FromAddress != domain.ETHEREUM_ZERO_ADDRESS {
		var balance schema.Balance
		err := tx.WithContext(ctx).
			Where("token_id = ? AND owner_address = ?",
				event.TokenID, *event.FromAddress).
			First(&balance).Error

		// If balance is now 0 or doesn't exist, close ownership period
		if errors.Is(err, gorm.ErrRecordNotFound) || (err == nil && balance.Quantity == "0") {
			result := tx.WithContext(ctx).
				Model(&schema.TokenOwnershipPeriod{}).
				Where("token_id = ? AND owner_address = ? AND released_at IS NULL",
					event.TokenID, *event.FromAddress).
				Update("released_at", event.Timestamp)

			if result.Error != nil {
				return fmt.Errorf("failed to close ownership period for from_address: %w", result.Error)
			}
		} else if err != nil {
			return fmt.Errorf("failed to get balance for from_address: %w", err)
		}
		// If balance > 0, keep ownership period open
	}

	// Check to_address balance (if exists and not zero address)
	if event.ToAddress != nil && *event.ToAddress != "" && *event.ToAddress != domain.ETHEREUM_ZERO_ADDRESS {
		var balance schema.Balance
		err := tx.WithContext(ctx).
			Where("token_id = ? AND owner_address = ?",
				event.TokenID, *event.ToAddress).
			First(&balance).Error

		if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
			return fmt.Errorf("failed to get balance for to_address: %w", err)
		}

		// If they have a balance > 0, ensure ownership period exists
		if err == nil && balance.Quantity != "0" {
			var existingPeriod schema.TokenOwnershipPeriod
			err := tx.WithContext(ctx).
				Where("token_id = ? AND owner_address = ? AND released_at IS NULL",
					event.TokenID, *event.ToAddress).
				First(&existingPeriod).Error

			if errors.Is(err, gorm.ErrRecordNotFound) {
				// Create new ownership period with conflict handling
				ownershipPeriod := &schema.TokenOwnershipPeriod{
					TokenID:      event.TokenID,
					OwnerAddress: *event.ToAddress,
					AcquiredAt:   event.Timestamp,
					ReleasedAt:   nil,
				}

				// Handle conflict: if an active period already exists, do nothing
				// The partial unique index ensures only one active period exists per token-owner
				if err := tx.WithContext(ctx).
					Clauses(clause.OnConflict{
						DoNothing: true,
					}).
					Clauses(clause.Returning{Columns: []clause.Column{}}).
					Create(ownershipPeriod).Error; err != nil {
					return fmt.Errorf("failed to create ownership period for to_address: %w", err)
				}
			} else if err != nil {
				return fmt.Errorf("failed to check existing ownership period: %w", err)
			}
			// If period already exists and is open, nothing to do
		}
	}

	return nil
}

// GetBalanceByID retrieves a balance by ID
func (s *pgStore) GetBalanceByID(ctx context.Context, id uint64) (*schema.Balance, error) {
	var balance schema.Balance
	err := s.db.WithContext(ctx).Where("id = ?", id).First(&balance).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get balance: %w", err)
	}
	return &balance, nil
}

// GetTokenCIDsByOwner retrieves all token CIDs owned by an address (where balance > 0)
func (s *pgStore) GetTokenCIDsByOwner(ctx context.Context, ownerAddress string) ([]domain.TokenCID, error) {
	var tokenCIDs []domain.TokenCID
	err := s.db.WithContext(ctx).
		Table("tokens t").
		Select("DISTINCT t.token_cid").
		Joins("INNER JOIN balances b ON t.id = b.token_id").
		Where("b.owner_address = ? AND CAST(b.quantity AS NUMERIC) > 0", ownerAddress).
		Find(&tokenCIDs).Error

	if err != nil {
		return nil, fmt.Errorf("failed to query tokens by owner: %w", err)
	}

	return tokenCIDs, nil
}

// GetIndexingBlockRangeForAddress retrieves the indexing block range for an address and chain
// Returns min_block=0, max_block=0 if no range exists for the chain
func (s *pgStore) GetIndexingBlockRangeForAddress(ctx context.Context, address string, chainID domain.Chain) (minBlock uint64, maxBlock uint64, err error) {
	var watchedAddr schema.WatchedAddresses
	err = s.db.WithContext(ctx).
		Where("address = ?", address).
		First(&watchedAddr).Error

	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			// No watched address record exists yet
			return 0, 0, nil
		}
		return 0, 0, fmt.Errorf("failed to get watched address: %w", err)
	}

	// Check if block range exists for the chain
	if watchedAddr.LastSuccessfulIndexingBlkRange == nil {
		return 0, 0, nil
	}

	ranges := *watchedAddr.LastSuccessfulIndexingBlkRange
	blockRange, exists := ranges[chainID]
	if !exists {
		return 0, 0, nil
	}

	return blockRange.MinBlock, blockRange.MaxBlock, nil
}

// EnsureWatchedAddressExists creates a watched address record if it doesn't exist
func (s *pgStore) EnsureWatchedAddressExists(ctx context.Context, address string, chain domain.Chain, dailyQuota int) error {
	watchedAddr := schema.WatchedAddresses{
		Chain:           chain,
		Address:         address,
		Watching:        true,
		DailyTokenQuota: dailyQuota,
	}

	// Use ON CONFLICT DO NOTHING to handle concurrent inserts
	err := s.db.WithContext(ctx).
		Clauses(clause.OnConflict{DoNothing: true}).
		Create(&watchedAddr).Error

	if err != nil {
		return fmt.Errorf("failed to ensure watched address exists: %w", err)
	}

	return nil
}

// UpdateIndexingBlockRangeForAddress updates the indexing block range for an address and chain
// Assumes the watched address record already exists
func (s *pgStore) UpdateIndexingBlockRangeForAddress(ctx context.Context, address string, chainID domain.Chain, minBlock uint64, maxBlock uint64) error {
	// Validate the block range is valid
	if minBlock > maxBlock {
		return fmt.Errorf("min block must be less than max block")
	}
	if minBlock == 0 && maxBlock == 0 {
		return fmt.Errorf("min block and max block must not be 0")
	}

	// Use a transaction with row-level locking
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// Lock the record for update
		var watchedAddr schema.WatchedAddresses
		err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("address = ?", address).
			First(&watchedAddr).Error
		if err != nil {
			return fmt.Errorf("failed to lock watched address for update: %w", err)
		}

		// Initialize the block ranges map if nil
		if watchedAddr.LastSuccessfulIndexingBlkRange == nil {
			ranges := schema.IndexingBlockRanges{}
			watchedAddr.LastSuccessfulIndexingBlkRange = &ranges
		} else {
			// Validate the new min block is less than or equal to the current min block
			if (*watchedAddr.LastSuccessfulIndexingBlkRange)[chainID].MinBlock < minBlock {
				return fmt.Errorf("min block must be less than or equal to the current min block")
			}
			// Validate the new max block is greater than or equal to the current max block
			if (*watchedAddr.LastSuccessfulIndexingBlkRange)[chainID].MaxBlock > maxBlock {
				return fmt.Errorf("max block must be greater than or equal to the current max block")
			}
		}

		// Update the block range for the specific chain
		ranges := *watchedAddr.LastSuccessfulIndexingBlkRange
		ranges[chainID] = schema.BlockRange{
			MinBlock: minBlock,
			MaxBlock: maxBlock,
		}
		watchedAddr.LastSuccessfulIndexingBlkRange = &ranges

		// Save the updated record
		err = tx.Model(&schema.WatchedAddresses{}).
			Where("address = ?", address).
			Update("last_successful_indexing_blk_range", watchedAddr.LastSuccessfulIndexingBlkRange).Error
		if err != nil {
			return fmt.Errorf("failed to update indexing block range: %w", err)
		}

		return nil
	})
}

// GetMediaAssetByID retrieves a media asset by ID
func (s *pgStore) GetMediaAssetByID(ctx context.Context, id int64) (*schema.MediaAsset, error) {
	var media schema.MediaAsset
	err := s.db.WithContext(ctx).Where("id = ?", id).First(&media).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get media asset: %w", err)
	}
	return &media, nil
}

// GetMediaAssetBySourceURL retrieves a media asset by source URL
func (s *pgStore) GetMediaAssetBySourceURL(ctx context.Context, sourceURL string, provider schema.StorageProvider) (*schema.MediaAsset, error) {
	var media schema.MediaAsset
	err := s.db.WithContext(ctx).
		Where("source_url = ? AND provider = ?", sourceURL, provider).First(&media).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get media asset by source URL: %w", err)
	}
	return &media, nil
}

// GetMediaAssetsBySourceURLs retrieves media assets by multiple source URLs
func (s *pgStore) GetMediaAssetsBySourceURLs(ctx context.Context, sourceURLs []string) ([]schema.MediaAsset, error) {
	if len(sourceURLs) == 0 {
		return []schema.MediaAsset{}, nil
	}

	var mediaAssets []schema.MediaAsset
	err := s.db.WithContext(ctx).
		Where("source_url IN ?", sourceURLs).
		Find(&mediaAssets).Error
	if err != nil {
		return nil, fmt.Errorf("failed to get media assets by source URLs: %w", err)
	}
	return mediaAssets, nil
}

// CreateMediaAsset creates a new media asset record
// Uses ON CONFLICT to update existing records with new data
func (s *pgStore) CreateMediaAsset(ctx context.Context, input CreateMediaAssetInput) (*schema.MediaAsset, error) {
	var mediaAsset *schema.MediaAsset

	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// 1. Get the old media asset (if it exists) for change tracking - BEFORE upsert
		var oldMediaAsset *schema.MediaAsset
		err := tx.Where("source_url = ? AND provider = ?", input.SourceURL, input.Provider).First(&oldMediaAsset).Error
		if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
			return fmt.Errorf("failed to get old media asset: %w", err)
		}

		// 2. Upsert media asset
		newMediaAsset := schema.MediaAsset{
			SourceURL:        input.SourceURL,
			MimeType:         input.MimeType,
			FileSizeBytes:    input.FileSizeBytes,
			Provider:         input.Provider,
			ProviderAssetID:  input.ProviderAssetID,
			ProviderMetadata: input.ProviderMetadata,
			VariantURLs:      input.VariantURLs,
		}

		// Use ON CONFLICT to update all fields if duplicate exists
		err = tx.Clauses(clause.OnConflict{
			Columns: []clause.Column{{Name: "source_url"}, {Name: "provider"}},
			DoUpdates: clause.AssignmentColumns([]string{
				"mime_type",
				"file_size_bytes",
				"provider_asset_id",
				"provider_metadata",
				"variant_urls",
			}),
		}).Create(&newMediaAsset).Error

		if err != nil {
			return fmt.Errorf("failed to create media asset: %w", err)
		}

		mediaAsset = &newMediaAsset

		// 3. Create change journal entry
		// Convert provider metadata to string for meta
		var providerMetadataStr *string
		if len(input.ProviderMetadata) > 0 {
			str := string(input.ProviderMetadata)
			providerMetadataStr = &str
		}

		metaChanges := schema.MediaAssetChangeMeta{
			New: schema.MediaAssetFields{
				SourceURL:        input.SourceURL,
				Provider:         string(input.Provider),
				ProviderAssetID:  input.ProviderAssetID,
				MimeType:         input.MimeType,
				FileSizeBytes:    input.FileSizeBytes,
				VariantURLs:      string(input.VariantURLs),
				ProviderMetadata: providerMetadataStr,
			},
		}

		// Add old media asset if it existed
		if oldMediaAsset != nil {
			var oldProviderMetadataStr *string
			if len(oldMediaAsset.ProviderMetadata) > 0 {
				str := string(oldMediaAsset.ProviderMetadata)
				oldProviderMetadataStr = &str
			}

			metaChanges.Old = schema.MediaAssetFields{
				SourceURL:        oldMediaAsset.SourceURL,
				Provider:         string(oldMediaAsset.Provider),
				ProviderAssetID:  oldMediaAsset.ProviderAssetID,
				MimeType:         oldMediaAsset.MimeType,
				FileSizeBytes:    oldMediaAsset.FileSizeBytes,
				VariantURLs:      string(oldMediaAsset.VariantURLs),
				ProviderMetadata: oldProviderMetadataStr,
			}
		}

		metaJSON, err := json.Marshal(metaChanges)
		if err != nil {
			return fmt.Errorf("failed to marshal change journal meta: %w", err)
		}

		changeJournal := schema.ChangesJournal{
			SubjectType: schema.SubjectTypeMediaAsset,
			SubjectID:   fmt.Sprintf("%d", newMediaAsset.ID),
			ChangedAt:   time.Now(),
			Meta:        metaJSON,
		}

		// Use ON CONFLICT DO NOTHING to skip duplicates based on unique constraint
		if err := tx.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "subject_type"}, {Name: "subject_id"}, {Name: "changed_at"}},
			DoNothing: true,
		}).
			Clauses(clause.Returning{Columns: []clause.Column{}}).
			Create(&changeJournal).Error; err != nil {
			return fmt.Errorf("failed to create change journal: %w", err)
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return mediaAsset, nil
}

// SetKeyValue sets a key-value pair in the key-value store
func (s *pgStore) SetKeyValue(ctx context.Context, key string, value string) error {
	kv := schema.KeyValueStore{
		Key:   key,
		Value: value,
	}

	err := s.db.WithContext(ctx).Save(&kv).Error
	if err != nil {
		return fmt.Errorf("failed to set key-value: %w", err)
	}

	return nil
}

// GetKeyValue retrieves a value by key from the key-value store
func (s *pgStore) GetKeyValue(ctx context.Context, key string) (string, error) {
	var kv schema.KeyValueStore
	err := s.db.WithContext(ctx).Where("key = ?", key).First(&kv).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return "", nil
		}
		return "", fmt.Errorf("failed to get key-value: %w", err)
	}

	return kv.Value, nil
}

// GetAllKeyValuesByPrefix retrieves all key-value pairs with a specific prefix
func (s *pgStore) GetAllKeyValuesByPrefix(ctx context.Context, prefix string) (map[string]string, error) {
	var kvs []schema.KeyValueStore
	err := s.db.WithContext(ctx).Where("key LIKE ?", prefix+"%").Find(&kvs).Error
	if err != nil {
		return nil, fmt.Errorf("failed to get key-values by prefix: %w", err)
	}

	result := make(map[string]string, len(kvs))
	for _, kv := range kvs {
		result[kv.Key] = kv.Value
	}

	return result, nil
}

// GetActiveWebhookClientsByEventType retrieves active webhook clients that match the given event type
func (s *pgStore) GetActiveWebhookClientsByEventType(ctx context.Context, eventType string) ([]*schema.WebhookClient, error) {
	var clients []*schema.WebhookClient

	// Query for active clients where event_filters contains the event type or wildcard "*"
	// Using JSONB containment operator @> to check if the array contains the value
	err := s.db.WithContext(ctx).
		Where("is_active").
		Where("event_filters @> ?::jsonb OR event_filters @> ?::jsonb",
			fmt.Sprintf(`["%s"]`, eventType),
			`["*"]`).
		Find(&clients).Error

	if err != nil {
		return nil, fmt.Errorf("failed to get webhook clients by event type: %w", err)
	}

	return clients, nil
}

// GetWebhookClientByID retrieves a webhook client by client ID
func (s *pgStore) GetWebhookClientByID(ctx context.Context, clientID string) (*schema.WebhookClient, error) {
	var client schema.WebhookClient
	err := s.db.WithContext(ctx).Where("client_id = ?", clientID).First(&client).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get webhook client: %w", err)
	}
	return &client, nil
}

// CreateWebhookClient creates a new webhook client
func (s *pgStore) CreateWebhookClient(ctx context.Context, input CreateWebhookClientInput) (*schema.WebhookClient, error) {
	now := time.Now()
	client := &schema.WebhookClient{
		ClientID:         input.ClientID,
		WebhookURL:       input.WebhookURL,
		WebhookSecret:    input.WebhookSecret,
		EventFilters:     input.EventFilters,
		IsActive:         input.IsActive,
		RetryMaxAttempts: input.RetryMaxAttempts,
		CreatedAt:        now,
		UpdatedAt:        now,
	}

	err := s.db.WithContext(ctx).Create(client).Error
	if err != nil {
		return nil, fmt.Errorf("failed to create webhook client: %w", err)
	}
	return client, nil
}

// CreateWebhookDelivery creates a new webhook delivery record
func (s *pgStore) CreateWebhookDelivery(ctx context.Context, delivery *schema.WebhookDelivery) error {
	// Payload is already JSON bytes from the executor
	err := s.db.WithContext(ctx).Create(delivery).Error
	if err != nil {
		return fmt.Errorf("failed to create webhook delivery: %w", err)
	}
	return nil
}

// UpdateWebhookDeliveryStatus updates the status and result of a webhook delivery
func (s *pgStore) UpdateWebhookDeliveryStatus(ctx context.Context, deliveryID uint64, status schema.WebhookDeliveryStatus, attempts int, responseStatus *int, responseBody, errorMessage string) error {
	now := time.Now()
	updates := map[string]interface{}{
		"delivery_status": status,
		"attempts":        attempts,
		"response_body":   responseBody,
		"last_attempt_at": now,
		"updated_at":      now,
	}

	if responseStatus != nil {
		updates["response_status"] = *responseStatus
	}
	if errorMessage != "" {
		// Limit error message
		if len(errorMessage) > 1024 {
			errorMessage = errorMessage[:1024]
		}
		updates["error_message"] = errorMessage
	}

	err := s.db.WithContext(ctx).
		Model(&schema.WebhookDelivery{}).
		Where("id = ?", deliveryID).
		Updates(updates).Error

	if err != nil {
		return fmt.Errorf("failed to update webhook delivery status: %w", err)
	}

	return nil
}

// =============================================================================
// Budgeted Indexing Mode Quota Operations
// =============================================================================

// GetQuotaInfo retrieves quota information for an address
// Auto-resets quota if the 24-hour window has expired
func (s *pgStore) GetQuotaInfo(ctx context.Context, address string, chain domain.Chain) (*QuotaInfo, error) {
	var watched schema.WatchedAddresses
	err := s.db.WithContext(ctx).
		Where("chain = ? AND address = ?", chain, address).
		First(&watched).Error

	if err != nil {
		return nil, fmt.Errorf("failed to get watched address: %w", err)
	}

	now := time.Now()
	nextReset := now.Add(24 * time.Hour)

	// AUTO-RESET: If quota period expired, reset it
	if watched.QuotaResetAt != nil && now.After(*watched.QuotaResetAt) {
		err = s.db.WithContext(ctx).
			Model(&schema.WatchedAddresses{}).
			Where("chain = ? AND address = ?", chain, address).
			Updates(map[string]interface{}{
				"tokens_indexed_today": 0,
				"quota_reset_at":       nextReset,
			}).Error

		if err != nil {
			return nil, fmt.Errorf("failed to reset quota: %w", err)
		}

		// Return fresh quota
		return &QuotaInfo{
			RemainingQuota:     watched.DailyTokenQuota,
			TotalQuota:         watched.DailyTokenQuota,
			TokensIndexedToday: 0,
			QuotaResetAt:       nextReset,
			QuotaExhausted:     false,
		}, nil
	}

	// If quota_reset_at is NULL (first time), set it
	if watched.QuotaResetAt == nil {
		err = s.db.WithContext(ctx).
			Model(&schema.WatchedAddresses{}).
			Where("chain = ? AND address = ?", chain, address).
			Update("quota_reset_at", nextReset).Error

		if err != nil {
			return nil, fmt.Errorf("failed to initialize quota reset time: %w", err)
		}

		watched.QuotaResetAt = &nextReset
	}

	remaining := max(watched.DailyTokenQuota-watched.TokensIndexedToday, 0)

	return &QuotaInfo{
		RemainingQuota:     remaining,
		TotalQuota:         watched.DailyTokenQuota,
		TokensIndexedToday: watched.TokensIndexedToday,
		QuotaResetAt:       *watched.QuotaResetAt,
		QuotaExhausted:     remaining == 0,
	}, nil
}

// IncrementTokensIndexed increments the token counter after successful indexing
func (s *pgStore) IncrementTokensIndexed(ctx context.Context, address string, chain domain.Chain, count int) error {
	if count <= 0 {
		return fmt.Errorf("count must be positive")
	}

	result := s.db.WithContext(ctx).
		Model(&schema.WatchedAddresses{}).
		Where("chain = ? AND address = ?", chain, address).
		Update("tokens_indexed_today", gorm.Expr("tokens_indexed_today + ?", count))

	if result.Error != nil {
		return fmt.Errorf("failed to increment tokens indexed: %w", result.Error)
	}

	if result.RowsAffected == 0 {
		return fmt.Errorf("watched address not found: %s on chain %s", address, chain)
	}

	return nil
}

// =============================================================================
// Address Indexing Job Operations
// =============================================================================

// CreateAddressIndexingJob creates a new address indexing job record
func (s *pgStore) CreateAddressIndexingJob(ctx context.Context, input CreateAddressIndexingJobInput) error {
	now := time.Now()
	job := &schema.AddressIndexingJob{
		Address:       input.Address,
		Chain:         input.Chain,
		Status:        input.Status,
		WorkflowID:    input.WorkflowID,
		WorkflowRunID: input.WorkflowRunID,
		StartedAt:     now, // Always set started_at since we only track running workflows
	}

	// Set appropriate timestamp field based on status
	switch input.Status {
	case schema.IndexingJobStatusRunning:
		// StartedAt already set above
	case schema.IndexingJobStatusPaused:
		job.PausedAt = &now
	case schema.IndexingJobStatusCompleted:
		job.CompletedAt = &now
	case schema.IndexingJobStatusFailed:
		job.FailedAt = &now
	case schema.IndexingJobStatusCanceled:
		job.CanceledAt = &now
	}

	err := s.db.WithContext(ctx).
		Clauses(clause.OnConflict{DoNothing: true}).
		Create(job).Error

	if err != nil {
		return fmt.Errorf("failed to create address indexing job: %w", err)
	}

	return nil
}

// GetAddressIndexingJobByWorkflowID retrieves a job by workflow ID
func (s *pgStore) GetAddressIndexingJobByWorkflowID(ctx context.Context, workflowID string) (*schema.AddressIndexingJob, error) {
	var job schema.AddressIndexingJob

	err := s.db.WithContext(ctx).Where("workflow_id = ?", workflowID).First(&job).Error
	if err == nil {
		return &job, nil
	}
	if !errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, fmt.Errorf("failed to get job: %w", err)
	}
	if !hasDBResolver(s.db) {
		return nil, fmt.Errorf("job not found for workflow: %s", workflowID)
	}

	// Replica can lag behind primary; retry on primary before returning not found.
	err = s.db.WithContext(ctx).
		Clauses(dbresolver.Write).
		Where("workflow_id = ?", workflowID).
		First(&job).Error
	if err == nil {
		return &job, nil
	}
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, fmt.Errorf("job not found for workflow: %s", workflowID)
	}
	return nil, fmt.Errorf("failed to get job: %w", err)
}

// GetActiveIndexingJobForAddress retrieves an active (running or paused) job for a specific address and chain
// Returns nil if no active job is found (not an error)
func (s *pgStore) GetActiveIndexingJobForAddress(ctx context.Context, address string, chainID domain.Chain) (*schema.AddressIndexingJob, error) {
	var job schema.AddressIndexingJob

	whereClause := "address = ? AND chain = ? AND status IN ?"
	activeStatuses := []schema.IndexingJobStatus{
		schema.IndexingJobStatusRunning,
		schema.IndexingJobStatusPaused,
	}

	query := func(db *gorm.DB) error {
		return db.WithContext(ctx).
			Where(whereClause, address, chainID, activeStatuses).
			Order("created_at DESC"). // Get the most recent one if multiple exist
			First(&job).Error
	}

	err := query(s.db)
	if err == nil {
		return &job, nil
	}
	if !errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, fmt.Errorf("failed to get active job for address %s on chain %s: %w", address, chainID, err)
	}
	if !hasDBResolver(s.db) {
		return nil, nil // No active job found (not an error)
	}

	// Replica can lag behind primary; retry on primary before returning nil.
	err = query(s.db.Clauses(dbresolver.Write))
	if err == nil {
		return &job, nil
	}
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil // No active job found (not an error)
	}
	return nil, fmt.Errorf("failed to get active job for address %s on chain %s: %w", address, chainID, err)
}

// UpdateAddressIndexingJobStatus updates job status with timestamp
func (s *pgStore) UpdateAddressIndexingJobStatus(ctx context.Context, workflowID string, status schema.IndexingJobStatus, timestamp time.Time) error {
	updates := make(map[string]interface{})
	updates["status"] = status

	// Set appropriate timestamp field based on status
	// Note: started_at is set during creation, not during status updates
	switch status {
	case schema.IndexingJobStatusPaused:
		updates["paused_at"] = timestamp
	case schema.IndexingJobStatusCompleted:
		updates["completed_at"] = timestamp
	case schema.IndexingJobStatusFailed:
		updates["failed_at"] = timestamp
	case schema.IndexingJobStatusCanceled:
		updates["canceled_at"] = timestamp
	}

	return s.db.WithContext(ctx).
		Model(&schema.AddressIndexingJob{}).
		Where("workflow_id = ?", workflowID).
		Updates(updates).Error
}

// UpdateAddressIndexingJobProgress updates job progress metrics
// Note: This method accumulates tokens_processed by incrementing the existing value
func (s *pgStore) UpdateAddressIndexingJobProgress(ctx context.Context, workflowID string, tokensProcessed int, minBlock, maxBlock uint64) error {
	updates := map[string]interface{}{
		"tokens_processed":  gorm.Expr("tokens_processed + ?", tokensProcessed),
		"current_min_block": minBlock,
		"current_max_block": maxBlock,
	}

	return s.db.WithContext(ctx).
		Model(&schema.AddressIndexingJob{}).
		Where("workflow_id = ?", workflowID).
		Updates(updates).Error
}

// GetTokenCountsByAddress retrieves total token counts for an address on a specific chain
// Returns both total tokens indexed and total tokens viewable (with metadata or enrichment)
func (s *pgStore) GetTokenCountsByAddress(ctx context.Context, address string, chain domain.Chain) (*TokenCountsByAddress, error) {
	query := `
		SELECT 
			COUNT(*) as total_indexed,
			COUNT(*) FILTER (
				WHERE EXISTS (
					SELECT 1 FROM token_metadata tm WHERE tm.token_id = tokens.id
				) OR EXISTS (
					SELECT 1 FROM enrichment_sources es WHERE es.token_id = tokens.id
				)
			) as total_viewable
		FROM tokens
		LEFT JOIN balances ON balances.token_id = tokens.id
		WHERE 
			tokens.chain = $1 
			AND (balances.owner_address = $2 OR tokens.current_owner = $2)
	`

	var counts TokenCountsByAddress
	err := s.db.WithContext(ctx).Raw(query, string(chain), address).Scan(&counts).Error
	if err != nil {
		return nil, fmt.Errorf("failed to get token counts for address %s on chain %s: %w", address, chain, err)
	}

	return &counts, nil
}

// =============================================================================
// Token Media Health Operations
// =============================================================================

// syncSingleMediaURL syncs a single media URL health record
// Only performs DB operations if the URL has changed
func (s *pgStore) syncSingleMediaURL(tx *gorm.DB, tokenID uint64, oldURL, newURL *string, source schema.MediaHealthSource) error {
	// Check if URL changed
	oldURLStr := ""
	if oldURL != nil {
		oldURLStr = *oldURL
	}
	newURLStr := ""
	if newURL != nil {
		newURLStr = *newURL
	}

	// No change - skip
	if oldURLStr == newURLStr {
		return nil
	}

	// URL removed or changed - delete old record
	if oldURLStr != "" {
		if err := tx.Where("token_id = ? AND media_url = ? AND media_source = ?", tokenID, oldURLStr, source).
			Delete(&schema.TokenMediaHealth{}).Error; err != nil {
			return fmt.Errorf("failed to delete old health record: %w", err)
		}
	}

	// URL added or changed - insert new record
	if newURLStr != "" {
		healthStatus := schema.MediaHealthStatusUnknown
		if oldURL == nil {
			// If the URL is new, set it to healthy by default and let the sweeper check it
			healthStatus = schema.MediaHealthStatusHealthy
		}
		health := &schema.TokenMediaHealth{
			TokenID:       tokenID,
			MediaURL:      newURLStr,
			MediaSource:   source,
			HealthStatus:  healthStatus,
			LastCheckedAt: time.Unix(0, 0), // Force immediate check by sweeper
		}
		if err := tx.Create(health).Error; err != nil {
			return fmt.Errorf("failed to create health record: %w", err)
		}
	}

	return nil
}

// GetURLsForChecking returns distinct URLs that need health checking
// Returns URLs ordered by oldest check time first
func (s *pgStore) GetURLsForChecking(ctx context.Context, recheckAfter time.Duration, limit int) ([]string, error) {
	cutoffTime := time.Now().Add(-recheckAfter)

	var urls []string
	err := s.db.WithContext(ctx).
		Model(&schema.TokenMediaHealth{}).
		Select("media_url").
		Where("last_checked_at < ? OR health_status = ?",
			cutoffTime,
			schema.MediaHealthStatusUnknown).
		Group("media_url").
		Order("MIN(last_checked_at) ASC").
		Limit(limit).
		Pluck("media_url", &urls).Error

	if err != nil {
		return nil, fmt.Errorf("failed to get URLs for checking: %w", err)
	}

	return urls, nil
}

// GetTokensViewabilityByMediaURL returns all tokens that use a specific URL along with their viewability status
// A token is viewable if it has at least one healthy animation URL, or if no animation URLs exist, at least one healthy image URL
func (s *pgStore) GetTokensViewabilityByMediaURL(ctx context.Context, url string) ([]TokenViewabilityInfo, error) {
	query := `
		SELECT 
			tokens.id as token_id,
			tokens.token_cid as token_cid,
			CASE 
				-- Token is viewable if it has at least one healthy animation URL
				WHEN EXISTS (
					SELECT 1 FROM token_media_health tmh
					WHERE tmh.token_id = tokens.id
						AND tmh.health_status = $1
						AND tmh.media_source IN ($2, $3)
				) THEN true
				-- OR if no animation URLs exist AND has at least one healthy image URL
				WHEN NOT EXISTS (
					SELECT 1 FROM token_media_health tmh
					WHERE tmh.token_id = tokens.id 
						AND tmh.media_source IN ($2, $3)
				)
				AND EXISTS (
					SELECT 1 FROM token_media_health tmh
					WHERE tmh.token_id = tokens.id
						AND tmh.health_status = $1
						AND tmh.media_source IN ($4, $5)
				) THEN true
				ELSE false
			END as is_viewable
		FROM tokens
		INNER JOIN token_media_health tmh ON tmh.token_id = tokens.id
		WHERE tmh.media_url = $6
		GROUP BY tokens.id, tokens.token_cid
	`

	var results []TokenViewabilityInfo
	err := s.db.WithContext(ctx).Raw(query,
		string(schema.MediaHealthStatusHealthy),
		string(schema.MediaHealthSourceMetadataAnimation),
		string(schema.MediaHealthSourceEnrichmentAnimation),
		string(schema.MediaHealthSourceMetadataImage),
		string(schema.MediaHealthSourceEnrichmentImage),
		url,
	).Scan(&results).Error

	if err != nil {
		return nil, fmt.Errorf("failed to get tokens viewability by media URL: %w", err)
	}

	return results, nil
}

// GetTokensViewabilityByIDs returns viewability status for a specific set of token IDs
func (s *pgStore) GetTokensViewabilityByIDs(ctx context.Context, tokenIDs []uint64) ([]TokenViewabilityInfo, error) {
	if len(tokenIDs) == 0 {
		return []TokenViewabilityInfo{}, nil
	}

	query := `
		SELECT 
			tokens.id as token_id,
			tokens.token_cid as token_cid,
			CASE 
				-- Token is viewable if it has at least one healthy animation URL
				WHEN EXISTS (
					SELECT 1 FROM token_media_health tmh
					WHERE tmh.token_id = tokens.id
						AND tmh.health_status = ?
						AND tmh.media_source IN (?, ?)
				) THEN true
				-- OR if no animation URLs exist AND has at least one healthy image URL
				WHEN NOT EXISTS (
					SELECT 1 FROM token_media_health tmh
					WHERE tmh.token_id = tokens.id 
						AND tmh.media_source IN (?, ?)
				)
				AND EXISTS (
					SELECT 1 FROM token_media_health tmh
					WHERE tmh.token_id = tokens.id
						AND tmh.health_status = ?
						AND tmh.media_source IN (?, ?)
				) THEN true
				ELSE false
			END as is_viewable
		FROM tokens
		WHERE tokens.id IN ?
	`

	var results []TokenViewabilityInfo
	err := s.db.WithContext(ctx).Raw(query,
		string(schema.MediaHealthStatusHealthy),
		string(schema.MediaHealthSourceMetadataAnimation),
		string(schema.MediaHealthSourceEnrichmentAnimation),
		string(schema.MediaHealthSourceMetadataAnimation),
		string(schema.MediaHealthSourceEnrichmentAnimation),
		string(schema.MediaHealthStatusHealthy),
		string(schema.MediaHealthSourceMetadataImage),
		string(schema.MediaHealthSourceEnrichmentImage),
		tokenIDs,
	).Scan(&results).Error

	if err != nil {
		return nil, fmt.Errorf("failed to get tokens viewability by IDs: %w", err)
	}

	return results, nil
}

// UpdateTokenMediaHealthByURL updates health status for all records with a specific URL
func (s *pgStore) UpdateTokenMediaHealthByURL(ctx context.Context, url string, status schema.MediaHealthStatus, lastError *string) error {
	updates := map[string]interface{}{
		"health_status":   status,
		"last_checked_at": time.Now(),
	}

	if lastError != nil {
		updates["last_error"] = *lastError
	} else {
		updates["last_error"] = nil
	}

	return s.db.WithContext(ctx).
		Model(&schema.TokenMediaHealth{}).
		Where("media_url = ?", url).
		Updates(updates).Error
}

// UpdateMediaURLAndPropagate updates a URL across token_media_health and source tables (metadata/enrichment) in a transaction
func (s *pgStore) UpdateMediaURLAndPropagate(ctx context.Context, oldURL string, newURL string) error {
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// 1. Update token_media_health
		if err := tx.Model(&schema.TokenMediaHealth{}).
			Where("media_url = ?", oldURL).
			Updates(map[string]interface{}{
				"media_url":       newURL,
				"health_status":   schema.MediaHealthStatusHealthy,
				"last_checked_at": time.Now(),
				"last_error":      nil,
			}).Error; err != nil {
			return fmt.Errorf("failed to update token_media_health: %w", err)
		}

		// 2. Update token_metadata.image_url
		if err := tx.Model(&schema.TokenMetadata{}).
			Where("image_url = ?", oldURL).
			Update("image_url", newURL).Error; err != nil {
			return fmt.Errorf("failed to update token_metadata.image_url: %w", err)
		}

		// 3. Update token_metadata.animation_url
		if err := tx.Model(&schema.TokenMetadata{}).
			Where("animation_url = ?", oldURL).
			Update("animation_url", newURL).Error; err != nil {
			return fmt.Errorf("failed to update token_metadata.animation_url: %w", err)
		}

		// 4. Update enrichment_sources.image_url
		if err := tx.Model(&schema.EnrichmentSource{}).
			Where("image_url = ?", oldURL).
			Update("image_url", newURL).Error; err != nil {
			return fmt.Errorf("failed to update enrichment_sources.image_url: %w", err)
		}

		// 5. Update enrichment_sources.animation_url
		if err := tx.Model(&schema.EnrichmentSource{}).
			Where("animation_url = ?", oldURL).
			Update("animation_url", newURL).Error; err != nil {
			return fmt.Errorf("failed to update enrichment_sources.animation_url: %w", err)
		}

		return nil
	})
}

// CreateTokenViewabilityChange creates a changes_journal entry for a token viewability change
func (s *pgStore) CreateTokenViewabilityChange(ctx context.Context, tokenID uint64, tokenCID string, isViewable bool) error {
	meta := schema.TokenViewabilityChangeMeta{
		TokenID:    tokenID,
		TokenCID:   tokenCID,
		IsViewable: isViewable,
	}

	metaJSON, err := json.Marshal(meta)
	if err != nil {
		return fmt.Errorf("failed to marshal viewability change meta: %w", err)
	}

	changeJournal := schema.ChangesJournal{
		SubjectType: schema.SubjectTypeTokenViewability,
		SubjectID:   fmt.Sprintf("%d", tokenID),
		ChangedAt:   time.Now(),
		Meta:        metaJSON,
	}

	// Use ON CONFLICT DO NOTHING to handle rare duplicate cases
	if err := s.db.WithContext(ctx).
		Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "subject_type"}, {Name: "subject_id"}, {Name: "changed_at"}},
			DoNothing: true,
		}).
		Create(&changeJournal).Error; err != nil {
		return fmt.Errorf("failed to create viewability change journal: %w", err)
	}

	return nil
}
