package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/store/schema"
)

// scanLogInsertBatchSize bounds one INSERT statement's row count when staging
// window logs; a window of a dense address can carry thousands of logs.
const scanLogInsertBatchSize = 500

// GetAddressScanSession returns the active scan session for (chain, address),
// or nil when none exists. At most one session exists per pair (unique constraint).
func (s *pgStore) GetAddressScanSession(ctx context.Context, chain domain.Chain, address string) (*schema.AddressScanSession, error) {
	var session schema.AddressScanSession
	err := s.db.WithContext(ctx).
		Where("chain = ? AND address = ?", chain, address).
		First(&session).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get address scan session: %w", err)
	}
	return &session, nil
}

// CreateAddressScanSession creates a scanning session with the cursor at the
// range start. If a session already exists for (chain, address) — e.g. two
// workers racing — the existing session is returned instead of an error, so
// callers always converge on the same checkpoint row.
func (s *pgStore) CreateAddressScanSession(ctx context.Context, chain domain.Chain, address string, fromBlock, toBlock uint64) (*schema.AddressScanSession, error) {
	if fromBlock > toBlock {
		return nil, fmt.Errorf("invalid scan range: from_block %d > to_block %d", fromBlock, toBlock)
	}

	session := schema.AddressScanSession{
		Chain:       chain,
		Address:     address,
		FromBlock:   fromBlock,
		ToBlock:     toBlock,
		CursorBlock: fromBlock,
		Status:      schema.AddressScanStatusScanning,
	}
	err := s.db.WithContext(ctx).
		Clauses(clause.OnConflict{DoNothing: true}).
		Create(&session).Error
	if err != nil {
		return nil, fmt.Errorf("failed to create address scan session: %w", err)
	}

	// ON CONFLICT DO NOTHING leaves ID zero when the row already existed; fetch
	// the winner either way so racers share one session.
	if session.ID == 0 {
		existing, err := s.GetAddressScanSession(ctx, chain, address)
		if err != nil {
			return nil, err
		}
		if existing == nil {
			return nil, fmt.Errorf("scan session insert conflicted but no session found for %s/%s", chain, address)
		}
		return existing, nil
	}
	return &session, nil
}

// AppendScanLogsAdvanceCursor persists one window's logs and advances the
// session cursor in a single transaction — the atomicity that makes the window
// loop resumable: a crash between fetch and commit leaves the cursor unmoved,
// and the window's re-fetch re-inserts the same rows idempotently (identity PK,
// ON CONFLICT DO NOTHING).
//
// Constraints: the cursor only moves forward (guarded in SQL), so a duplicate
// delivery of an already-committed window is a no-op.
func (s *pgStore) AppendScanLogsAdvanceCursor(ctx context.Context, sessionID int64, logs []schema.AddressScanLog, newCursor uint64) error {
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if len(logs) > 0 {
			for i := range logs {
				logs[i].SessionID = sessionID
			}
			if err := tx.Clauses(clause.OnConflict{DoNothing: true}).
				CreateInBatches(logs, scanLogInsertBatchSize).Error; err != nil {
				return fmt.Errorf("failed to stage scan logs: %w", err)
			}
		}

		result := tx.Model(&schema.AddressScanSession{}).
			Where("id = ? AND status = ? AND cursor_block < ?",
				sessionID, schema.AddressScanStatusScanning, newCursor).
			Update("cursor_block", newCursor)
		if result.Error != nil {
			return fmt.Errorf("failed to advance scan cursor: %w", result.Error)
		}
		return nil
	})
}

// GetAddressScanLogs returns all staged logs for a session in chain order
// (block, tx index, log index) for the ownership replay.
func (s *pgStore) GetAddressScanLogs(ctx context.Context, sessionID int64) ([]schema.AddressScanLog, error) {
	var logs []schema.AddressScanLog
	err := s.db.WithContext(ctx).
		Where("session_id = ?", sessionID).
		Order("block_number ASC, tx_index ASC, log_index ASC").
		Find(&logs).Error
	if err != nil {
		return nil, fmt.Errorf("failed to get scan logs: %w", err)
	}
	return logs, nil
}

// FinishAddressScanReplay persists the replayed token list, deletes the staged
// logs, and marks the session replayed — in one transaction, so a crash at any
// point leaves the session either fully 'scanning' (replay re-runs
// deterministically from the intact logs) or fully 'replayed'.
//
// Reason: the staged logs are pure intermediate state — re-derivable from chain
// and the bulkiest rows — while the token list is the artifact quota-paced
// indexing must survive on. Deleting logs any earlier would break replay
// idempotency; any later would leak them if completion never runs.
func (s *pgStore) FinishAddressScanReplay(ctx context.Context, sessionID int64, tokens []schema.AddressScanToken) error {
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if len(tokens) > 0 {
			for i := range tokens {
				tokens[i].SessionID = sessionID
			}
			if err := tx.Clauses(clause.OnConflict{DoNothing: true}).
				CreateInBatches(tokens, scanLogInsertBatchSize).Error; err != nil {
				return fmt.Errorf("failed to persist scan tokens: %w", err)
			}
		}

		if err := tx.Where("session_id = ?", sessionID).
			Delete(&schema.AddressScanLog{}).Error; err != nil {
			return fmt.Errorf("failed to delete staged scan logs: %w", err)
		}

		result := tx.Model(&schema.AddressScanSession{}).
			Where("id = ?", sessionID).
			Update("status", schema.AddressScanStatusReplayed)
		if result.Error != nil {
			return fmt.Errorf("failed to mark scan session replayed: %w", result.Error)
		}
		if result.RowsAffected == 0 {
			return fmt.Errorf("scan session %d not found", sessionID)
		}
		return nil
	})
}

// GetPendingAddressScanTokens returns the session's un-indexed tokens, newest
// blocks first (matching the indexing order the owner workflow uses).
func (s *pgStore) GetPendingAddressScanTokens(ctx context.Context, sessionID int64) ([]schema.AddressScanToken, error) {
	var tokens []schema.AddressScanToken
	err := s.db.WithContext(ctx).
		Where("session_id = ? AND indexed_at IS NULL", sessionID).
		Order("block_number DESC, token_cid ASC").
		Find(&tokens).Error
	if err != nil {
		return nil, fmt.Errorf("failed to get pending scan tokens: %w", err)
	}
	return tokens, nil
}

// MarkAddressScanTokensIndexed stamps indexed_at on the given tokens after a
// chunk lands, so quota pauses and restarts resume from the remaining rows.
func (s *pgStore) MarkAddressScanTokensIndexed(ctx context.Context, sessionID int64, tokenCIDs []domain.TokenCID) error {
	if len(tokenCIDs) == 0 {
		return nil
	}
	err := s.db.WithContext(ctx).Model(&schema.AddressScanToken{}).
		Where("session_id = ? AND token_cid IN ?", sessionID, tokenCIDs).
		Update("indexed_at", time.Now().UTC()).Error
	if err != nil {
		return fmt.Errorf("failed to mark scan tokens indexed: %w", err)
	}
	return nil
}

// DeleteAddressScanSession removes a completed session; token rows cascade.
func (s *pgStore) DeleteAddressScanSession(ctx context.Context, sessionID int64) error {
	err := s.db.WithContext(ctx).
		Delete(&schema.AddressScanSession{}, sessionID).Error
	if err != nil {
		return fmt.Errorf("failed to delete scan session: %w", err)
	}
	return nil
}
