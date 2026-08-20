package workflows

import (
	"context"
	"fmt"

	"github.com/ethereum/go-ethereum/common"
	ethtypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/lib/pq"

	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/store/schema"
)

// ScanSessionInfo is the workflow-facing view of a checkpointed owner-scan
// session (see docs/address_scan_sessions.md). The workflow drives the window
// loop from CursorBlock and the raw staged logs never cross this boundary.
type ScanSessionInfo struct {
	ID          int64
	FromBlock   uint64
	ToBlock     uint64
	CursorBlock uint64
	// Replayed is true once the token list is persisted and the staged logs are
	// deleted; the remaining work is quota-paced indexing of the pending tokens.
	Replayed bool
}

func scanSessionInfoFromSchema(session *schema.AddressScanSession) *ScanSessionInfo {
	if session == nil {
		return nil
	}
	return &ScanSessionInfo{
		ID:          session.ID,
		FromBlock:   session.FromBlock,
		ToBlock:     session.ToBlock,
		CursorBlock: session.CursorBlock,
		Replayed:    session.Status == schema.AddressScanStatusReplayed,
	}
}

// GetEthereumScanSession returns the active owner-scan session for the address,
// or nil when none exists.
func (e *coreExecutor) GetEthereumScanSession(ctx context.Context, address string, chainID domain.Chain) (*ScanSessionInfo, error) {
	session, err := e.store.GetAddressScanSession(ctx, chainID, address)
	if err != nil {
		return nil, err
	}
	return scanSessionInfoFromSchema(session), nil
}

// CreateEthereumScanSession creates a scanning session for the block range, or
// returns the existing session when a concurrent worker created one first.
func (e *coreExecutor) CreateEthereumScanSession(ctx context.Context, address string, chainID domain.Chain, fromBlock, toBlock uint64) (*ScanSessionInfo, error) {
	session, err := e.store.CreateAddressScanSession(ctx, chainID, address, fromBlock, toBlock)
	if err != nil {
		return nil, err
	}
	return scanSessionInfoFromSchema(session), nil
}

// ScanEthereumOwnerWindow fetches one window of merged owner logs from the chain
// and persists them together with the cursor advance in one transaction — the
// checkpoint that bounds the loss of any failure to this window's RPC calls.
func (e *coreExecutor) ScanEthereumOwnerWindow(ctx context.Context, address string, sessionID int64, fromBlock, toBlock uint64) error {
	logs, err := e.ethClient.FetchOwnerLogsWindow(ctx, address, fromBlock, toBlock)
	if err != nil {
		return fmt.Errorf("fetch owner logs window [%d, %d]: %w", fromBlock, toBlock, err)
	}

	rows := make([]schema.AddressScanLog, len(logs))
	for i, vLog := range logs {
		rows[i] = scanLogRowFromEthLog(vLog)
	}
	if err := e.store.AppendScanLogsAdvanceCursor(ctx, sessionID, rows, toBlock+1); err != nil {
		return fmt.Errorf("persist scan window [%d, %d]: %w", fromBlock, toBlock, err)
	}
	return nil
}

// ReplayEthereumScanSession derives the owned-token list from the session's
// staged logs (receipt repairs + unified ownership replay, blacklist applied)
// and persists it, deleting the staged logs in the same transaction. Returns
// the number of discovered tokens.
//
// Constraints: idempotent — a crash before the final transaction leaves the
// logs intact and the session 'scanning', so the replay re-runs
// deterministically on resume.
func (e *coreExecutor) ReplayEthereumScanSession(ctx context.Context, address string, sessionID int64) (int, error) {
	rows, err := e.store.GetAddressScanLogs(ctx, sessionID)
	if err != nil {
		return 0, err
	}

	logs := make([]ethtypes.Log, len(rows))
	for i, row := range rows {
		logs[i] = ethLogFromScanLogRow(row)
	}

	tokens, err := e.ethClient.DiscoverOwnedTokensFromLogs(ctx, address, logs, e.blacklist)
	if err != nil {
		return 0, err
	}

	tokenRows := make([]schema.AddressScanToken, len(tokens))
	for i, token := range tokens {
		tokenRows[i] = schema.AddressScanToken{
			TokenCID:    token.TokenCID,
			BlockNumber: token.BlockNumber,
		}
	}
	if err := e.store.FinishAddressScanReplay(ctx, sessionID, tokenRows); err != nil {
		return 0, err
	}
	return len(tokens), nil
}

// GetPendingScanTokens returns the session's un-indexed tokens, newest blocks first.
func (e *coreExecutor) GetPendingScanTokens(ctx context.Context, sessionID int64) ([]domain.TokenWithBlock, error) {
	rows, err := e.store.GetPendingAddressScanTokens(ctx, sessionID)
	if err != nil {
		return nil, err
	}
	tokens := make([]domain.TokenWithBlock, len(rows))
	for i, row := range rows {
		tokens[i] = domain.TokenWithBlock{TokenCID: row.TokenCID, BlockNumber: row.BlockNumber}
	}
	return tokens, nil
}

// MarkScanTokensIndexed stamps tokens indexed after a chunk lands, so quota
// pauses and restarts resume from the remaining rows with zero re-scan RPC.
func (e *coreExecutor) MarkScanTokensIndexed(ctx context.Context, sessionID int64, tokenCIDs []domain.TokenCID) error {
	return e.store.MarkAddressScanTokensIndexed(ctx, sessionID, tokenCIDs)
}

// DeleteEthereumScanSession removes a completed session; its token rows cascade.
func (e *coreExecutor) DeleteEthereumScanSession(ctx context.Context, sessionID int64) error {
	return e.store.DeleteAddressScanSession(ctx, sessionID)
}

// scanLogRowFromEthLog converts a go-ethereum log to its staged-row form.
// Everything the ownership replay and the CryptoPunks receipt repair read is
// preserved: emitting address, topics, data, block/tx/log position.
func scanLogRowFromEthLog(vLog ethtypes.Log) schema.AddressScanLog {
	topics := make(pq.StringArray, len(vLog.Topics))
	for i, topic := range vLog.Topics {
		topics[i] = topic.Hex()
	}
	return schema.AddressScanLog{
		BlockNumber: vLog.BlockNumber,
		TxHash:      vLog.TxHash.Hex(),
		LogIndex:    vLog.Index,
		Address:     vLog.Address.Hex(),
		Topics:      topics,
		Data:        vLog.Data,
		TxIndex:     vLog.TxIndex,
		BlockHash:   vLog.BlockHash.Hex(),
	}
}

// ethLogFromScanLogRow restores a staged row to the go-ethereum log shape the
// replay consumes.
func ethLogFromScanLogRow(row schema.AddressScanLog) ethtypes.Log {
	topics := make([]common.Hash, len(row.Topics))
	for i, topic := range row.Topics {
		topics[i] = common.HexToHash(topic)
	}
	return ethtypes.Log{
		Address:     common.HexToAddress(row.Address),
		Topics:      topics,
		Data:        row.Data,
		BlockNumber: row.BlockNumber,
		TxHash:      common.HexToHash(row.TxHash),
		TxIndex:     row.TxIndex,
		BlockHash:   common.HexToHash(row.BlockHash),
		Index:       row.LogIndex,
	}
}
