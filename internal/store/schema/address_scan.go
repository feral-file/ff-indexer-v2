package schema

import (
	"time"

	"github.com/lib/pq"

	"github.com/feral-file/ff-indexer-v2/internal/domain"
)

// AddressScanSessionStatus represents the lifecycle stage of an owner-scan session.
type AddressScanSessionStatus string

const (
	// AddressScanStatusScanning means the window loop is fetching logs; cursor_block
	// is the resume point.
	AddressScanStatusScanning AddressScanSessionStatus = "scanning"
	// AddressScanStatusReplayed means the token list is persisted and the staged
	// logs are deleted; indexing consumes the pending tokens.
	AddressScanStatusReplayed AddressScanSessionStatus = "replayed"
)

// AddressScanSession represents the address_scan_sessions table: checkpointed
// progress of one Ethereum owner scan. One active session per (chain, address);
// completed sessions are deleted. See docs/address_scan_sessions.md.
type AddressScanSession struct {
	// ID is the internal database primary key
	ID int64 `gorm:"column:id;primaryKey"`
	// Chain is the blockchain network being scanned
	Chain domain.Chain `gorm:"column:chain;not null;type:blockchain_chain"`
	// Address is the owner address being scanned
	Address string `gorm:"column:address;not null"`
	// FromBlock is the inclusive lower bound of the scanned range
	FromBlock uint64 `gorm:"column:from_block;not null"`
	// ToBlock is the inclusive upper bound of the scanned range
	ToBlock uint64 `gorm:"column:to_block;not null"`
	// CursorBlock is the next un-fetched block; > ToBlock means fetching is complete
	CursorBlock uint64 `gorm:"column:cursor_block;not null"`
	// Status is the session lifecycle stage
	Status AddressScanSessionStatus `gorm:"column:status;not null;type:address_scan_session_status"`
	// CreatedAt is the timestamp when the session was created
	CreatedAt time.Time `gorm:"column:created_at;not null;default:now()"`
	// UpdatedAt is the timestamp when the session was last updated
	UpdatedAt time.Time `gorm:"column:updated_at;not null;default:now()"`
}

// TableName overrides the GORM table name.
func (AddressScanSession) TableName() string { return "address_scan_sessions" }

// AddressScanLog represents the address_scan_logs table: one raw owner-scoped log
// staged during the window loop. The composite identity primary key makes window
// re-fetch after a crash idempotent. Rows are deleted when the session replays.
type AddressScanLog struct {
	// SessionID references the owning scan session
	SessionID int64 `gorm:"column:session_id;primaryKey"`
	// BlockNumber is the block containing the log
	BlockNumber uint64 `gorm:"column:block_number;primaryKey"`
	// TxHash is the transaction hash containing the log
	TxHash string `gorm:"column:tx_hash;primaryKey"`
	// LogIndex is the log's index within the block
	LogIndex uint `gorm:"column:log_index;primaryKey"`
	// Address is the emitting contract address
	Address string `gorm:"column:address;not null"`
	// Topics are the log's topic hashes as 0x-prefixed hex strings, in topic order
	Topics pq.StringArray `gorm:"column:topics;not null;type:text[]"`
	// Data is the log's raw data payload
	Data []byte `gorm:"column:data"`
	// TxIndex is the transaction's index within the block
	TxIndex uint `gorm:"column:tx_index;not null;default:0"`
	// BlockHash is the hash of the block containing the log
	BlockHash string `gorm:"column:block_hash"`
	// BlockTimestamp is the Unix block time carried on the log when the window
	// was served by the log warehouse (migration 029); 0 = unknown, in which
	// case the replay resolves it through the block provider as before.
	BlockTimestamp uint64 `gorm:"column:block_timestamp;not null;default:0"`
}

// TableName overrides the GORM table name.
func (AddressScanLog) TableName() string { return "address_scan_logs" }

// AddressScanToken represents the address_scan_tokens table: the durable discovery
// result of a replayed scan session. indexed_at IS NULL means the token is still
// pending indexing, so daily-quota resumes continue here with zero re-scan RPC.
type AddressScanToken struct {
	// SessionID references the owning scan session
	SessionID int64 `gorm:"column:session_id;primaryKey"`
	// TokenCID is the discovered token's chain-scoped identifier
	TokenCID domain.TokenCID `gorm:"column:token_cid;primaryKey"`
	// BlockNumber is the token's last ownership-affecting block (drives block-aligned chunking)
	BlockNumber uint64 `gorm:"column:block_number;not null"`
	// IndexedAt is when the token was indexed; NULL means pending
	IndexedAt *time.Time `gorm:"column:indexed_at"`
}

// TableName overrides the GORM table name.
func (AddressScanToken) TableName() string { return "address_scan_tokens" }
