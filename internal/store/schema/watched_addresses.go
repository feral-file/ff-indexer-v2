package schema

import (
	"database/sql/driver"
	"encoding/json"
	"time"

	"github.com/feral-file/ff-indexer-v2/internal/domain"
)

// BlockRange represents the min and max block range for indexing
type BlockRange struct {
	MinBlock uint64 `json:"min_block"`
	MaxBlock uint64 `json:"max_block"`
}

// IndexingBlockRanges represents the last successful indexing block ranges per chain
// Key is chain ID (e.g., "eip155:1", "tezos:mainnet")
type IndexingBlockRanges map[domain.Chain]BlockRange

// Scan implements sql.Scanner interface for JSONB
func (r *IndexingBlockRanges) Scan(value interface{}) error {
	if value == nil {
		*r = make(IndexingBlockRanges)
		return nil
	}
	bytes, ok := value.([]byte)
	if !ok {
		return nil
	}
	return json.Unmarshal(bytes, r)
}

// Value implements driver.Valuer interface for JSONB
func (r IndexingBlockRanges) Value() (driver.Value, error) {
	if r == nil {
		return nil, nil
	}
	return json.Marshal(r)
}

// WatchedAddresses represents the watched_addresses table - for owner-based indexing
type WatchedAddresses struct {
	// Chain identifies the blockchain network
	Chain domain.Chain `gorm:"column:chain;not null;primaryKey;type:text"`
	// Address is the blockchain address being watched
	Address string `gorm:"column:address;not null;primaryKey;type:text"`
	// Watching indicates whether this address is currently being monitored
	Watching bool `gorm:"column:watching;not null;default:true"`
	// LastQueriedAt records when the API last queried this address
	LastQueriedAt *time.Time `gorm:"column:last_queried_at;type:timestamptz"`
	// LastSuccessfulIndexingBlkRange stores the last successful indexing block ranges per chain
	// Format: {"eip155:1": {"min_block": 123, "max_block": 456}}
	LastSuccessfulIndexingBlkRange *IndexingBlockRanges `gorm:"column:last_successful_indexing_blk_range;type:jsonb"`
	// CreatedAt is when this watch entry was created
	CreatedAt time.Time `gorm:"column:created_at;not null;default:now();type:timestamptz"`
	// UpdatedAt is when this watch entry was last modified
	UpdatedAt time.Time `gorm:"column:updated_at;not null;default:now();type:timestamptz"`
}

// TableName specifies the table name for the WatchedAddresses model
func (WatchedAddresses) TableName() string {
	return "watched_addresses"
}
