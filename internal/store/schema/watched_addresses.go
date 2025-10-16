package schema

import "time"

// WatchedAddresses represents the watched_addresses table - for owner-based indexing
type WatchedAddresses struct {
	// Chain identifies the blockchain network
	Chain string `gorm:"column:chain;not null;primaryKey;type:text"`
	// Address is the blockchain address being watched
	Address string `gorm:"column:address;not null;primaryKey;type:text"`
	// Watching indicates whether this address is currently being monitored
	Watching bool `gorm:"column:watching;not null;default:true"`
	// AddedBy identifies who or what added this address to the watch list
	AddedBy string `gorm:"column:added_by;not null;type:text"`
	// Reason provides context for why this address is being watched
	Reason *string `gorm:"column:reason;type:text"`
	// LastQueriedAt records when the API last queried this address
	LastQueriedAt *time.Time `gorm:"column:last_queried_at;type:timestamptz"`
	// CreatedAt is when this watch entry was created
	CreatedAt time.Time `gorm:"column:created_at;not null;default:now();type:timestamptz"`
	// UpdatedAt is when this watch entry was last modified
	UpdatedAt time.Time `gorm:"column:updated_at;not null;default:now();type:timestamptz"`
}

// TableName specifies the table name for the WatchedAddresses model
func (WatchedAddresses) TableName() string {
	return "watched_addresses"
}
