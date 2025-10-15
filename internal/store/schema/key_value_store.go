package schema

import "time"

// KeyValueStore stores arbitrary key-value pairs for configuration and state
// Used for storing block cursors, chain states, etc.
type KeyValueStore struct {
	Key       string    `gorm:"primaryKey;type:text"`
	Value     string    `gorm:"type:text;not null"`
	UpdatedAt time.Time `gorm:"autoUpdateTime"`
	CreatedAt time.Time `gorm:"autoCreateTime"`
}

func (KeyValueStore) TableName() string {
	return "key_value_store"
}
