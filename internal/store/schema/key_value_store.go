package schema

import "time"

// KeyValueStore stores arbitrary key-value pairs for configuration and state
// Used for storing block cursors, chain states, etc.
type KeyValueStore struct {
	Key       string    `gorm:"column:\"key\";primaryKey;type:text"`
	Value     string    `gorm:"column:value;type:text;not null"`
	UpdatedAt time.Time `gorm:"column:updated_at;not null;default:now();type:timestamptz"`
	CreatedAt time.Time `gorm:"column:created_at;not null;default:now();type:timestamptz"`
}

func (KeyValueStore) TableName() string {
	return "key_value_store"
}
