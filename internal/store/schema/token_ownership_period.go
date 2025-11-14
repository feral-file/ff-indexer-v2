package schema

import "time"

// TokenOwnershipPeriod represents the token_ownership_periods table - tracks when addresses owned tokens with quantity > 0
type TokenOwnershipPeriod struct {
	// ID is the internal database primary key
	ID uint64 `gorm:"column:id;primaryKey;autoIncrement"`
	// TokenID references the token this ownership period relates to
	TokenID uint64 `gorm:"column:token_id;not null"`
	// OwnerAddress is the blockchain address that owns/owned the token
	OwnerAddress string `gorm:"column:owner_address;not null;type:text"`
	// AcquiredAt is the timestamp when the address acquired the token (first transfer in)
	AcquiredAt time.Time `gorm:"column:acquired_at;not null;type:timestamptz"`
	// ReleasedAt is the timestamp when the address released the token (balance became 0)
	// NULL means the address still owns the token (balance > 0)
	ReleasedAt *time.Time `gorm:"column:released_at;type:timestamptz"`
	// CreatedAt is the timestamp when this record was created
	CreatedAt time.Time `gorm:"column:created_at;not null;default:now();type:timestamptz"`
	// UpdatedAt is the timestamp when this record was last updated
	UpdatedAt time.Time `gorm:"column:updated_at;not null;default:now();type:timestamptz"`

	// Associations
	Token Token `gorm:"foreignKey:TokenID;constraint:OnDelete:CASCADE"`
}

// TableName specifies the table name for the TokenOwnershipPeriod model
func (TokenOwnershipPeriod) TableName() string {
	return "token_ownership_periods"
}
