package schema

import (
	"time"
)

// Balance represents the balances table - tracks ownership quantities for multi-edition tokens (ERC1155, FA2)
type Balance struct {
	// ID is the internal database primary key
	ID int64 `gorm:"column:id;primaryKey;autoIncrement"`
	// TokenID references the token being owned
	TokenID int64 `gorm:"column:token_id;not null;uniqueIndex:idx_balances_token_owner,priority:1"`
	// OwnerAddress is the blockchain address of the owner
	OwnerAddress string `gorm:"column:owner_address;not null;type:text;uniqueIndex:idx_balances_token_owner,priority:2"`
	// Quantity is the number of editions owned (stored as string to support up to 78 digits for blockchain compatibility)
	Quantity string `gorm:"column:quantity;not null;type:numeric(78,0)"`
	// CreatedAt is the timestamp when this balance was created
	CreatedAt time.Time `gorm:"column:created_at;not null;default:now();type:timestamptz"`
	// UpdatedAt is the timestamp when this balance was last updated
	UpdatedAt time.Time `gorm:"column:updated_at;not null;default:now();type:timestamptz"`

	// Associations
	Token Token `gorm:"foreignKey:TokenID;constraint:OnDelete:CASCADE"`
}

// TableName specifies the table name for the Balance model
func (Balance) TableName() string {
	return "balances"
}
