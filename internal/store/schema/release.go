package schema

import "time"

// Release represents the releases table — a cross-vendor series/project abstraction.
type Release struct {
	// ID is the stable internal release identifier.
	ID int64 `gorm:"column:id;primaryKey"`
	// Vendor identifies the source platform (artblocks, feralfile).
	Vendor Vendor `gorm:"column:vendor;not null;type:text"`
	// VendorReleaseID is the external release key (FF seriesID UUID or AB {contract}-{projectID}).
	VendorReleaseID string `gorm:"column:vendor_release_id;not null;type:text"`
	// CreatedAt is when this release row was first created.
	CreatedAt time.Time `gorm:"column:created_at;not null;default:now();type:timestamptz"`
	// UpdatedAt is when this release row was last updated.
	UpdatedAt time.Time `gorm:"column:updated_at;not null;default:now();type:timestamptz"`
}

// TableName specifies the table name for the Release model.
func (Release) TableName() string {
	return "releases"
}

// ReleaseMember represents the release_members table — a token's membership in a release.
type ReleaseMember struct {
	// ID is the internal row identifier.
	ID int64 `gorm:"column:id;primaryKey"`
	// ReleaseID references the parent release.
	ReleaseID int64 `gorm:"column:release_id;not null"`
	// TokenID references the member token.
	TokenID uint64 `gorm:"column:token_id;not null"`
	// MintNumber is the authoritative 1-based mint/edition order within the release.
	MintNumber int64 `gorm:"column:mint_number;not null"`
	// CreatedAt is when this membership row was first created.
	CreatedAt time.Time `gorm:"column:created_at;not null;default:now();type:timestamptz"`

	// Associations
	Release Release `gorm:"foreignKey:ReleaseID;constraint:OnDelete:CASCADE"`
	Token   Token   `gorm:"foreignKey:TokenID;constraint:OnDelete:CASCADE"`
}

// TableName specifies the table name for the ReleaseMember model.
func (ReleaseMember) TableName() string {
	return "release_members"
}
