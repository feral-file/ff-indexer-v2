package schema

import (
	"time"
)

// SpamSource identifies who published a spam verdict for a token.
//
// A source is either a moderating vendor (OpenSea, objkt) or Feral File's own
// moderation system. The set is deliberately NOT the same as Vendor: most
// enrichment vendors (ArtBlocks, fxhash, ...) publish no moderation signal and
// therefore never appear here, and 'feralfile' as a spam source means the future
// FF moderation system (user reports, operator decisions), not the feralfile
// enrichment vendor.
type SpamSource string

const (
	// SpamSourceOpenSea is OpenSea's moderation verdict (NFT API is_disabled).
	SpamSourceOpenSea SpamSource = "opensea"
	// SpamSourceObjkt is objkt's moderation verdict (token flag=banned).
	SpamSourceObjkt SpamSource = "objkt"
	// SpamSourceFeralFile is Feral File's own moderation verdict. Reserved: the
	// recompute rule already gives it absolute precedence over vendor sources
	// (in both directions — true pins spam, false whitelists), but nothing
	// writes it yet. The future FF moderation system slots in as just another
	// writer with no schema change.
	SpamSourceFeralFile SpamSource = "feralfile"
)

// String returns the string representation of the spam source
func (s SpamSource) String() string {
	return string(s)
}

// SpamSourceForVendor maps an enrichment vendor to its spam verdict source.
//
// ok is false for vendors that publish no moderation signal (ArtBlocks, fxhash,
// Foundation, SuperRare, and the feralfile enrichment vendor) — their
// enrichments must not create verdict rows, because absence of a row is the
// tri-state "no opinion" and is deliberately distinct from a clean verdict.
func SpamSourceForVendor(v Vendor) (SpamSource, bool) {
	switch v {
	case VendorOpenSea:
		return SpamSourceOpenSea, true
	case VendorObjkt:
		return SpamSourceObjkt, true
	default:
		return "", false
	}
}

// TokenSpamVerdict represents the token_spam_verdicts table
//
// Source of truth for spam moderation: one row per (token, source). Rows exist
// only after a source has actually published a verdict — absence means "no
// opinion", deliberately distinct from a clean verdict. tokens.is_spam is the
// materialized combination, recomputed transactionally on every verdict write.
type TokenSpamVerdict struct {
	// TokenID is the foreign key to tokens table (composite PK with Source)
	TokenID uint64 `gorm:"column:token_id;primaryKey"`

	// Source is who published this verdict (composite PK with TokenID)
	Source SpamSource `gorm:"column:source;primaryKey;type:spam_source"`

	// Verdict is the source's spam decision: true = spam
	Verdict bool `gorm:"column:verdict;not null"`

	// Detail carries the raw moderation fields only ({"is_disabled":true} /
	// {"flag":"banned"}); the full vendor payload lives in enrichment_sources.vendor_json
	Detail []byte `gorm:"column:detail;type:jsonb"`

	// LastCheckedAt is the last time the source CONFIRMED the stored verdict.
	// Failed checks do not touch it — an error is not a verdict (tri-state).
	LastCheckedAt time.Time `gorm:"column:last_checked_at;not null;default:now();type:timestamptz"`

	// NextCheckAt is when the spam sweeper should re-check this source.
	// NULL = never swept (feralfile rows).
	NextCheckAt *time.Time `gorm:"column:next_check_at;type:timestamptz"`

	// ConsecutiveFailures counts failed sweeper checks since the last success,
	// driving the sweeper's error backoff. Reset to 0 on every successful check.
	ConsecutiveFailures int `gorm:"column:consecutive_failures;not null;default:0"`

	// LastError stores the error message from the last failed check (NULL after a successful one)
	LastError *string `gorm:"column:last_error;type:text"`

	// Timestamps
	CreatedAt time.Time `gorm:"column:created_at;not null;default:now();type:timestamptz"`
	UpdatedAt time.Time `gorm:"column:updated_at;not null;default:now();type:timestamptz"`
}

// TableName specifies the table name for the TokenSpamVerdict model
func (TokenSpamVerdict) TableName() string {
	return "token_spam_verdicts"
}
