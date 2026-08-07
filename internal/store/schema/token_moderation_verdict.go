package schema

import (
	"time"
)

// ModerationStatus is a moderation verdict value: what a source decided about a
// token, and — once combined across sources — what tokens.moderation_status
// holds.
//
// An enum rather than a boolean so new verdict kinds (nsfw, copyright takedown,
// ...) become additional values instead of another column and another table.
// Only "none" and "spam" exist today; adding one means extending this type,
// moderationStatusSeverity below, and the PG enum — no structural change.
type ModerationStatus string

const (
	// ModerationStatusNone is an affirmative "nothing to act on". On a verdict
	// row it means the source checked and found the token clean, which is
	// deliberately distinct from having no row at all ("no opinion").
	ModerationStatusNone ModerationStatus = "none"
	// ModerationStatusSpam marks unsolicited airdrop scams and the vendor
	// takedowns that stand in for them (OpenSea is_disabled, objkt banned).
	// Read paths hide these unless include_moderated is requested.
	ModerationStatusSpam ModerationStatus = "spam"
)

// moderationStatusSeverity orders verdicts so that combining several sources has
// a defined answer: the most severe vendor verdict wins. Ordering, not a boolean
// OR, because once a second non-none verdict exists the combination has to pick
// one, and "most severe" is the only rule that cannot end up hiding a token less
// than some source asked for.
//
// A status missing from this map ranks 0 — it loses to any verdict this binary
// understands rather than silently outranking them. That affects only which
// verdict is materialized; whether the result hides the token is IsModerated.
var moderationStatusSeverity = map[ModerationStatus]int{
	ModerationStatusNone: 0,
	ModerationStatusSpam: 1,
}

// Severity returns the status's rank for combining sources; see moderationStatusSeverity.
func (s ModerationStatus) Severity() int {
	return moderationStatusSeverity[s]
}

// IsModerated reports whether this status hides the token from default read
// paths. Only "none" stays visible — every verdict hides, so a moderation kind
// added later is filtered by default rather than leaking until someone updates
// this check.
//
// The empty string reads as not moderated. It is never a stored value (the
// column is NOT NULL DEFAULT 'none'), only reachable on a Go struct nobody
// populated, and an unset field is the absence of a verdict rather than one —
// letting it hide a token would invert this feature's fail-open stance in
// exactly the case where nothing was ever decided.
func (s ModerationStatus) IsModerated() bool {
	return s != "" && s != ModerationStatusNone
}

// String returns the string representation of the moderation status.
func (s ModerationStatus) String() string {
	return string(s)
}

// ModerationSource identifies who published a moderation verdict for a token.
//
// A source is either a moderating vendor (OpenSea, objkt) or Feral File's own
// moderation system. The set is deliberately NOT the same as Vendor: most
// enrichment vendors (ArtBlocks, fxhash, ...) publish no moderation signal and
// therefore never appear here, and 'feralfile' as a moderation source means the
// future FF moderation system (user reports, operator decisions), not the
// feralfile enrichment vendor.
type ModerationSource string

const (
	// ModerationSourceOpenSea is OpenSea's moderation verdict (NFT API is_disabled).
	ModerationSourceOpenSea ModerationSource = "opensea"
	// ModerationSourceObjkt is objkt's moderation verdict (token flag: the
	// takedown states "banned" and "removed" both count as spam; see
	// objkt.Token.IsSpam).
	ModerationSourceObjkt ModerationSource = "objkt"
	// ModerationSourceFeralFile is Feral File's own moderation verdict. Reserved: the
	// recompute rule already gives it absolute precedence over vendor sources
	// (in both directions — a non-none status pins the token, none whitelists
	// it against vendors), but nothing writes it yet. The future FF moderation
	// system slots in as just another writer with no schema change.
	ModerationSourceFeralFile ModerationSource = "feralfile"
)

// String returns the string representation of the moderation source
func (s ModerationSource) String() string {
	return string(s)
}

// ModerationSourceForVendor maps an enrichment vendor to its moderation verdict source.
//
// ok is false for vendors that publish no moderation signal (ArtBlocks, fxhash,
// Foundation, SuperRare, and the feralfile enrichment vendor) — their
// enrichments must not create verdict rows, because absence of a row is the
// tri-state "no opinion" and is deliberately distinct from a clean verdict.
func ModerationSourceForVendor(v Vendor) (ModerationSource, bool) {
	switch v {
	case VendorOpenSea:
		return ModerationSourceOpenSea, true
	case VendorObjkt:
		return ModerationSourceObjkt, true
	default:
		return "", false
	}
}

// TokenModerationVerdict represents the token_moderation_verdicts table
//
// Source of truth for token moderation: one row per (token, source). Rows exist
// only after a source has actually published a verdict — absence means "no
// opinion", deliberately distinct from a "none" verdict. tokens.moderation_status
// is the materialized combination, recomputed transactionally on every write.
type TokenModerationVerdict struct {
	// TokenID is the foreign key to tokens table (composite PK with Source)
	TokenID uint64 `gorm:"column:token_id;primaryKey"`

	// Source is who published this verdict (composite PK with TokenID)
	Source ModerationSource `gorm:"column:source;primaryKey;type:moderation_source"`

	// Verdict is this source's moderation decision ("none" | "spam"); see
	// ModerationStatus for why it is an enum rather than a boolean.
	Verdict ModerationStatus `gorm:"column:verdict;not null;type:moderation_status"`

	// Detail carries the raw moderation fields only ({"is_disabled":true} /
	// {"flag":"banned"}); the full vendor payload lives in enrichment_sources.vendor_json
	Detail []byte `gorm:"column:detail;type:jsonb"`

	// LastCheckedAt is the last time the source CONFIRMED the stored verdict.
	// Failed checks do not touch it — an error is not a verdict (tri-state).
	LastCheckedAt time.Time `gorm:"column:last_checked_at;not null;default:now();type:timestamptz"`

	// NextCheckAt is when the moderation sweeper should re-check this source.
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

// TableName specifies the table name for the TokenModerationVerdict model
func (TokenModerationVerdict) TableName() string {
	return "token_moderation_verdicts"
}
