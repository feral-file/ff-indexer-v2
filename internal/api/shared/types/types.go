package types

// Order enumeration for sorting
type Order string

const (
	OrderAsc  Order = "asc"
	OrderDesc Order = "desc"
)

func (o Order) Desc() bool {
	return o == OrderDesc
}

func (o Order) Asc() bool {
	return o == OrderAsc
}

// Valid checks if an order is valid
func (o Order) Valid() bool {
	return o == OrderAsc || o == OrderDesc
}

// TokenSortBy enumeration for token sorting
type TokenSortBy string

const (
	TokenSortByCreatedAt  TokenSortBy = "created_at"
	TokenLatestProvenance TokenSortBy = "latest_provenance"
)

// Valid checks if a token sort by is valid
func (t TokenSortBy) Valid() bool {
	return t == TokenSortByCreatedAt || t == TokenLatestProvenance
}

// Expansion enumeration for expansions
type Expansion string

const (
	ExpansionOwners           Expansion = "owners"
	ExpansionProvenanceEvents Expansion = "provenance_events"
	ExpansionMetadata         Expansion = "metadata"
	ExpansionEnrichmentSource Expansion = "enrichment_source"
	ExpansionMediaAsset       Expansion = "media_asset"
	ExpansionOwnerProvenances Expansion = "owner_provenances"
	ExpansionDisplay          Expansion = "display"
	// Deprecated: Use ExpansionMediaAsset instead
	ExpansionMetadataMediaAsset Expansion = "metadata_media_asset"
	// Deprecated: Use ExpansionMediaAsset instead
	ExpansionEnrichmentSourceMediaAsset Expansion = "enrichment_source_media_asset"
	ExpansionSubject                    Expansion = "subject"
)

type MediaAssetVariantKey string

const (
	MediaAssetVariantKeyXs      MediaAssetVariantKey = "xs"
	MediaAssetVariantKeyS       MediaAssetVariantKey = "s"
	MediaAssetVariantKeyM       MediaAssetVariantKey = "m"
	MediaAssetVariantKeyL       MediaAssetVariantKey = "l"
	MediaAssetVariantKeyXl      MediaAssetVariantKey = "xl"
	MediaAssetVariantKeyXxl     MediaAssetVariantKey = "xxl"
	MediaAssetVariantKeyHls     MediaAssetVariantKey = "hls"
	MediaAssetVariantKeyDash    MediaAssetVariantKey = "dash"
	MediaAssetVariantKeyPreview MediaAssetVariantKey = "preview"
)

var AllMediaAssetVariantKey = []MediaAssetVariantKey{
	MediaAssetVariantKeyXs,
	MediaAssetVariantKeyS,
	MediaAssetVariantKeyM,
	MediaAssetVariantKeyL,
	MediaAssetVariantKeyXl,
	MediaAssetVariantKeyXxl,
	MediaAssetVariantKeyHls,
	MediaAssetVariantKeyDash,
	MediaAssetVariantKeyPreview,
}
