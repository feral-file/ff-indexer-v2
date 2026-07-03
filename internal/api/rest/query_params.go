package rest

import (
	"fmt"
	"strings"

	"github.com/gin-gonic/gin"

	"github.com/feral-file/ff-indexer-v2/internal/api/shared/constants"
	apierrors "github.com/feral-file/ff-indexer-v2/internal/api/shared/errors"
	"github.com/feral-file/ff-indexer-v2/internal/api/shared/types"
	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/store/schema"
	internalTypes "github.com/feral-file/ff-indexer-v2/internal/types"
)

// GetTokenQueryParams holds query parameters for GET /tokens/:cid
type GetTokenQueryParams struct {
	Expand []types.Expansion `form:"expand"`

	// Owners expansion parameters
	OwnerLimit  uint8  `form:"owners.limit,default=10"`
	OwnerOffset uint64 `form:"owners.offset,default=0"`

	// Provenance events expansion parameters
	ProvenanceEventLimit  uint8       `form:"provenance_events.limit,default=10"`
	ProvenanceEventOffset uint64      `form:"provenance_events.offset,default=0"`
	ProvenanceEventOrder  types.Order `form:"provenance_events.order,default=desc"`
}

// Validate validates the query parameters for GET /tokens/:cid
func (p *GetTokenQueryParams) Validate() error {
	// Validate expansions
	for _, expansion := range p.Expand {
		if expansion != types.ExpansionOwners &&
			expansion != types.ExpansionProvenanceEvents &&
			expansion != types.ExpansionMetadata &&
			expansion != types.ExpansionEnrichmentSource &&
			expansion != types.ExpansionMediaAsset &&
			expansion != types.ExpansionDisplay {
			return apierrors.NewValidationError(fmt.Sprintf("Invalid expansion: %s. Must be a valid expansion", expansion))
		}
	}

	// Validate provenance event order
	if !p.ProvenanceEventOrder.Valid() {
		return apierrors.NewValidationError(fmt.Sprintf("Invalid provenance event order: %s. Must be a valid order", p.ProvenanceEventOrder))
	}

	return nil
}

// ListTokensQueryParams holds query parameters for GET /tokens
type ListTokensQueryParams struct {
	// Filters
	Owners            []string       `form:"owner"`
	Chains            []domain.Chain `form:"chain"`
	ContractAddresses []string       `form:"contract_address"`
	TokenNumbers      []string       `form:"token_number"`
	TokenIDs          []uint64       `form:"token_id"`
	TokenCIDs         []string       `form:"token_cid"`
	ReleaseID         *uint64        `form:"release_id"`
	// MintFrom and MintTo are 1-based mint number range filters.
	// Only meaningful (and validated) when release_id is also set.
	// Clients use these to poll for indexed tokens after triggering IndexRelease.
	MintFrom          *int64 `form:"mint_from"`
	MintTo            *int64 `form:"mint_to"`
	IncludeUnviewable bool   `form:"include_unviewable,default=false"` // Include tokens with is_viewable=false

	// Pagination
	Limit  uint8  `form:"limit,default=20"`
	Offset uint64 `form:"offset,default=0"`

	// Sorting
	SortBy    types.TokenSortBy `form:"sort_by,default=latest_provenance"`
	SortOrder types.Order       `form:"sort_order,default=desc"`

	// Expansion
	Expansions []types.Expansion `form:"expand"`
}

// Validate validates the query parameters for GET /tokens
func (p *ListTokensQueryParams) Validate() error {
	// Validate owners
	for _, owner := range p.Owners {
		if !internalTypes.IsTezosAddress(owner) && !internalTypes.IsEthereumAddress(owner) {
			return apierrors.NewValidationError(fmt.Sprintf("Invalid owner: %s. Must be a valid Tezos or Ethereum address", owner))
		}
	}

	// Validate chains
	for _, chain := range p.Chains {
		if !domain.IsValidChain(chain) {
			return apierrors.NewValidationError(fmt.Sprintf("Invalid chain: %s. Must be a valid Tezos or Ethereum chain", chain))
		}
	}

	// Validate contract addresses
	for _, contractAddress := range p.ContractAddresses {
		if !internalTypes.IsTezosContractAddress(contractAddress) && !internalTypes.IsEthereumAddress(contractAddress) {
			return apierrors.NewValidationError(fmt.Sprintf("Invalid contract address: %s. Must be a valid Tezos or Ethereum address", contractAddress))
		}
	}

	// Validate token CIDs
	for _, tokenCID := range p.TokenCIDs {
		if !domain.TokenCID(tokenCID).Valid() {
			return apierrors.NewValidationError(fmt.Sprintf("Invalid token CID: %s. Must be a valid token CID", tokenCID))
		}
	}

	// Validate token numbers
	for _, tokenNumber := range p.TokenNumbers {
		if !internalTypes.IsNumeric(tokenNumber) {
			return apierrors.NewValidationError(fmt.Sprintf("Invalid token number: %s. Must be a valid positive numeric value", tokenNumber))
		}
	}

	// Validate expansions
	for _, expansion := range p.Expansions {
		if expansion != types.ExpansionOwners &&
			expansion != types.ExpansionOwnerProvenances &&
			expansion != types.ExpansionProvenanceEvents &&
			expansion != types.ExpansionMetadata &&
			expansion != types.ExpansionEnrichmentSource &&
			expansion != types.ExpansionMediaAsset &&
			expansion != types.ExpansionDisplay {
			return apierrors.NewValidationError(fmt.Sprintf("Invalid expansion: %s. Must be a valid expansion", expansion))
		}
	}

	// Validate sort_by
	if !p.SortBy.Valid() {
		return apierrors.NewValidationError(fmt.Sprintf("Invalid sort_by: %s. Must be a valid sort field", p.SortBy))
	}

	// uint64 is always non-negative; zero is the only invalid value.
	if p.ReleaseID != nil && *p.ReleaseID == 0 {
		return apierrors.NewValidationError("Invalid release_id: must be a positive integer")
	}

	if p.SortBy == types.TokenSortByMintNumber && p.ReleaseID == nil {
		return apierrors.NewValidationError("sort_by=mint_number requires release_id")
	}

	// Validate mint range filters — only valid when release_id is also provided.
	if (p.MintFrom != nil || p.MintTo != nil) && p.ReleaseID == nil {
		return apierrors.NewValidationError("mint_from and mint_to require release_id")
	}
	if p.MintFrom != nil && *p.MintFrom < 1 {
		return apierrors.NewValidationError("mint_from must be >= 1")
	}
	if p.MintFrom != nil && p.MintTo != nil && *p.MintTo < *p.MintFrom {
		return apierrors.NewValidationError("mint_to must be >= mint_from")
	}

	// Validate sort_order
	if !p.SortOrder.Valid() {
		return apierrors.NewValidationError(fmt.Sprintf("Invalid sort_order: %s. Must be a valid order", p.SortOrder))
	}

	return nil
}

// ParseGetTokenQuery parses query parameters for GET /tokens/:cid
func ParseGetTokenQuery(c *gin.Context) (*GetTokenQueryParams, error) {
	var params GetTokenQueryParams
	if err := c.ShouldBindQuery(&params); err != nil {
		return nil, err
	}

	// Cap limits
	if params.OwnerLimit > constants.MAX_PAGE_SIZE {
		params.OwnerLimit = constants.MAX_PAGE_SIZE
	}
	if params.ProvenanceEventLimit > constants.MAX_PAGE_SIZE {
		params.ProvenanceEventLimit = constants.MAX_PAGE_SIZE
	}

	// Validate order
	if !params.ProvenanceEventOrder.Asc() && !params.ProvenanceEventOrder.Desc() {
		params.ProvenanceEventOrder = types.OrderDesc
	}

	return &params, nil
}

// ParseListTokensQuery parses query parameters for GET /tokens.
//
// Invalid sort_by and sort_order values are preserved as-is so that the
// subsequent Validate() call can return an actionable client error. Silently
// rewriting invalid values to defaults (the previous behavior) masked typos
// and was inconsistent with the release-member endpoint, which already lets
// validation reject unrecognized sort_order values.
func ParseListTokensQuery(c *gin.Context) (*ListTokensQueryParams, error) {
	var params ListTokensQueryParams
	if err := c.ShouldBindQuery(&params); err != nil {
		return nil, err
	}

	// Normalize addresses
	params.Owners = domain.NormalizeAddresses(params.Owners)
	params.ContractAddresses = domain.NormalizeAddresses(params.ContractAddresses)

	return &params, nil
}

// GetReleaseQueryParams holds query parameters for GET /releases/:id
type GetReleaseQueryParams struct {
	Limit      uint8             `form:"limit,default=20"`
	Offset     uint64            `form:"offset,default=0"`
	SortOrder  types.Order       `form:"sort_order,default=asc"`
	Expansions []types.Expansion `form:"expand"`
}

// Validate validates the query parameters for GET /releases/:id
func (p *GetReleaseQueryParams) Validate() error {
	// Limit must be at least 1; zero would stall cursor pagination.
	if p.Limit == 0 {
		return apierrors.NewValidationError("Invalid limit: must be at least 1")
	}

	for _, expansion := range p.Expansions {
		if expansion != types.ExpansionOwners &&
			expansion != types.ExpansionOwnerProvenances &&
			expansion != types.ExpansionProvenanceEvents &&
			expansion != types.ExpansionMetadata &&
			expansion != types.ExpansionEnrichmentSource &&
			expansion != types.ExpansionMediaAsset &&
			expansion != types.ExpansionDisplay {
			return apierrors.NewValidationError(fmt.Sprintf("Invalid expansion: %s. Must be a valid expansion", expansion))
		}
	}

	// Reject unrecognized sort_order values rather than silently rewriting them.
	if !p.SortOrder.Valid() {
		return apierrors.NewValidationError(fmt.Sprintf("Invalid sort_order: %s. Must be 'asc' or 'desc'", p.SortOrder))
	}

	return nil
}

// ParseGetReleaseQuery parses query parameters for GET /releases/:id
func ParseGetReleaseQuery(c *gin.Context) (*GetReleaseQueryParams, error) {
	var params GetReleaseQueryParams
	if err := c.ShouldBindQuery(&params); err != nil {
		return nil, err
	}

	if params.Limit > constants.MAX_PAGE_SIZE {
		params.Limit = constants.MAX_PAGE_SIZE
	}

	return &params, nil
}

// ListReleasesQueryParams holds query parameters for GET /releases.
// Call Validate() before using ParsedVendor, ParsedVendorReleaseID, ParsedVendorReleaseSlug, and ParsedIDs.
type ListReleasesQueryParams struct {
	// IDs is a repeated query parameter: ?ids=1&ids=2
	IDs               []uint64 `form:"ids"`
	Vendor            string   `form:"vendor"`
	VendorReleaseID   string   `form:"vendor_release_id"`
	VendorReleaseSlug string   `form:"vendor_release_slug"`
	Limit             uint8    `form:"limit,default=20"`
	Offset            uint64   `form:"offset,default=0"`

	// Parsed fields are populated by Validate().
	ParsedIDs               []uint64
	ParsedVendor            *schema.Vendor
	ParsedVendorReleaseID   *string
	ParsedVendorReleaseSlug *string
}

// Validate validates the query parameters for GET /releases.
// It populates ParsedVendor, ParsedVendorReleaseID, ParsedVendorReleaseSlug, and ParsedIDs on success.
// At least one of ids, vendor, vendor_release_id, or vendor_release_slug is required.
// Accepted vendor values: artblocks, feralfile, fxhash, objkt.
func (p *ListReleasesQueryParams) Validate() error {
	vendor := strings.TrimSpace(p.Vendor)
	vendorReleaseID := strings.TrimSpace(p.VendorReleaseID)
	vendorReleaseSlug := strings.TrimSpace(p.VendorReleaseSlug)

	if len(p.IDs) == 0 && vendor == "" && vendorReleaseID == "" && vendorReleaseSlug == "" {
		return apierrors.NewValidationError("at least one of ids, vendor, vendor_release_id, or vendor_release_slug is required")
	}

	if p.Limit == 0 {
		return apierrors.NewValidationError("Invalid limit: must be at least 1")
	}

	for _, id := range p.IDs {
		if id == 0 {
			return apierrors.NewValidationError("invalid id in ids: must be a positive integer")
		}
	}
	if len(p.IDs) > 0 {
		p.ParsedIDs = p.IDs
	}

	if vendor != "" {
		v := schema.Vendor(strings.ToLower(vendor))
		switch v {
		case schema.VendorArtBlocks, schema.VendorFeralFile, schema.VendorFXHash, schema.VendorObjkt:
			p.ParsedVendor = &v
		default:
			return apierrors.NewValidationError(fmt.Sprintf("invalid vendor: %s. Must be one of: artblocks, feralfile, fxhash, objkt", vendor))
		}
	}

	if vendorReleaseID != "" {
		p.ParsedVendorReleaseID = &vendorReleaseID
	}

	if vendorReleaseSlug != "" {
		p.ParsedVendorReleaseSlug = &vendorReleaseSlug
	}

	return nil
}

// ParseListReleasesQuery parses query parameters for GET /releases.
func ParseListReleasesQuery(c *gin.Context) (*ListReleasesQueryParams, error) {
	var params ListReleasesQueryParams
	if err := c.ShouldBindQuery(&params); err != nil {
		return nil, err
	}

	if params.Limit > constants.MAX_PAGE_SIZE {
		params.Limit = constants.MAX_PAGE_SIZE
	}

	return &params, nil
}
