package dto

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ──────────────────────────────────────────────────────────────────────────────
// TriggerReleaseIndexingRequest.Validate
// ──────────────────────────────────────────────────────────────────────────────

func ptr[T any](v T) *T { return &v }

func TestTriggerReleaseIndexingRequest_Validate_BothEmpty(t *testing.T) {
	t.Parallel()

	r := &TriggerReleaseIndexingRequest{
		Vendor: "artblocks",
		MintTo: 10,
	}
	err := r.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "exactly one of vendor_release_id or vendor_release_slug is required")
}

func TestTriggerReleaseIndexingRequest_Validate_BothProvided(t *testing.T) {
	t.Parallel()

	r := &TriggerReleaseIndexingRequest{
		Vendor:            "artblocks",
		VendorReleaseID:   "some-id",
		VendorReleaseSlug: "some-slug",
		MintTo:            10,
	}
	err := r.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "mutually exclusive")
}

func TestTriggerReleaseIndexingRequest_Validate_WhitespaceOnlyID(t *testing.T) {
	t.Parallel()

	r := &TriggerReleaseIndexingRequest{
		Vendor:          "artblocks",
		VendorReleaseID: "   ",
		MintTo:          10,
	}
	err := r.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "exactly one of vendor_release_id or vendor_release_slug is required")
}

func TestTriggerReleaseIndexingRequest_Validate_MintFromZeroInvalidForAllVendors(t *testing.T) {
	t.Parallel()

	for _, vendor := range []string{"artblocks", "feralfile", "fxhash", "objkt"} {
		vendor := vendor
		t.Run(vendor, func(t *testing.T) {
			t.Parallel()

			r := &TriggerReleaseIndexingRequest{
				Vendor:          vendor,
				VendorReleaseID: "some-id",
				MintFrom:        ptr(int64(0)),
				MintTo:          10,
			}
			err := r.Validate()
			require.Error(t, err)
			assert.Contains(t, err.Error(), "mint_from must be")
		})
	}
}

func TestTriggerReleaseIndexingRequest_Validate_OpenSeaRejected(t *testing.T) {
	t.Parallel()

	r := &TriggerReleaseIndexingRequest{
		Vendor:          "opensea",
		VendorReleaseID: "boredapeyachtclub",
		MintFrom:        ptr(int64(1)),
		MintTo:          100,
	}
	err := r.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported vendor")
}

func TestTriggerReleaseIndexingRequest_Validate_ValidRequest(t *testing.T) {
	t.Parallel()

	r := &TriggerReleaseIndexingRequest{
		Vendor:          "artblocks",
		VendorReleaseID: "1-0xa7d8d9ef8d8ce8992df33d8b8cf4aebabd5bd270-78",
		MintFrom:        ptr(int64(1)),
		MintTo:          50,
	}
	err := r.Validate()
	require.NoError(t, err)
}
