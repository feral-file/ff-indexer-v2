package dto

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ──────────────────────────────────────────────────────────────────────────────
// TriggerReleaseIndexingRequest.Validate
// ──────────────────────────────────────────────────────────────────────────────

func TestTriggerReleaseIndexingRequest_Validate_BothEmpty(t *testing.T) {
	t.Parallel()

	r := &TriggerReleaseIndexingRequest{
		Vendor:      "artblocks",
		MintNumbers: []int64{1},
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
		MintNumbers:       []int64{1},
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
		MintNumbers:     []int64{1},
	}
	err := r.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "exactly one of vendor_release_id or vendor_release_slug is required")
}

func TestTriggerReleaseIndexingRequest_Validate_MintNumberZeroInvalidForAllVendors(t *testing.T) {
	t.Parallel()

	for _, vendor := range []string{"artblocks", "feralfile", "fxhash", "objkt"} {
		vendor := vendor
		t.Run(vendor, func(t *testing.T) {
			t.Parallel()

			r := &TriggerReleaseIndexingRequest{
				Vendor:          vendor,
				VendorReleaseID: "some-id",
				MintNumbers:     []int64{0},
			}
			err := r.Validate()
			require.Error(t, err)
			assert.Contains(t, err.Error(), "mint_number must be")
		})
	}
}

func TestTriggerReleaseIndexingRequest_Validate_DuplicateMintNumbers(t *testing.T) {
	t.Parallel()

	r := &TriggerReleaseIndexingRequest{
		Vendor:          "artblocks",
		VendorReleaseID: "some-id",
		MintNumbers:     []int64{1, 2, 1},
	}
	err := r.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "duplicate mint_number")
}

func TestTriggerReleaseIndexingRequest_Validate_EmptyMintNumbers(t *testing.T) {
	t.Parallel()

	r := &TriggerReleaseIndexingRequest{
		Vendor:          "artblocks",
		VendorReleaseID: "some-id",
		MintNumbers:     []int64{},
	}
	err := r.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "mint_numbers is required")
}

func TestTriggerReleaseIndexingRequest_Validate_OpenSeaRejected(t *testing.T) {
	t.Parallel()

	r := &TriggerReleaseIndexingRequest{
		Vendor:          "opensea",
		VendorReleaseID: "boredapeyachtclub",
		MintNumbers:     []int64{1, 2, 3},
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
		MintNumbers:     []int64{1, 2, 3, 50},
	}
	err := r.Validate()
	require.NoError(t, err)
}

func TestTriggerReleaseIndexingRequest_Validate_TooManyMintNumbers(t *testing.T) {
	t.Parallel()

	nums := make([]int64, 51)
	for i := range nums {
		nums[i] = int64(i + 1)
	}
	r := &TriggerReleaseIndexingRequest{
		Vendor:          "artblocks",
		VendorReleaseID: "some-id",
		MintNumbers:     nums,
	}
	err := r.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "too many mint_numbers")
}
