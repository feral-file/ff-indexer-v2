package metadata_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/metadata"
	"github.com/feral-file/ff-indexer-v2/internal/providers/vendors/fxhash"
	"github.com/feral-file/ff-indexer-v2/internal/providers/vendors/objkt"
	"github.com/feral-file/ff-indexer-v2/internal/providers/vendors/opensea"
	"github.com/feral-file/ff-indexer-v2/internal/registry"
	"github.com/feral-file/ff-indexer-v2/internal/store/schema"
	"github.com/feral-file/ff-indexer-v2/internal/types"
)

// shippedPublisherRegistry is the registry file the repo ships (and ff-deploy copies).
const shippedPublisherRegistry = "../../tools/registry/publisher.json"

// loadPublisherRegistry loads a registry file through the production loader.
func loadPublisherRegistry(t *testing.T, path string) registry.PublisherRegistry {
	t.Helper()
	reg, err := registry.NewPublisherRegistryLoader(adapter.NewFileSystem(), adapter.NewJSON()).Load(path)
	require.NoError(t, err)
	return reg
}

// expectOpenSeaToken stubs an OpenSea NFT with a numbered name in the given collection,
// so the OpenSea branch builds a release before the shared-contract rule applies.
func expectOpenSeaToken(m *testEnhancerMocks, collection string) {
	name := "Untitled #3"
	nft := &opensea.NFTMetadata{Identifier: "3", Collection: collection, Name: &name}
	m.openseaClient.EXPECT().GetNFT(gomock.Any(), gomock.Any(), "3").Return(nft, nil)
	m.openseaClient.EXPECT().GetCollection(gomock.Any(), collection).
		Return(&opensea.CollectionMetadata{Collection: collection, Name: collection, TotalSupply: 100}, nil).AnyTimes()
	m.json.EXPECT().Marshal(nft).Return([]byte(`{}`), nil)
}

// ff-indexer-v2 #176: OpenSea files every token on a platform's shared minting contract
// under one collection, so its release grouped unrelated works by different artists as
// one series. On every shared contract in the shipped registry, whether the token
// reaches OpenSea as a known publisher's designated vendor (SuperRare, Foundation) or
// through the generic fallback, the token is enriched but gets no release.
func TestEnhancer_Enhance_SharedContract_DropsOpenSeaRelease(t *testing.T) {
	superRare := registry.PublisherNameSuperRare
	foundation := registry.PublisherNameFoundation
	tests := []struct {
		name       string
		contract   string
		standard   domain.ChainStandard
		publisher  *registry.PublisherName
		collection string
	}{
		{"SuperRare", "0xb932a70a57673d89f4acffbe830e8ed7f75fb9e0", domain.StandardERC721, &superRare, "superrare"},
		{"SupeRare v1", "0x41A322b28D0fF354040e2CbC676F0320d8c8850d", domain.StandardERC721, &superRare, "superrare"},
		{"Foundation", "0x3B3ee1931Dc30C1957379FAc9aba94D1C48a5405", domain.StandardERC721, &foundation, "foundation"},
		{"Rarible ERC-721", "0x60F80121C31A0d46B5279700f9DF786054aa5eE5", domain.StandardERC721, nil, "rarible"},
		{"Rarible ERC-1155", "0xd07dc4262BCDbf85190C01c996b4C06a461d2430", domain.StandardERC1155, nil, "rarible"},
		{"Zora", "0xabEFBc9fD2F806065b4f3C237d4b59D9A97Bcac7", domain.StandardERC721, nil, "zora"},
		{"OpenSea Shared Storefront", "0x495f947276749Ce646f68AC8c248420045cb7b5e", domain.StandardERC1155, nil, "a-creator-collection"},
	}
	reg := loadPublisherRegistry(t, shippedPublisherRegistry)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := setupTestEnhancerWithRegistry(t, reg)
			defer tearDownTestEnhancer(m)
			var publisher *metadata.Publisher
			if tt.publisher != nil {
				publisher = &metadata.Publisher{Name: tt.publisher}
			}
			expectOpenSeaToken(m, tt.collection)

			tokenCID := domain.NewTokenCID(domain.ChainEthereumMainnet, tt.standard, tt.contract, "3")
			result, err := m.enhancer.Enhance(context.Background(), tokenCID, nil, publisher)

			require.NoError(t, err)
			require.NotNil(t, result)
			assert.Equal(t, schema.VendorOpenSea, result.Vendor, "the vendor is unchanged; only the release goes")
			require.NotNil(t, result.Name)
			assert.Equal(t, "Untitled #3", *result.Name)
			assert.NotNil(t, result.ModerationStatus)
			assert.Nil(t, result.Release)
		})
	}
}

// The rule is scoped to the listed contracts: an OpenSea collection on any other
// contract is still the token's release.
func TestEnhancer_Enhance_SharedContract_KeepsOpenSeaReleaseElsewhere(t *testing.T) {
	m := setupTestEnhancerWithRegistry(t, loadPublisherRegistry(t, shippedPublisherRegistry))
	defer tearDownTestEnhancer(m)
	expectOpenSeaToken(m, "bored-ape-yacht-club")

	tokenCID := domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC721, "0xBC4CA0EdA7647A8aB7C2061c2E118A18a936f13D", "3")
	result, err := m.enhancer.Enhance(context.Background(), tokenCID, nil, nil)

	require.NoError(t, err)
	require.NotNil(t, result)
	require.NotNil(t, result.Release)
	assert.Equal(t, "bored-ape-yacht-club", result.Release.VendorReleaseID)
	assert.Equal(t, int64(3), result.Release.MintNumber)
}

// objkt is a collection-level vendor too. It reports hic et nunc's OBJKT contract as an
// open collection (no release) today; the registry rule holds even if a vendor ever
// classifies a shared contract as a custom collection.
func TestEnhancer_Enhance_SharedContract_DropsObjktRelease(t *testing.T) {
	m := setupTestEnhancerWithRegistry(t, loadPublisherRegistry(t, shippedPublisherRegistry))
	defer tearDownTestEnhancer(m)
	const hen = "KT1RJ6PbjHpwc3M5rw5s2Nbmefwbuwbdxton"
	token := &objkt.Token{
		Name: types.StringPtr("OBJKT #7"),
		FA:   &objkt.FA{Name: "hic et nunc", Editions: 100, CollectionType: "custom"},
	}
	m.objktClient.EXPECT().GetToken(gomock.Any(), hen, "7").Return(token, nil)
	m.json.EXPECT().Marshal(token).Return([]byte(`{}`), nil)

	tokenCID := domain.NewTokenCID(domain.ChainTezosMainnet, domain.StandardFA2, hen, "7")
	result, err := m.enhancer.Enhance(context.Background(), tokenCID, nil, nil)

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, schema.VendorObjkt, result.Vendor)
	assert.Nil(t, result.Release)
}

// A vendor that names a real series inside a shared contract keeps its release: fxhash's
// generative token is a series even when the contract it is minted on is listed shared.
func TestEnhancer_Enhance_SharedContract_SeriesVendorKeepsRelease(t *testing.T) {
	const gentkContract = "KT1U6EHmNxJTkvaWJ4ThczG4FSDaHC21ssvi"
	path := filepath.Join(t.TempDir(), "publisher.json")
	require.NoError(t, os.WriteFile(path, []byte(`{
		"version": 1,
		"shared_contracts": {"tezos:mainnet": [{"name": "fxhash GENTK", "address": "`+gentkContract+`"}]},
		"publishers": []
	}`), 0o600))
	m := setupTestEnhancerWithRegistry(t, loadPublisherRegistry(t, path))
	defer tearDownTestEnhancer(m)

	gentk := &fxhash.Gentk{
		Iteration: "5",
		Name:      types.StringPtr("Series #5"),
		GenerativeToken: &fxhash.GenerativeToken{
			ID: "9997", Name: "Series", Supply: "100",
			Author: &fxhash.Author{Name: "Artist", WalletAccount: &fxhash.WalletAccount{Address: "tz1burnburnburnburnburnburnburjAYjjX"}},
		},
	}
	m.fxhashClient.EXPECT().GetGentk(gomock.Any(), gentkContract, "5").Return(gentk, nil)
	m.json.EXPECT().Marshal(gentk).Return([]byte(`{}`), nil)

	publisherName := registry.PublisherNameFXHash
	tokenCID := domain.NewTokenCID(domain.ChainTezosMainnet, domain.StandardFA2, gentkContract, "5")
	result, err := m.enhancer.Enhance(context.Background(), tokenCID, nil, &metadata.Publisher{Name: &publisherName})

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, schema.VendorFXHash, result.Vendor)
	require.NotNil(t, result.Release)
	assert.Equal(t, "9997", result.Release.VendorReleaseID)
	assert.Equal(t, int64(5), result.Release.MintNumber)
}
