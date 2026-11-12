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
// so the OpenSea branch builds a release before the platform-release rule applies.
func expectOpenSeaToken(m *testEnhancerMocks, collection string) {
	name := "Untitled #3"
	nft := &opensea.NFTMetadata{Identifier: "3", Collection: collection, Name: &name}
	m.openseaClient.EXPECT().GetNFT(gomock.Any(), gomock.Any(), "3").Return(nft, nil)
	m.openseaClient.EXPECT().GetCollection(gomock.Any(), collection).
		Return(&opensea.CollectionMetadata{Collection: collection, Name: collection, TotalSupply: 100}, nil).AnyTimes()
	m.json.EXPECT().Marshal(nft).Return([]byte(`{}`), nil)
}

// sharedContractCase is one token on a shared minting contract, reaching OpenSea either
// as a known publisher's designated vendor (publisher set) or via the generic fallback.
type sharedContractCase struct {
	name       string
	contract   string
	standard   domain.ChainStandard
	publisher  *registry.PublisherName
	collection string
}

// enhanceOpenSeaCase runs Enhance for a sharedContractCase against the shipped registry
// and returns the result; the vendor and enrichment fields must be intact either way.
func enhanceOpenSeaCase(t *testing.T, reg registry.PublisherRegistry, tt sharedContractCase) *metadata.EnhancedMetadata {
	t.Helper()
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
	assert.Equal(t, schema.VendorOpenSea, result.Vendor, "the vendor is unchanged; only the release may go")
	require.NotNil(t, result.Name)
	assert.Equal(t, "Untitled #3", *result.Name)
	assert.NotNil(t, result.ModerationStatus)
	return result
}

// ff-indexer-v2 #176: OpenSea files nearly every token on a platform's shared minting
// contract under one platform-wide collection, so its release grouped unrelated works by
// different artists as one series. On every shared contract in the shipped registry, a
// token in that collection is enriched but gets no release, whether it reaches OpenSea
// as a known publisher's designated vendor (SuperRare, Foundation) or through the
// generic fallback (Rarible, Zora).
func TestEnhancer_Enhance_SharedContract_DropsPlatformRelease(t *testing.T) {
	superRare := registry.PublisherNameSuperRare
	foundation := registry.PublisherNameFoundation
	tests := []sharedContractCase{
		{"SuperRare", "0xb932a70a57673d89f4acffbe830e8ed7f75fb9e0", domain.StandardERC721, &superRare, "superrare"},
		{"SuperRare, OpenSea's misfiled bucket", "0xb932a70a57673d89f4acffbe830e8ed7f75fb9e0", domain.StandardERC721, &superRare, "0x57f1887a8bf19b14fc0df6fd9b2acc9af147ea85"},
		{"SupeRare v1", "0x41A322b28D0fF354040e2CbC676F0320d8c8850d", domain.StandardERC721, &superRare, "superrare"},
		{"Foundation", "0x3B3ee1931Dc30C1957379FAc9aba94D1C48a5405", domain.StandardERC721, &foundation, "fnd"},
		{"Rarible ERC-721", "0x60F80121C31A0d46B5279700f9DF786054aa5eE5", domain.StandardERC721, nil, "rarible"},
		{"Rarible ERC-721, 2020 (0x6A5F)", "0x6A5Ff3CEEcaE9CeB96E6ac6c76B82aF8B39f0Eb3", domain.StandardERC721, nil, "rarible"},
		{"Rarible ERC-721, 2020 (0xF679)", "0xF6793dA657495ffeFF9Ee6350824910Abc21356C", domain.StandardERC721, nil, "rarible"},
		{"Rarible ERC-721 (0xc915)", "0xc9154424B823b10579895cCBE442d41b9Abd96Ed", domain.StandardERC721, nil, "rarible"},
		{"Rarible ERC-721 (0xe03e)", "0xe03e4721c75188de6DD9ccf4Fc0eb7C81399E305", domain.StandardERC721, nil, "rarible"},
		{"Rarible ERC-1155", "0xd07dc4262BCDbf85190C01c996b4C06a461d2430", domain.StandardERC1155, nil, "rarible"},
		{"Rarible ERC-1155 (0xB66a)", "0xB66a603f4cFe17e3D27B87a8BfCaD319856518B8", domain.StandardERC1155, nil, "rarible"},
		{"Zora", "0xabEFBc9fD2F806065b4f3C237d4b59D9A97Bcac7", domain.StandardERC721, nil, "zora"},
	}
	reg := loadPublisherRegistry(t, shippedPublisherRegistry)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := enhanceOpenSeaCase(t, reg, tt)
			assert.Nil(t, result.Release)
		})
	}
}

// OpenSea also carves real series out of the same contracts (XCOPY's Rarible works, a
// Gazell.io exhibition on SuperRare). A release is dropped only when it is the
// platform-wide collection, not because of the contract or the vendor, so these stand.
func TestEnhancer_Enhance_SharedContract_KeepsCreatorCollection(t *testing.T) {
	superRare := registry.PublisherNameSuperRare
	tests := []sharedContractCase{
		{"XCOPY on Rarible ERC-1155 via the generic fallback", "0xd07dc4262BCDbf85190C01c996b4C06a461d2430", domain.StandardERC1155, nil, "xcopy-rarible"},
		{"Gazell.io exhibition on SuperRare via the designated vendor", "0xb932a70a57673d89f4acffbe830e8ed7f75fb9e0", domain.StandardERC721, &superRare, "gazellio-spatial-exhibition"},
	}
	reg := loadPublisherRegistry(t, shippedPublisherRegistry)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := enhanceOpenSeaCase(t, reg, tt)
			require.NotNil(t, result.Release)
			assert.Equal(t, tt.collection, result.Release.VendorReleaseID)
			assert.Equal(t, int64(3), result.Release.MintNumber)
		})
	}
}

// The rule is scoped to the listed contracts: an OpenSea collection on any other
// contract is still the token's release, even one named like a platform bucket.
func TestEnhancer_Enhance_SharedContract_KeepsOpenSeaReleaseElsewhere(t *testing.T) {
	reg := loadPublisherRegistry(t, shippedPublisherRegistry)
	for _, tt := range []sharedContractCase{
		{"BAYC", "0xBC4CA0EdA7647A8aB7C2061c2E118A18a936f13D", domain.StandardERC721, nil, "bored-ape-yacht-club"},
		{"platform slug on an unlisted contract", "0xBC4CA0EdA7647A8aB7C2061c2E118A18a936f13D", domain.StandardERC721, nil, "rarible"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			result := enhanceOpenSeaCase(t, reg, tt)
			require.NotNil(t, result.Release)
			assert.Equal(t, tt.collection, result.Release.VendorReleaseID)
			assert.Equal(t, int64(3), result.Release.MintNumber)
		})
	}
}

// objkt's release for a custom collection is the contract's own KT1 address, so the
// platform-wide release of hic et nunc's OBJKT is that address. objkt reports OBJKT as
// an open collection (no release) today; the registry rule holds even if a vendor ever
// classifies the shared contract as a custom collection.
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

// A vendor that names a real series inside a shared contract keeps its release: the
// platform-wide release is matched by vendor and release id, so objkt's custom-collection
// release on a contract listed with an OpenSea platform collection is untouched.
func TestEnhancer_Enhance_SharedContract_SeriesVendorKeepsRelease(t *testing.T) {
	const contract = "KT1AbcCustomArtistContract123456789"
	path := filepath.Join(t.TempDir(), "publisher.json")
	require.NoError(t, os.WriteFile(path, []byte(`{
		"version": 1,
		"shared_contracts": {"tezos:mainnet": [{"name": "Shared", "address": "`+contract+`", "platform_releases": {"opensea": ["`+contract+`"], "objkt": ["KT1SomeOtherContract"]}}]},
		"publishers": []
	}`), 0o600))
	m := setupTestEnhancerWithRegistry(t, loadPublisherRegistry(t, path))
	defer tearDownTestEnhancer(m)

	token := &objkt.Token{
		Name: types.StringPtr("Series #5"),
		FA:   &objkt.FA{Name: "Series", Editions: 100, CollectionType: "custom"},
	}
	m.objktClient.EXPECT().GetToken(gomock.Any(), contract, "5").Return(token, nil)
	m.json.EXPECT().Marshal(token).Return([]byte(`{}`), nil)

	tokenCID := domain.NewTokenCID(domain.ChainTezosMainnet, domain.StandardFA2, contract, "5")
	result, err := m.enhancer.Enhance(context.Background(), tokenCID, nil, nil)

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, schema.VendorObjkt, result.Vendor)
	require.NotNil(t, result.Release, "the listed contract is OpenSea's platform release, not objkt's")
	assert.Equal(t, contract, result.Release.VendorReleaseID)
	assert.Equal(t, int64(5), result.Release.MintNumber)
}
