package metadata_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/registry"
)

const publisherTestContract = "0x0000000000000000000000000000000000000123"

var publisherTestCID = domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC721, publisherTestContract, "1")

// expectDeployerLookup wires the registry miss and deployer RPC that precede a
// deployer-based publisher lookup.
func expectDeployerLookup(m *testResolverMocks, deployer string, err error) {
	m.registry.EXPECT().LookupPublisherByCollection(domain.ChainEthereumMainnet, publisherTestContract).Return(nil)
	m.registry.EXPECT().GetMinBlock(domain.ChainEthereumMainnet).Return(uint64(0), false)
	m.ethClient.EXPECT().GetContractDeployer(gomock.Any(), publisherTestContract, uint64(0)).Return(deployer, err)
	if err == nil {
		m.store.EXPECT().SetKeyValue(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
	}
}

// A registry collection hit must resolve from memory alone: this is the path that
// keeps Art Blocks tokens routed to Art Blocks when their tokenURI fetch fails.
func TestResolvePublisher_CollectionHit(t *testing.T) {
	m := setupTestResolver(t)
	defer tearDownTestResolver(m)

	m.ethClient.EXPECT().IsVendorOnlyMetadata(publisherTestContract).Return(false)
	m.registry.EXPECT().LookupPublisherByCollection(domain.ChainEthereumMainnet, publisherTestContract).
		Return(&registry.PublisherInfo{Name: registry.PublisherNameArtBlocks, URL: "https://artblocks.io"})

	publisher, err := m.resolver.ResolvePublisher(context.Background(), publisherTestCID)

	require.NoError(t, err)
	require.NotNil(t, publisher)
	assert.Equal(t, registry.PublisherNameArtBlocks, *publisher.Name)
	assert.Equal(t, "https://artblocks.io", *publisher.URL)
}

func TestResolvePublisher_DeployerHit(t *testing.T) {
	m := setupTestResolver(t)
	defer tearDownTestResolver(m)

	m.ethClient.EXPECT().IsVendorOnlyMetadata(publisherTestContract).Return(false)
	expectDeployerLookup(m, "0xdeployer", nil)
	m.registry.EXPECT().LookupPublisherByDeployer(domain.ChainEthereumMainnet, "0xdeployer").
		Return(&registry.PublisherInfo{Name: registry.PublisherNameFeralFile, URL: "https://feralfile.com"})

	publisher, err := m.resolver.ResolvePublisher(context.Background(), publisherTestCID)

	require.NoError(t, err)
	require.NotNil(t, publisher)
	assert.Equal(t, registry.PublisherNameFeralFile, *publisher.Name)
}

// A completed lookup with no match is (nil, nil): the only case allowed to reach the
// generic OpenSea/objkt enrichment.
func TestResolvePublisher_NoPublisher(t *testing.T) {
	m := setupTestResolver(t)
	defer tearDownTestResolver(m)

	m.ethClient.EXPECT().IsVendorOnlyMetadata(publisherTestContract).Return(false)
	expectDeployerLookup(m, "", nil)

	publisher, err := m.resolver.ResolvePublisher(context.Background(), publisherTestCID)

	require.NoError(t, err)
	assert.Nil(t, publisher)
}

// A failed deployer lookup must surface as an error, not as "no publisher", or the
// caller would route a possibly-known publisher's token to the generic fallback.
func TestResolvePublisher_DeployerLookupFails(t *testing.T) {
	m := setupTestResolver(t)
	defer tearDownTestResolver(m)

	m.ethClient.EXPECT().IsVendorOnlyMetadata(publisherTestContract).Return(false)
	expectDeployerLookup(m, "", assert.AnError)

	publisher, err := m.resolver.ResolvePublisher(context.Background(), publisherTestCID)

	require.ErrorIs(t, err, assert.AnError)
	assert.Nil(t, publisher)
}

// Normalization keeps storing fetched metadata when the publisher lookup fails, but
// flags it so enrichment can tell "lookup failed" from "no publisher".
func TestResolve_PublisherLookupFails_MarksPublisherUnresolved(t *testing.T) {
	m := setupTestResolver(t)
	defer tearDownTestResolver(m)

	m.ethClient.EXPECT().IsVendorOnlyMetadata(publisherTestContract).Return(false)
	m.ethClient.EXPECT().TokenURI(gomock.Any(), publisherTestContract, "1", domain.StandardERC721).
		Return("data:application/json;base64,eyJuYW1lIjoiVGVzdCBORlQifQ==", nil)
	m.json.EXPECT().Unmarshal(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ []byte, v interface{}) error {
			*v.(*map[string]interface{}) = map[string]interface{}{"name": "Test NFT"}
			return nil
		})
	expectDeployerLookup(m, "", assert.AnError)

	result, err := m.resolver.Resolve(context.Background(), publisherTestCID)

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, "Test NFT", result.Name)
	assert.Nil(t, result.Publisher)
	assert.True(t, result.PublisherUnresolved)
}

// Vendor-only contracts (CryptoPunks) never reached a deployer lookup before this
// lookup existed; it must not add an uncached archive binary search per token, nor
// make their OpenSea enrichment depend on that RPC succeeding.
func TestResolvePublisher_VendorOnly_SkipsDeployerLookup(t *testing.T) {
	m := setupTestResolver(t)
	defer tearDownTestResolver(m)

	punks := "0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb"
	// TokenCID checksums the address, so match on it rather than the literal.
	cid := domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC721, punks, "1")
	_, _, checksummed, _ := cid.Parse()
	m.ethClient.EXPECT().IsVendorOnlyMetadata(checksummed).Return(true)
	m.registry.EXPECT().LookupPublisherByCollection(domain.ChainEthereumMainnet, checksummed).Return(nil)
	m.ethClient.EXPECT().GetContractDeployer(gomock.Any(), gomock.Any(), gomock.Any()).Times(0)

	publisher, err := m.resolver.ResolvePublisher(context.Background(), cid)

	require.NoError(t, err)
	assert.Nil(t, publisher)
}
