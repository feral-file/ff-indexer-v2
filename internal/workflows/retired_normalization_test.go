package workflows_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/metadata"
	"github.com/feral-file/ff-indexer-v2/internal/mocks"
	"github.com/feral-file/ff-indexer-v2/internal/providers/vendors/objkt"
	"github.com/feral-file/ff-indexer-v2/internal/registry"
	"github.com/feral-file/ff-indexer-v2/internal/security/ssrf"
	"github.com/feral-file/ff-indexer-v2/internal/store"
	"github.com/feral-file/ff-indexer-v2/internal/store/schema"
	"github.com/feral-file/ff-indexer-v2/internal/types"
	"github.com/feral-file/ff-indexer-v2/internal/uri"
	"github.com/feral-file/ff-indexer-v2/internal/workflows"
)

// TestIndexTokenMetadata_RetirementFailurePreservesEnrichment crosses the real
// normalizer, executor, workflow, and vendor enhancer. The generic vendor could
// supply valid, different media after losing publisher context; that path must
// not overwrite an existing enrichment when normalization failed retirement.
func TestIndexTokenMetadata_RetirementFailurePreservesEnrichment(t *testing.T) {
	m := setupTestExecutor(t)
	ctx := context.Background()
	const contract = "KT1LjmAdYQCLBjwv4S2oFkEzyHVkomAf5MrW"
	cid := domain.NewTokenCID(domain.ChainTezosMainnet, domain.StandardFA2, contract, "15422")
	const retired = "https://ipfs.io/ipfs/QmVJn8AG9x22BrbUaUj2CAQFtKMozSHyLvgV2X6X8dmtPw"
	const vendorMedia = "https://vendor.example/another-image.gif"
	uriResolver := mocks.NewMockURIResolver(m.ctrl)
	uriResolver.EXPECT().Resolve(ctx, retired).Return("", assert.AnError)
	uriResolver.EXPECT().Resolve(ctx, vendorMedia).Return(vendorMedia, nil).AnyTimes()
	pub := mocks.NewMockPublisherRegistry(m.ctrl)
	pub.EXPECT().LookupPublisherByCollection(domain.ChainTezosMainnet, contract).
		Return(&registry.PublisherInfo{Name: registry.PublisherNameFeralFile})
	m.tzktClient.EXPECT().GetTokenMetadata(ctx, contract, "15422").
		Return(map[string]interface{}{"artifactUri": retired}, nil)
	normalizer := metadata.NewResolver(m.ethClient, m.tzktClient, m.httpClient, uriResolver,
		m.json, m.clock, mocks.NewMockJCS(m.ctrl), mocks.NewMockBase64(m.ctrl), m.store, pub)
	m.metadataResolver.EXPECT().Resolve(ctx, cid).DoAndReturn(normalizer.Resolve)

	// These optional calls expose the faulty continuation all the way to the
	// real executor's upsert, rather than failing at a missing mock expectation.
	vendor := mocks.NewMockObjktClient(m.ctrl)
	response := &objkt.Token{DisplayURI: types.StringPtr(vendorMedia), Mime: types.StringPtr("image/gif")}
	vendor.EXPECT().GetToken(ctx, contract, "15422").Return(response, nil).AnyTimes()
	m.json.EXPECT().Marshal(response).Return([]byte(`{}`), nil).AnyTimes()
	enhancer := metadata.NewEnhancer(m.httpClient, uriResolver,
		mocks.NewMockArtBlocksClient(m.ctrl), mocks.NewMockFeralFileClient(m.ctrl),
		mocks.NewMockFxhashClient(m.ctrl), vendor, mocks.NewMockOpenSeaClient(m.ctrl),
		m.json, mocks.NewMockJCS(m.ctrl))
	m.metadataEnhancer.EXPECT().Enhance(ctx, cid, (*metadata.NormalizedMetadata)(nil)).DoAndReturn(enhancer.Enhance).AnyTimes()
	m.metadataEnhancer.EXPECT().VendorJsonHash(gomock.Any()).Return([]byte{1}, nil).AnyTimes()
	m.store.EXPECT().GetTokenByTokenCID(ctx, cid.String()).Return(&schema.Token{ID: 1, TokenCID: cid.String()}, nil).AnyTimes()
	m.store.EXPECT().UpsertTokenMetadata(gomock.Any(), gomock.Any()).Times(0)
	m.store.EXPECT().UpsertEnrichmentSource(gomock.Any(), gomock.Any()).Times(0)
	queue := mocks.NewMockJobQueue(m.ctrl)
	queue.EXPECT().Enqueue(gomock.Any(), gomock.Any()).Times(0)
	wf := workflows.NewCoreWorkflows(m.executor, defaultCompactCoreWfConfig(), m.blacklist, queue)

	err := wf.IndexTokenMetadata(ctx, cid, nil)
	require.ErrorIs(t, err, uri.ErrRetiredGatewayReplacement)
	require.ErrorIs(t, err, assert.AnError)
}

// The real URI resolver, metadata resolver, executor, and workflow must carry a
// failed big-map gateway replacement past the stale-cache fallback. Otherwise
// the executor can overwrite already-signed metadata with an unsigned placeholder.
func TestIndexTokenMetadata_BigMapRetirementFailurePreservesSignedMetadata(t *testing.T) {
	m := setupTestExecutor(t)
	ctx := context.Background()
	const contract = "KT1LjmAdYQCLBjwv4S2oFkEzyHVkomAf5MrW"
	cid := domain.NewTokenCID(domain.ChainTezosMainnet, domain.StandardFA2, contract, "59")
	const ref = "QmVJn8AG9x22BrbUaUj2CAQFtKMozSHyLvgV2X6X8dmtPw"
	m.tzktClient.EXPECT().GetTokenMetadata(ctx, contract, "59").
		Return(map[string]interface{}{"name": "[WAITING TO BE SIGNED]"}, nil)
	m.tzktClient.EXPECT().GetTokenMetadataURI(ctx, contract, "59").
		Return("https://ipfs.io/ipfs/"+ref, nil)
	m.httpClient.EXPECT().GetResponseNoRetry(gomock.Any(), "https://ipfs.filebase.io/ipfs/"+ref, gomock.Any()).
		Return(nil, ssrf.ErrResolutionFailed)
	uriResolver := uri.NewResolver(m.httpClient, m.io, &uri.Config{
		IPFSGateways: []string{"https://ipfs.filebase.io"},
	})
	normalizer := metadata.NewResolver(m.ethClient, m.tzktClient, m.httpClient, uriResolver,
		m.json, m.clock, mocks.NewMockJCS(m.ctrl), mocks.NewMockBase64(m.ctrl), m.store, nil)
	m.metadataResolver.EXPECT().Resolve(ctx, cid).DoAndReturn(normalizer.Resolve)

	// These optional calls let the pre-fix path reach the real metadata upsert,
	// proving the write is prevented rather than merely failing a mock earlier.
	signed := []byte(`{"name":"Signed artwork"}`)
	m.store.EXPECT().GetTokenWithMetadataByTokenCID(ctx, cid.String()).
		Return(&store.TokensWithMetadataResult{
			Token: &schema.Token{ID: 1, TokenCID: cid.String()},
			Metadata: &schema.TokenMetadata{
				Name: types.StringPtr("Signed artwork"), OriginJSON: signed, LatestJSON: signed,
			},
		}, nil).AnyTimes()
	m.metadataResolver.EXPECT().RawHash(gomock.Any()).
		Return([]byte{1}, []byte(`{"name":"[WAITING TO BE SIGNED]"}`), nil).AnyTimes()
	m.clock.EXPECT().Now().Return(time.Time{}).AnyTimes()
	m.store.EXPECT().UpsertTokenMetadata(gomock.Any(), gomock.Any()).Times(0)
	m.store.EXPECT().UpsertEnrichmentSource(gomock.Any(), gomock.Any()).Times(0)
	m.store.EXPECT().UpdateTokenMediaHealthByURL(gomock.Any(), gomock.Any(), gomock.Any()).Times(0)
	m.metadataEnhancer.EXPECT().Enhance(gomock.Any(), gomock.Any(), gomock.Any()).Times(0)
	queue := mocks.NewMockJobQueue(m.ctrl)
	queue.EXPECT().Enqueue(gomock.Any(), gomock.Any()).Times(0)
	wf := workflows.NewCoreWorkflows(m.executor, defaultCompactCoreWfConfig(), m.blacklist, queue)

	err := wf.IndexTokenMetadata(ctx, cid, nil)
	require.ErrorIs(t, err, uri.ErrRetiredGatewayReplacement)
	require.ErrorIs(t, err, ssrf.ErrResolutionFailed)
}
