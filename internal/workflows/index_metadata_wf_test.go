//go:build cgo

// Package workflows_test: plain Go tests for metadata workflows (no Temporal testsuite).
package workflows_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/metadata"
	"github.com/feral-file/ff-indexer-v2/internal/mocks"
	"github.com/feral-file/ff-indexer-v2/internal/providers/jobs"
	"github.com/feral-file/ff-indexer-v2/internal/webhook"
	"github.com/feral-file/ff-indexer-v2/internal/workflows"
)

func metadataWfConfig(mediaEnabled bool) workflows.CoreWorkflowsConfig {
	return workflows.CoreWorkflowsConfig{
		TezosChainID:                 domain.ChainTezosMainnet,
		EthereumChainID:              domain.ChainEthereumMainnet,
		EthereumTokenSweepStartBlock: 0,
		TezosTokenSweepStartBlock:    0,
		MediaEnabled:                 mediaEnabled,
		MediaTaskQueue:               "media_index",
	}
}

// newMetadataWf builds [workflows.CoreWorkflows] with a [mocks.MockJobQueue]; attach EXPECTs on [coreWfDeps.MockJQ].
func newMetadataWf(t *testing.T, mediaEnabled bool) *coreWfDeps {
	t.Helper()
	return newCoreWfDeps(t, metadataWfConfig(mediaEnabled), nil)
}

// indexTokenMetadataChain configures executor calls for a successful IndexTokenMetadata when job queue is set (webhooks as enqueue).
func indexTokenMetadataCore(
	t *testing.T,
	exec *mocks.MockCoreExecutor,
	tokenCID domain.TokenCID,
	normalized *metadata.NormalizedMetadata,
	enhanced *metadata.EnhancedMetadata,
	health *workflows.MediaHealthCheckResult,
) {
	t.Helper()
	exec.EXPECT().ResolveTokenMetadata(gomock.Any(), tokenCID).Return(normalized, nil)
	exec.EXPECT().EnhanceTokenMetadata(gomock.Any(), tokenCID, gomock.Any()).Return(enhanced, nil)
	exec.EXPECT().CheckMediaURLsHealthAndUpdateViewability(gomock.Any(), gomock.Any(), gomock.Any()).Return(health, nil)
}

// ====================================================================================
// IndexMetadataUpdate
// ====================================================================================

func TestIndexMetadataUpdate_Success(t *testing.T) {
	t.Parallel()
	f := newMetadataWf(t, true)
	defer f.Ctrl.Finish()

	event := &domain.BlockchainEvent{
		Chain:           domain.ChainEthereumMainnet,
		Standard:        domain.StandardERC721,
		ContractAddress: "0x1234567890123456789012345678901234567890",
		TokenNumber:     "1",
		EventType:       domain.EventTypeMetadataUpdate,
		TxHash:          "0xabcd",
		BlockNumber:     100,
	}
	f.Exec.EXPECT().CreateMetadataUpdate(gomock.Any(), event).Return(nil)
	f.MockJQ.EXPECT().Enqueue(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, o jobs.EnqueueOptions) (any, bool, error) {
			require.Equal(t, "IndexTokenMetadata", o.Kind)
			require.Len(t, o.Args, 2)
			return nil, true, nil
		}).Times(1)

	err := f.Wf.IndexMetadataUpdate(f.Ctx, event)
	require.NoError(t, err)
}

func TestIndexMetadataUpdate_CreateMetadataUpdateError(t *testing.T) {
	t.Parallel()
	f := newMetadataWf(t, true)
	defer f.Ctrl.Finish()
	event := &domain.BlockchainEvent{
		Chain:           domain.ChainEthereumMainnet,
		Standard:        domain.StandardERC721,
		ContractAddress: "0x1234567890123456789012345678901234567890",
		TokenNumber:     "1",
		EventType:       domain.EventTypeMetadataUpdate,
		TxHash:          "0xabcd",
		BlockNumber:     100,
	}
	f.Exec.EXPECT().CreateMetadataUpdate(gomock.Any(), event).Return(errors.New("database error"))

	err := f.Wf.IndexMetadataUpdate(f.Ctx, event)
	require.Error(t, err)
}

func TestIndexMetadataUpdate_EnqueueFails(t *testing.T) {
	t.Parallel()
	f := newMetadataWf(t, true)
	defer f.Ctrl.Finish()
	event := &domain.BlockchainEvent{
		Chain:           domain.ChainEthereumMainnet,
		Standard:        domain.StandardERC721,
		ContractAddress: "0x1234567890123456789012345678901234567890",
		TokenNumber:     "1",
		EventType:       domain.EventTypeMetadataUpdate,
		TxHash:          "0xabcd",
		BlockNumber:     100,
	}
	f.Exec.EXPECT().CreateMetadataUpdate(gomock.Any(), event).Return(nil)
	f.MockJQ.EXPECT().Enqueue(gomock.Any(), gomock.Any()).Return(nil, false, errors.New("enqueue failed"))

	err := f.Wf.IndexMetadataUpdate(f.Ctx, event)
	require.Error(t, err)
}

// ====================================================================================
// IndexTokenMetadata
// ====================================================================================

func TestIndexTokenMetadata_Success_WithoutEnhancement(t *testing.T) {
	t.Parallel()
	f := newMetadataWf(t, true)
	defer f.Ctrl.Finish()
	stubJqAnyEnqueue(f.MockJQ)

	tokenCID := domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC721, "0x1234567890123456789012345678901234567890", "1")
	normalized := &metadata.NormalizedMetadata{
		Name: "Test Token", Description: "Test Description", Image: "https://example.com/image.jpg",
	}
	health := &workflows.MediaHealthCheckResult{IsViewable: true, HealthyURLs: []string{normalized.Image}}
	indexTokenMetadataCore(t, f.Exec, tokenCID, normalized, nil, health)

	err := f.Wf.IndexTokenMetadata(f.Ctx, tokenCID, nil)
	require.NoError(t, err)
}

func TestIndexTokenMetadata_MediaDisabled_DoesNotEnqueueMedia(t *testing.T) {
	t.Parallel()
	f := newMetadataWf(t, false)
	defer f.Ctrl.Finish()
	var mediaEnqueues int
	f.MockJQ.EXPECT().Enqueue(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, o jobs.EnqueueOptions) (any, bool, error) {
			if o.Kind == "IndexMediaWorkflow" {
				mediaEnqueues++
			}
			return nil, true, nil
		}).AnyTimes()

	tokenCID := domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC721, "0x1234567890123456789012345678901234567890", "1")
	normalized := &metadata.NormalizedMetadata{
		Name: "Test Token", Description: "Test Description", Image: "https://example.com/image.jpg",
	}
	health := &workflows.MediaHealthCheckResult{IsViewable: true, HealthyURLs: []string{normalized.Image}}
	indexTokenMetadataCore(t, f.Exec, tokenCID, normalized, nil, health)

	err := f.Wf.IndexTokenMetadata(f.Ctx, tokenCID, nil)
	require.NoError(t, err)
	require.Zero(t, mediaEnqueues, "media indexing must be skipped when MediaEnabled is false")
}

func TestIndexTokenMetadata_Success_WithEnhancement(t *testing.T) {
	t.Parallel()
	f := newMetadataWf(t, true)
	defer f.Ctrl.Finish()
	stubJqAnyEnqueue(f.MockJQ)

	tokenCID := domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC721, "0x1234567890123456789012345678901234567890", "1")
	baseImg := "https://example.com/image.jpg"
	enhancedURL := "https://example.com/enhanced-image.jpg"
	normalized := &metadata.NormalizedMetadata{Name: "T", Image: baseImg}
	enhanced := &metadata.EnhancedMetadata{ImageURL: &enhancedURL}
	health := &workflows.MediaHealthCheckResult{IsViewable: true, HealthyURLs: []string{baseImg, enhancedURL}}
	indexTokenMetadataCore(t, f.Exec, tokenCID, normalized, enhanced, health)

	err := f.Wf.IndexTokenMetadata(f.Ctx, tokenCID, nil)
	require.NoError(t, err)
}

func TestIndexTokenMetadata_FetchMetadataError(t *testing.T) {
	t.Parallel()
	f := newMetadataWf(t, true)
	defer f.Ctrl.Finish()
	stubJqAnyEnqueue(f.MockJQ)

	tokenCID := domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC721, "0x1234567890123456789012345678901234567890", "1")
	f.Exec.EXPECT().ResolveTokenMetadata(gomock.Any(), tokenCID).Return(nil, errors.New("failed to fetch metadata"))
	f.Exec.EXPECT().EnhanceTokenMetadata(gomock.Any(), tokenCID, gomock.Any()).Return(nil, nil)
	f.Exec.EXPECT().CheckMediaURLsHealthAndUpdateViewability(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&workflows.MediaHealthCheckResult{IsViewable: false, HealthyURLs: nil}, nil)

	err := f.Wf.IndexTokenMetadata(f.Ctx, tokenCID, nil)
	require.NoError(t, err, "metadata fetch error is non-fatal for the workflow")
}

func TestIndexTokenMetadata_EnhancementError_NonFatal(t *testing.T) {
	t.Parallel()
	f := newMetadataWf(t, true)
	defer f.Ctrl.Finish()
	stubJqAnyEnqueue(f.MockJQ)

	tokenCID := domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC721, "0x1234567890123456789012345678901234567890", "1")
	normalized := &metadata.NormalizedMetadata{Image: "https://example.com/image.jpg"}
	health := &workflows.MediaHealthCheckResult{IsViewable: true, HealthyURLs: []string{normalized.Image}}
	f.Exec.EXPECT().ResolveTokenMetadata(gomock.Any(), tokenCID).Return(normalized, nil)
	f.Exec.EXPECT().EnhanceTokenMetadata(gomock.Any(), tokenCID, normalized).Return(nil, errors.New("enhancement failed"))
	f.Exec.EXPECT().CheckMediaURLsHealthAndUpdateViewability(gomock.Any(), gomock.Any(), gomock.Any()).Return(health, nil)

	err := f.Wf.IndexTokenMetadata(f.Ctx, tokenCID, nil)
	require.NoError(t, err)
}

func TestIndexTokenMetadata_MediaEnqueueFailure_NonFatal(t *testing.T) {
	t.Parallel()
	f := newMetadataWf(t, true)
	defer f.Ctrl.Finish()
	tokenCID := domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC721, "0x1234567890123456789012345678901234567890", "1")
	normalized := &metadata.NormalizedMetadata{Image: "https://example.com/image.jpg"}
	health := &workflows.MediaHealthCheckResult{IsViewable: true, HealthyURLs: []string{normalized.Image}}
	indexTokenMetadataCore(t, f.Exec, tokenCID, normalized, nil, health)
	f.MockJQ.EXPECT().Enqueue(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, o jobs.EnqueueOptions) (any, bool, error) {
			if o.Kind == "IndexMediaWorkflow" {
				return nil, false, errors.New("queue offline")
			}
			return nil, true, nil
		}).AnyTimes()

	err := f.Wf.IndexTokenMetadata(f.Ctx, tokenCID, nil)
	require.NoError(t, err, "IndexMedia job enqueue errors are non-fatal")
}

func TestIndexTokenMetadata_WebhookViewableEvent(t *testing.T) {
	t.Parallel()
	f := newMetadataWf(t, true)
	defer f.Ctrl.Finish()
	var eventTypes []string
	f.MockJQ.EXPECT().Enqueue(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, o jobs.EnqueueOptions) (any, bool, error) {
			if o.Kind == "NotifyWebhookClients" && len(o.Args) > 0 {
				if ev, ok := o.Args[0].(webhook.WebhookEvent); ok {
					eventTypes = append(eventTypes, ev.EventType)
				}
			}
			return nil, true, nil
		}).AnyTimes()

	tokenCID := domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC721, "0x1234567890123456789012345678901234567890", "1")
	normalized := &metadata.NormalizedMetadata{Image: "https://example.com/a.jpg", Animation: "https://example.com/b.mp4"}
	health := &workflows.MediaHealthCheckResult{IsViewable: true, HealthyURLs: []string{"https://example.com/a.jpg", "https://example.com/b.mp4"}}
	indexTokenMetadataCore(t, f.Exec, tokenCID, normalized, nil, health)

	require.NoError(t, f.Wf.IndexTokenMetadata(f.Ctx, tokenCID, nil))
	require.Contains(t, eventTypes, webhook.EventTypeTokenIndexingViewable)
}

func TestIndexTokenMetadata_WebhookUnviewableEvent(t *testing.T) {
	t.Parallel()
	f := newMetadataWf(t, true)
	defer f.Ctrl.Finish()
	var eventTypes []string
	f.MockJQ.EXPECT().Enqueue(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, o jobs.EnqueueOptions) (any, bool, error) {
			if o.Kind == "NotifyWebhookClients" && len(o.Args) > 0 {
				if ev, ok := o.Args[0].(webhook.WebhookEvent); ok {
					eventTypes = append(eventTypes, ev.EventType)
				}
			}
			return nil, true, nil
		}).AnyTimes()

	tokenCID := domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC721, "0x1234567890123456789012345678901234567890", "1")
	normalized := &metadata.NormalizedMetadata{Image: "https://x.com/b.jpg", Animation: "https://x.com/b2.mp4"}
	health := &workflows.MediaHealthCheckResult{IsViewable: false, HealthyURLs: nil}
	indexTokenMetadataCore(t, f.Exec, tokenCID, normalized, nil, health)

	require.NoError(t, f.Wf.IndexTokenMetadata(f.Ctx, tokenCID, nil))
	require.Contains(t, eventTypes, webhook.EventTypeTokenIndexingUnviewable)
}

func TestIndexTokenMetadata_NoMediaURLs(t *testing.T) {
	t.Parallel()
	f := newMetadataWf(t, true)
	defer f.Ctrl.Finish()
	stubJqAnyEnqueue(f.MockJQ)
	tokenCID := domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC721, "0x1234567890123456789012345678901234567890", "1")
	normalized := &metadata.NormalizedMetadata{Name: "T"}
	health := &workflows.MediaHealthCheckResult{IsViewable: false, HealthyURLs: nil}
	indexTokenMetadataCore(t, f.Exec, tokenCID, normalized, nil, health)

	require.NoError(t, f.Wf.IndexTokenMetadata(f.Ctx, tokenCID, nil))
}

func TestIndexTokenMetadata_NilMetadata(t *testing.T) {
	t.Parallel()
	f := newMetadataWf(t, true)
	defer f.Ctrl.Finish()
	stubJqAnyEnqueue(f.MockJQ)
	tokenCID := domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC721, "0x1234567890123456789012345678901234567890", "1")
	f.Exec.EXPECT().ResolveTokenMetadata(gomock.Any(), tokenCID).Return(nil, nil)
	f.Exec.EXPECT().EnhanceTokenMetadata(gomock.Any(), tokenCID, (*metadata.NormalizedMetadata)(nil)).Return(nil, nil)
	f.Exec.EXPECT().CheckMediaURLsHealthAndUpdateViewability(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&workflows.MediaHealthCheckResult{IsViewable: false, HealthyURLs: nil}, nil)

	require.NoError(t, f.Wf.IndexTokenMetadata(f.Ctx, tokenCID, nil))
}

// ====================================================================================
// IndexMultipleTokensMetadata
// ====================================================================================

func TestIndexMultipleTokensMetadata_Success(t *testing.T) {
	t.Parallel()
	f := newMetadataWf(t, true)
	defer f.Ctrl.Finish()
	tokenCIDs := []domain.TokenCID{
		domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC721, "0x1234567890123456789012345678901234567890", "1"),
		domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC721, "0x1234567890123456789012345678901234567890", "2"),
		domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC721, "0x1234567890123456789012345678901234567890", "3"),
	}
	f.MockJQ.EXPECT().Enqueue(gomock.Any(), gomock.Any()).
		Return(nil, true, nil).Times(3)

	require.NoError(t, f.Wf.IndexMultipleTokensMetadata(f.Ctx, tokenCIDs))
}

func TestIndexMultipleTokensMetadata_EmptyList(t *testing.T) {
	t.Parallel()
	f := newMetadataWf(t, true)
	defer f.Ctrl.Finish()
	require.NoError(t, f.Wf.IndexMultipleTokensMetadata(f.Ctx, nil))
}

func TestIndexMultipleTokensMetadata_EnqueueError(t *testing.T) {
	t.Parallel()
	f := newMetadataWf(t, true)
	defer f.Ctrl.Finish()
	tokens := []domain.TokenCID{
		domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC721, "0x1234567890123456789012345678901234567890", "1"),
	}
	f.MockJQ.EXPECT().Enqueue(gomock.Any(), gomock.Any()).Return(nil, false, errors.New("no queue"))
	err := f.Wf.IndexMultipleTokensMetadata(f.Ctx, tokens)
	require.Error(t, err)
}

func TestIndexMultipleTokensMetadata_SingleToken(t *testing.T) {
	t.Parallel()
	f := newMetadataWf(t, true)
	defer f.Ctrl.Finish()
	tok := domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC721, "0x1234567890123456789012345678901234567890", "1")
	f.MockJQ.EXPECT().Enqueue(gomock.Any(), gomock.Any()).Return(nil, true, nil).Times(1)
	require.NoError(t, f.Wf.IndexMultipleTokensMetadata(f.Ctx, []domain.TokenCID{tok}))
}

func TestIndexMultipleTokensMetadata_LargeList_Enqueues(t *testing.T) {
	t.Parallel()
	f := newMetadataWf(t, true)
	defer f.Ctrl.Finish()
	list := make([]domain.TokenCID, 20)
	for i := range 20 {
		list[i] = domain.NewTokenCID(
			domain.ChainEthereumMainnet, domain.StandardERC721,
			"0x1234567890123456789012345678901234567890", fmt.Sprintf("%d", i),
		)
	}
	f.MockJQ.EXPECT().Enqueue(gomock.Any(), gomock.Any()).Return(nil, true, nil).Times(20)
	require.NoError(t, f.Wf.IndexMultipleTokensMetadata(f.Ctx, list))
}
