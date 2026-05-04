//go:build cgo

package workflows_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"

	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/logger"
	"github.com/feral-file/ff-indexer-v2/internal/mocks"
	"github.com/feral-file/ff-indexer-v2/internal/providers/cloudflare"
	"github.com/feral-file/ff-indexer-v2/internal/store/schema"
	"github.com/feral-file/ff-indexer-v2/internal/workflows"
)

// testMediaExecutorMocks contains all the mocks needed for testing the media executor.
type testMediaExecutorMocks struct {
	ctrl           *gomock.Controller
	store          *mocks.MockStore
	mediaProcessor *mocks.MockMediaProcessor
	executor       workflows.MediaExecutor
}

// setupTestMediaExecutor creates all the mocks and executor for testing.
func setupTestMediaExecutor(t *testing.T) *testMediaExecutorMocks {
	// Initialize logger for tests (required for activities that log)
	err := logger.Initialize(logger.Config{
		Debug: true,
	})
	if err != nil {
		t.Fatalf("Failed to initialize logger: %v", err)
	}

	ctrl := gomock.NewController(t)
	store := mocks.NewMockStore(ctrl)
	mediaProcessor := mocks.NewMockMediaProcessor(ctrl)

	tm := &testMediaExecutorMocks{
		ctrl:           ctrl,
		mediaProcessor: mediaProcessor,
		store:          store,
	}

	tm.executor = workflows.NewMediaExecutor(
		store,
		mediaProcessor,
	)

	return tm
}

// tearDownTestMediaExecutor cleans up the test mocks.
func tearDownTestMediaExecutor(m *testMediaExecutorMocks) {
	m.ctrl.Finish()
}

func TestIndexMediaFile_Success(t *testing.T) {
	m := setupTestMediaExecutor(t)
	defer tearDownTestMediaExecutor(m)

	ctx := context.Background()
	url := "https://example.com/image.png"

	// Mock store GetMediaAssetBySourceURL to return nil (not exists)
	m.mediaProcessor.EXPECT().
		Provider().
		Return(cloudflare.CLOUDFLARE_PROVIDER_NAME)

	m.store.EXPECT().
		GetMediaAssetBySourceURL(ctx, url, schema.StorageProviderCloudflare).
		Return(nil, nil)

	// Mock mediaProcessor.Process to succeed
	m.mediaProcessor.EXPECT().
		Process(ctx, url).
		Return(nil)

	err := m.executor.IndexMediaFile(ctx, url)

	assert.NoError(t, err)
}

func TestIndexMediaFile_DataURI(t *testing.T) {
	m := setupTestMediaExecutor(t)
	defer tearDownTestMediaExecutor(m)

	ctx := context.Background()
	url := "data:image/png;base64,iVBORw0KGgo="

	// Mock store GetMediaAssetBySourceURL to return nil (not exists)
	m.mediaProcessor.EXPECT().
		Provider().
		Return(cloudflare.CLOUDFLARE_PROVIDER_NAME)

	m.store.EXPECT().
		GetMediaAssetBySourceURL(ctx, url, schema.StorageProviderCloudflare).
		Return(nil, nil)

	// Mock mediaProcessor.Process to succeed
	m.mediaProcessor.EXPECT().
		Process(ctx, url).
		Return(nil)

	err := m.executor.IndexMediaFile(ctx, url)

	assert.NoError(t, err)
}

func TestIndexMediaFile_EmptyURL(t *testing.T) {
	m := setupTestMediaExecutor(t)
	defer tearDownTestMediaExecutor(m)

	ctx := context.Background()
	url := ""

	err := m.executor.IndexMediaFile(ctx, url)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "media URL is empty")
}

func TestIndexMediaFile_AlreadyExists(t *testing.T) {
	m := setupTestMediaExecutor(t)
	defer tearDownTestMediaExecutor(m)

	ctx := context.Background()
	url := "https://example.com/image.png"

	// Mock store GetMediaAssetBySourceURL to return existing asset
	existingAsset := &schema.MediaAsset{
		ID:        1,
		SourceURL: url,
	}

	m.mediaProcessor.EXPECT().
		Provider().
		Return(cloudflare.CLOUDFLARE_PROVIDER_NAME)

	m.store.EXPECT().
		GetMediaAssetBySourceURL(ctx, url, schema.StorageProviderCloudflare).
		Return(existingAsset, nil)

	err := m.executor.IndexMediaFile(ctx, url)

	// Should succeed without processing (skips existing)
	assert.NoError(t, err)
}

func TestIndexMediaFile_CheckExistingError(t *testing.T) {
	m := setupTestMediaExecutor(t)
	defer tearDownTestMediaExecutor(m)

	ctx := context.Background()
	url := "https://example.com/image.png"

	// Mock store GetMediaAssetBySourceURL to return error
	storeErr := errors.New("database error")

	m.mediaProcessor.EXPECT().
		Provider().
		Return(cloudflare.CLOUDFLARE_PROVIDER_NAME)

	m.store.EXPECT().
		GetMediaAssetBySourceURL(ctx, url, schema.StorageProviderCloudflare).
		Return(nil, storeErr)

	err := m.executor.IndexMediaFile(ctx, url)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to check existing media asset")
}

func TestIndexMediaFile_UnsupportedMediaFile(t *testing.T) {
	m := setupTestMediaExecutor(t)
	defer tearDownTestMediaExecutor(m)

	ctx := context.Background()
	url := "https://example.com/unsupported.xyz"

	// Mock store GetMediaAssetBySourceURL to return nil (not exists)
	m.mediaProcessor.EXPECT().
		Provider().
		Return(cloudflare.CLOUDFLARE_PROVIDER_NAME)

	m.store.EXPECT().
		GetMediaAssetBySourceURL(ctx, url, schema.StorageProviderCloudflare).
		Return(nil, nil)

	// Mock mediaProcessor.Process to return unsupported file error
	m.mediaProcessor.EXPECT().
		Process(ctx, url).
		Return(domain.ErrUnsupportedMediaFile)

	err := m.executor.IndexMediaFile(ctx, url)

	// Should not return error for known skip errors
	assert.NoError(t, err)
}

func TestIndexMediaFile_ExceededMaxFileSize(t *testing.T) {
	m := setupTestMediaExecutor(t)
	defer tearDownTestMediaExecutor(m)

	ctx := context.Background()
	url := "https://example.com/huge-file.mp4"

	// Mock store GetMediaAssetBySourceURL to return nil (not exists)
	m.mediaProcessor.EXPECT().
		Provider().
		Return(cloudflare.CLOUDFLARE_PROVIDER_NAME)

	m.store.EXPECT().
		GetMediaAssetBySourceURL(ctx, url, schema.StorageProviderCloudflare).
		Return(nil, nil)

	// Mock mediaProcessor.Process to return exceeded max file size error
	m.mediaProcessor.EXPECT().
		Process(ctx, url).
		Return(domain.ErrExceededMaxFileSize)

	err := m.executor.IndexMediaFile(ctx, url)

	// Should not return error for known skip errors
	assert.NoError(t, err)
}

func TestIndexMediaFile_MissingContentLength(t *testing.T) {
	m := setupTestMediaExecutor(t)
	defer tearDownTestMediaExecutor(m)

	ctx := context.Background()
	url := "https://example.com/no-content-length.png"

	// Mock store GetMediaAssetBySourceURL to return nil (not exists)
	m.mediaProcessor.EXPECT().
		Provider().
		Return(cloudflare.CLOUDFLARE_PROVIDER_NAME)

	m.store.EXPECT().
		GetMediaAssetBySourceURL(ctx, url, schema.StorageProviderCloudflare).
		Return(nil, nil)

	// Mock mediaProcessor.Process to return missing content length error
	m.mediaProcessor.EXPECT().
		Process(ctx, url).
		Return(domain.ErrMissingContentLength)

	err := m.executor.IndexMediaFile(ctx, url)

	// Should not return error for known skip errors
	assert.NoError(t, err)
}

func TestIndexMediaFile_ProcessError(t *testing.T) {
	m := setupTestMediaExecutor(t)
	defer tearDownTestMediaExecutor(m)

	ctx := context.Background()
	url := "https://example.com/image.png"

	// Mock store GetMediaAssetBySourceURL to return nil (not exists)
	m.mediaProcessor.EXPECT().
		Provider().
		Return(cloudflare.CLOUDFLARE_PROVIDER_NAME)

	m.store.EXPECT().
		GetMediaAssetBySourceURL(ctx, url, schema.StorageProviderCloudflare).
		Return(nil, nil)

	// Mock mediaProcessor.Process to return unexpected error
	processErr := errors.New("failed to upload")
	m.mediaProcessor.EXPECT().
		Process(ctx, url).
		Return(processErr)

	err := m.executor.IndexMediaFile(ctx, url)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to process media file")
}

func TestIndexMediaFile_ProviderSelfHosted(t *testing.T) {
	m := setupTestMediaExecutor(t)
	defer tearDownTestMediaExecutor(m)

	ctx := context.Background()
	url := "https://example.com/image.png"

	// Mock store GetMediaAssetBySourceURL to return nil (not exists)
	// Test with self-hosted provider
	m.mediaProcessor.EXPECT().
		Provider().
		Return(schema.StorageProviderSelfHosted.String())

	m.store.EXPECT().
		GetMediaAssetBySourceURL(ctx, url, schema.StorageProviderSelfHosted).
		Return(nil, nil)

	// Mock mediaProcessor.Process to succeed
	m.mediaProcessor.EXPECT().
		Process(ctx, url).
		Return(nil)

	err := m.executor.IndexMediaFile(ctx, url)

	assert.NoError(t, err)
}
