package workflowsmedia_test

import (
	"context"
	"errors"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"

	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/logger"
	"github.com/feral-file/ff-indexer-v2/internal/mocks"
	"github.com/feral-file/ff-indexer-v2/internal/providers/cloudflare"
	"github.com/feral-file/ff-indexer-v2/internal/store/schema"
	workflowsmedia "github.com/feral-file/ff-indexer-v2/internal/workflows/media"
)

// testExecutorMocks contains all the mocks needed for testing the executor
type testExecutorMocks struct {
	ctrl           *gomock.Controller
	store          *mocks.MockStore
	mediaProcessor *mocks.MockMediaProcessor
	executor       workflowsmedia.Executor
}

// setupTestExecutor creates all the mocks and executor for testing
func setupTestExecutor(t *testing.T) *testExecutorMocks {
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

	tm := &testExecutorMocks{
		ctrl:           ctrl,
		mediaProcessor: mediaProcessor,
		store:          store,
	}

	tm.executor = workflowsmedia.NewExecutor(
		store,
		mediaProcessor,
	)

	return tm
}

// tearDownTestExecutor cleans up the test mocks
func tearDownTestExecutor(mocks *testExecutorMocks) {
	mocks.ctrl.Finish()
}

func TestIndexMediaFile_Success(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	url := "https://example.com/image.png"

	// Mock store GetMediaAssetBySourceURL to return nil (not exists)
	mocks.mediaProcessor.EXPECT().
		Provider().
		Return(cloudflare.CLOUDFLARE_PROVIDER_NAME)

	mocks.store.EXPECT().
		GetMediaAssetBySourceURL(ctx, url, schema.StorageProviderCloudflare).
		Return(nil, nil)

	// Mock mediaProcessor.Process to succeed
	mocks.mediaProcessor.EXPECT().
		Process(ctx, url).
		Return(nil)

	err := mocks.executor.IndexMediaFile(ctx, url)

	assert.NoError(t, err)
}

func TestIndexMediaFile_EmptyURL(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	url := ""

	err := mocks.executor.IndexMediaFile(ctx, url)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "media URL is empty")
}

func TestIndexMediaFile_AlreadyExists(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	url := "https://example.com/image.png"

	// Mock store GetMediaAssetBySourceURL to return existing asset
	existingAsset := &schema.MediaAsset{
		ID:        1,
		SourceURL: url,
	}

	mocks.mediaProcessor.EXPECT().
		Provider().
		Return(cloudflare.CLOUDFLARE_PROVIDER_NAME)

	mocks.store.EXPECT().
		GetMediaAssetBySourceURL(ctx, url, schema.StorageProviderCloudflare).
		Return(existingAsset, nil)

	err := mocks.executor.IndexMediaFile(ctx, url)

	// Should succeed without processing (skips existing)
	assert.NoError(t, err)
}

func TestIndexMediaFile_CheckExistingError(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	url := "https://example.com/image.png"

	// Mock store GetMediaAssetBySourceURL to return error
	storeErr := errors.New("database error")

	mocks.mediaProcessor.EXPECT().
		Provider().
		Return(cloudflare.CLOUDFLARE_PROVIDER_NAME)

	mocks.store.EXPECT().
		GetMediaAssetBySourceURL(ctx, url, schema.StorageProviderCloudflare).
		Return(nil, storeErr)

	err := mocks.executor.IndexMediaFile(ctx, url)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to check existing media asset")
}

func TestIndexMediaFile_UnsupportedMediaFile(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	url := "https://example.com/unsupported.xyz"

	// Mock store GetMediaAssetBySourceURL to return nil (not exists)
	mocks.mediaProcessor.EXPECT().
		Provider().
		Return(cloudflare.CLOUDFLARE_PROVIDER_NAME)

	mocks.store.EXPECT().
		GetMediaAssetBySourceURL(ctx, url, schema.StorageProviderCloudflare).
		Return(nil, nil)

	// Mock mediaProcessor.Process to return unsupported file error
	mocks.mediaProcessor.EXPECT().
		Process(ctx, url).
		Return(domain.ErrUnsupportedMediaFile)

	err := mocks.executor.IndexMediaFile(ctx, url)

	// Should not return error for known skip errors
	assert.NoError(t, err)
}

func TestIndexMediaFile_UnsupportedSelfHostedMediaFile(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	url := "https://mysite.com/image.png"

	// Mock store GetMediaAssetBySourceURL to return nil (not exists)
	mocks.mediaProcessor.EXPECT().
		Provider().
		Return(cloudflare.CLOUDFLARE_PROVIDER_NAME)

	mocks.store.EXPECT().
		GetMediaAssetBySourceURL(ctx, url, schema.StorageProviderCloudflare).
		Return(nil, nil)

	// Mock mediaProcessor.Process to return unsupported self-hosted error
	mocks.mediaProcessor.EXPECT().
		Process(ctx, url).
		Return(domain.ErrUnsupportedSelfHostedMediaFile)

	err := mocks.executor.IndexMediaFile(ctx, url)

	// Should not return error for known skip errors
	assert.NoError(t, err)
}

func TestIndexMediaFile_ExceededMaxFileSize(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	url := "https://example.com/huge-file.mp4"

	// Mock store GetMediaAssetBySourceURL to return nil (not exists)
	mocks.mediaProcessor.EXPECT().
		Provider().
		Return(cloudflare.CLOUDFLARE_PROVIDER_NAME)

	mocks.store.EXPECT().
		GetMediaAssetBySourceURL(ctx, url, schema.StorageProviderCloudflare).
		Return(nil, nil)

	// Mock mediaProcessor.Process to return exceeded max file size error
	mocks.mediaProcessor.EXPECT().
		Process(ctx, url).
		Return(domain.ErrExceededMaxFileSize)

	err := mocks.executor.IndexMediaFile(ctx, url)

	// Should not return error for known skip errors
	assert.NoError(t, err)
}

func TestIndexMediaFile_MissingContentLength(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	url := "https://example.com/no-content-length.png"

	// Mock store GetMediaAssetBySourceURL to return nil (not exists)
	mocks.mediaProcessor.EXPECT().
		Provider().
		Return(cloudflare.CLOUDFLARE_PROVIDER_NAME)

	mocks.store.EXPECT().
		GetMediaAssetBySourceURL(ctx, url, schema.StorageProviderCloudflare).
		Return(nil, nil)

	// Mock mediaProcessor.Process to return missing content length error
	mocks.mediaProcessor.EXPECT().
		Process(ctx, url).
		Return(domain.ErrMissingContentLength)

	err := mocks.executor.IndexMediaFile(ctx, url)

	// Should not return error for known skip errors
	assert.NoError(t, err)
}

func TestIndexMediaFile_ProcessError(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	url := "https://example.com/image.png"

	// Mock store GetMediaAssetBySourceURL to return nil (not exists)
	mocks.mediaProcessor.EXPECT().
		Provider().
		Return(cloudflare.CLOUDFLARE_PROVIDER_NAME)

	mocks.store.EXPECT().
		GetMediaAssetBySourceURL(ctx, url, schema.StorageProviderCloudflare).
		Return(nil, nil)

	// Mock mediaProcessor.Process to return unexpected error
	processErr := errors.New("failed to upload")
	mocks.mediaProcessor.EXPECT().
		Process(ctx, url).
		Return(processErr)

	err := mocks.executor.IndexMediaFile(ctx, url)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to process media file")
}

func TestIndexMediaFile_ProviderSelfHosted(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	url := "https://example.com/image.png"

	// Mock store GetMediaAssetBySourceURL to return nil (not exists)
	// Test with self-hosted provider
	mocks.mediaProcessor.EXPECT().
		Provider().
		Return(schema.StorageProviderSelfHosted.String())

	mocks.store.EXPECT().
		GetMediaAssetBySourceURL(ctx, url, schema.StorageProviderSelfHosted).
		Return(nil, nil)

	// Mock mediaProcessor.Process to succeed
	mocks.mediaProcessor.EXPECT().
		Process(ctx, url).
		Return(nil)

	err := mocks.executor.IndexMediaFile(ctx, url)

	assert.NoError(t, err)
}
