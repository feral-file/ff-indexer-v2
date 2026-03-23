//go:build cgo

package processor_test

import (
	"context"
	"io"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/logger"
	"github.com/feral-file/ff-indexer-v2/internal/media/processor"
	mediaprovider "github.com/feral-file/ff-indexer-v2/internal/media/provider"
	"github.com/feral-file/ff-indexer-v2/internal/media/transformer"
	"github.com/feral-file/ff-indexer-v2/internal/mocks"
	"github.com/feral-file/ff-indexer-v2/internal/store"
	"github.com/feral-file/ff-indexer-v2/internal/store/schema"
	"github.com/feral-file/ff-indexer-v2/internal/uri"
)

const testDataURIPNG = "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMB/6Y9K2kAAAAASUVORK5CYII="

func init() {
	_ = logger.Initialize(logger.Config{Debug: true})
}

func TestProcess_DataURIImage(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.Background()
	jsonAdapter := adapter.NewJSON()

	httpClient := mocks.NewMockHTTPClient(ctrl)
	uriResolver := mocks.NewMockURIResolver(ctrl)
	dataChecker := mocks.NewMockDataURIChecker(ctrl)
	st := mocks.NewMockStore(ctrl)
	raster := mocks.NewMockRasterizer(ctrl)
	fs := mocks.NewMockFileSystem(ctrl)
	ioAdapter := mocks.NewMockIO(ctrl)
	dl := mocks.NewMockDownloader(ctrl)
	trans := mocks.NewMockTransformer(ctrl)
	provider := mocks.NewMockMediaProvider(ctrl)

	provider.EXPECT().Name().Return("cloudflare").AnyTimes()

	dataChecker.EXPECT().
		Check(testDataURIPNG).
		Return(uri.DataURICheckResult{
			Valid:            true,
			MimeType:         "image/png",
			DeclaredMimeType: "image/png",
		})

	trans.EXPECT().
		Transform(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, input *transformer.TransformInput) (*transformer.TransformResult, error) {
			require.Empty(t, input.SourceURL)
			require.NotEmpty(t, input.Data)
			require.Equal(t, "data-uri", input.Filename)
			require.False(t, input.IsAnimated)

			return &transformer.TransformResult{
				Data:            []byte("webp"),
				ContentType:     "image/webp",
				Filename:        "image.webp",
				OriginalSize:    int64(len(input.Data)),
				TransformedSize: 4,
				Width:           1,
				Height:          1,
				Resized:         false,
				Compressed:      true,
				Quality:         80,
			}, nil
		})

	provider.EXPECT().
		UploadImageFromReader(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, reader io.Reader, filename, contentType string, metadata map[string]interface{}) (*mediaprovider.UploadResult, error) {
			require.Equal(t, "image/webp", contentType)
			require.NotEmpty(t, filename)
			require.Equal(t, true, metadata["data_uri"])
			require.Equal(t, true, metadata["transformed"])

			return &mediaprovider.UploadResult{
				ProviderAssetID: "asset-123",
				VariantURLs: map[string]string{
					"original": "https://cdn.example.com/asset-123",
				},
				ProviderMetadata: map[string]interface{}{
					"provider": "test",
				},
			}, nil
		})

	st.EXPECT().
		CreateMediaAsset(ctx, gomock.Any()).
		DoAndReturn(func(ctx context.Context, input store.CreateMediaAssetInput) (*schema.MediaAsset, error) {
			require.Equal(t, testDataURIPNG, input.SourceURL)
			require.Equal(t, schema.StorageProviderCloudflare, input.Provider)
			require.NotNil(t, input.MimeType)
			require.Equal(t, "image/webp", *input.MimeType)
			return &schema.MediaAsset{ID: 1}, nil
		})

	proc := processor.NewProcessor(
		httpClient,
		uriResolver,
		dataChecker,
		provider,
		st,
		raster,
		fs,
		ioAdapter,
		jsonAdapter,
		dl,
		trans,
		10*1024*1024,
		10*1024*1024,
	)

	err := proc.Process(ctx, testDataURIPNG)
	require.NoError(t, err)
}

func TestProcess_DataURIImage_LocalProvider(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.Background()
	jsonAdapter := adapter.NewJSON()

	httpClient := mocks.NewMockHTTPClient(ctrl)
	uriResolver := mocks.NewMockURIResolver(ctrl)
	dataChecker := mocks.NewMockDataURIChecker(ctrl)
	st := mocks.NewMockStore(ctrl)
	raster := mocks.NewMockRasterizer(ctrl)
	fs := mocks.NewMockFileSystem(ctrl)
	ioAdapter := mocks.NewMockIO(ctrl)
	dl := mocks.NewMockDownloader(ctrl)
	trans := mocks.NewMockTransformer(ctrl)
	provider := mocks.NewMockMediaProvider(ctrl)

	provider.EXPECT().Name().Return("local").AnyTimes()

	dataChecker.EXPECT().
		Check(testDataURIPNG).
		Return(uri.DataURICheckResult{Valid: true, MimeType: "image/png", DeclaredMimeType: "image/png"})

	trans.EXPECT().
		Transform(gomock.Any(), gomock.Any()).
		Return(&transformer.TransformResult{Data: []byte("webp"), ContentType: "image/webp", Filename: "image.webp"}, nil)

	provider.EXPECT().
		UploadImageFromReader(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&mediaprovider.UploadResult{
			ProviderAssetID: "asset-123",
			VariantURLs: map[string]string{
				"original": "http://localhost/media/asset-123",
			},
			ProviderMetadata: map[string]interface{}{"provider": "local"},
		}, nil)

	st.EXPECT().
		CreateMediaAsset(ctx, gomock.Any()).
		DoAndReturn(func(ctx context.Context, input store.CreateMediaAssetInput) (*schema.MediaAsset, error) {
			require.Equal(t, schema.StorageProviderLocal, input.Provider)
			return &schema.MediaAsset{ID: 1}, nil
		})

	proc := processor.NewProcessor(
		httpClient,
		uriResolver,
		dataChecker,
		provider,
		st,
		raster,
		fs,
		ioAdapter,
		jsonAdapter,
		dl,
		trans,
		10*1024*1024,
		10*1024*1024,
	)

	err := proc.Process(ctx, testDataURIPNG)
	require.NoError(t, err)
}
