//go:build cgo

package processor_test

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/downloader"
	"github.com/feral-file/ff-indexer-v2/internal/logger"
	"github.com/feral-file/ff-indexer-v2/internal/media/processor"
	mediaprovider "github.com/feral-file/ff-indexer-v2/internal/media/provider"
	"github.com/feral-file/ff-indexer-v2/internal/media/rasterizer"
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
		false,
	)

	err := proc.Process(ctx, testDataURIPNG)
	require.NoError(t, err)
}

func TestProcess_VideoDisabled(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.Background()
	jsonAdapter := adapter.NewJSON()
	videoURL := "https://example.com/a.mp4"

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

	httpClient.EXPECT().Head(gomock.Any(), videoURL).Return(&http.Response{
		StatusCode:    http.StatusOK,
		Header:        http.Header{"Content-Type": []string{"video/mp4"}},
		ContentLength: 1024,
		Body:          io.NopCloser(strings.NewReader("")),
	}, nil)

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
		300*1024*1024,
		false,
	)

	require.NoError(t, proc.Process(ctx, videoURL))
}

func TestProcess_VideoEnabled(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.Background()
	jsonAdapter := adapter.NewJSON()
	videoURL := "https://example.com/a.mp4"

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

	httpClient.EXPECT().Head(gomock.Any(), videoURL).Return(&http.Response{
		StatusCode:    http.StatusOK,
		Header:        http.Header{"Content-Type": []string{"video/mp4"}},
		ContentLength: 1024,
		Body:          io.NopCloser(strings.NewReader("")),
	}, nil)

	provider.EXPECT().
		UploadVideo(gomock.Any(), videoURL, gomock.Any()).
		Return(&mediaprovider.UploadResult{
			ProviderAssetID: "vid-1",
			VariantURLs: map[string]string{
				"hls": "https://customer.example.cloudflarestream.com/vid/manifest/video.m3u8",
			},
			ProviderMetadata: map[string]interface{}{
				"media_type": "video",
			},
		}, nil)

	st.EXPECT().
		CreateMediaAsset(ctx, gomock.Any()).
		DoAndReturn(func(ctx context.Context, input store.CreateMediaAssetInput) (*schema.MediaAsset, error) {
			require.Equal(t, videoURL, input.SourceURL)
			require.NotNil(t, input.MimeType)
			require.Equal(t, "video/mp4", *input.MimeType)
			require.Equal(t, schema.StorageProviderCloudflare, input.Provider)
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
		300*1024*1024,
		true,
	)

	require.NoError(t, proc.Process(ctx, videoURL))
}

// TestProcess_SVGUnsupportedFilter verifies that ErrUnsupportedSVGFilter (returned by the
// rasterizer when an SVG contains feDisplacementMap and no browser is available) propagates
// as a real error from processor.Process. The job must be marked failed with last_error set,
// NOT silently swallowed as a successful skip — which would hide the failure from operators.
func TestProcess_SVGUnsupportedFilter(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.Background()
	jsonAdapter := adapter.NewJSON()
	svgURL := "https://example.com/crash.svg"

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

	httpClient.EXPECT().Head(gomock.Any(), svgURL).Return(&http.Response{
		StatusCode:    http.StatusOK,
		Header:        http.Header{"Content-Type": []string{"image/svg+xml"}},
		ContentLength: 512,
		Body:          io.NopCloser(strings.NewReader("")),
	}, nil)

	svgBody := []byte(`<svg xmlns="http://www.w3.org/2000/svg"><filter id="f"><feDisplacementMap in="SourceGraphic" scale="10"/></filter></svg>`)
	dl.EXPECT().Download(gomock.Any(), svgURL).Return(
		&downloader.DownloadResult{}, nil,
	)
	ioAdapter.EXPECT().ReadAll(gomock.Any()).Return(svgBody, nil)

	// Rasterizer returns ErrUnsupportedSVGFilter (feDisplacementMap + no browser).
	raster.EXPECT().Rasterize(gomock.Any(), svgBody).Return(nil, rasterizer.ErrUnsupportedSVGFilter)

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
		300*1024*1024,
		false,
	)

	err := proc.Process(ctx, svgURL)
	// Must propagate as a real error so the caller marks the job failed, not succeeded.
	require.Error(t, err, "ErrUnsupportedSVGFilter must not be silently swallowed as success")
	require.ErrorIs(t, err, rasterizer.ErrUnsupportedSVGFilter)
}
