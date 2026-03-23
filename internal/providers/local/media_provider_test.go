package local_test

import (
	"bytes"
	"context"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/downloader"
	"github.com/feral-file/ff-indexer-v2/internal/logger"
	"github.com/feral-file/ff-indexer-v2/internal/providers/local"
)

func init() {
	_ = logger.Initialize(logger.Config{Debug: true})
}

func TestUploadImageFromReader_LocalProvider(t *testing.T) {
	ctx := context.Background()

	storageDir := t.TempDir()
	provider := local.NewMediaProvider(&local.Config{
		StorageDir: storageDir,
		BaseURL:    "http://localhost:8082/media",
	}, nil, adapter.NewFileSystem())

	data := []byte("test-image-bytes")
	result, err := provider.UploadImageFromReader(ctx, bytes.NewReader(data), "image.png", "image/png", map[string]interface{}{})
	require.NoError(t, err)
	require.NotNil(t, result)
	require.NotEmpty(t, result.ProviderAssetID)

	url := result.VariantURLs["original"]
	require.True(t, strings.HasPrefix(url, "http://localhost:8082/media/"))

	metadata, ok := result.ProviderMetadata["storage_path"].(string)
	require.True(t, ok)
	expectedPath := filepath.Join(storageDir, result.ProviderAssetID)
	require.Equal(t, expectedPath, metadata)

	content, err := fs.ReadFile(os.DirFS(storageDir), result.ProviderAssetID)
	require.NoError(t, err)
	require.Equal(t, data, content)

	require.True(t, strings.HasPrefix(metadata, storageDir))
	require.Equal(t, filepath.Base(metadata), result.ProviderAssetID)
}

func TestUploadImageFromReader_RequiresStorageDir(t *testing.T) {
	ctx := context.Background()

	provider := local.NewMediaProvider(&local.Config{}, nil, adapter.NewFileSystem())
	_, err := provider.UploadImageFromReader(ctx, bytes.NewReader([]byte("x")), "image", "image/png", nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "storage directory is required")
}

func TestUploadImageFromReader_EmptyBaseURLUsesStoragePath(t *testing.T) {
	ctx := context.Background()

	storageDir := t.TempDir()
	provider := local.NewMediaProvider(&local.Config{
		StorageDir: storageDir,
	}, nil, adapter.NewFileSystem())

	result, err := provider.UploadImageFromReader(ctx, bytes.NewReader([]byte("hello")), "generated-name", "image/png", nil)
	require.NoError(t, err)
	require.NotNil(t, result)

	storagePath, ok := result.ProviderMetadata["storage_path"].(string)
	require.True(t, ok)
	require.Equal(t, storagePath, result.VariantURLs["original"])
	require.True(t, strings.HasSuffix(result.ProviderAssetID, ".png"))
}

func TestUploadImageFromURL_LocalProvider(t *testing.T) {
	ctx := context.Background()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "image/png")
		_, _ = w.Write([]byte("png-bytes"))
	}))
	t.Cleanup(server.Close)

	storageDir := t.TempDir()
	dl := downloader.NewDownloader(adapter.NewHTTPClient(5*time.Second), adapter.NewFileSystem())
	provider := local.NewMediaProvider(&local.Config{StorageDir: storageDir}, dl, adapter.NewFileSystem())

	result, err := provider.UploadImageFromURL(ctx, server.URL+"/images/source.png", nil)
	require.NoError(t, err)
	require.NotNil(t, result)

	storagePath, ok := result.ProviderMetadata["storage_path"].(string)
	require.True(t, ok)
	content, err := fs.ReadFile(os.DirFS(storageDir), result.ProviderAssetID)
	require.NoError(t, err)
	require.Equal(t, []byte("png-bytes"), content)
	require.Equal(t, storagePath, result.VariantURLs["original"])
}

func TestUploadVideo_LocalProvider(t *testing.T) {
	ctx := context.Background()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "video/mp4")
		_, _ = w.Write([]byte("mp4-bytes"))
	}))
	t.Cleanup(server.Close)

	storageDir := t.TempDir()
	dl := downloader.NewDownloader(adapter.NewHTTPClient(5*time.Second), adapter.NewFileSystem())
	provider := local.NewMediaProvider(&local.Config{StorageDir: storageDir}, dl, adapter.NewFileSystem())

	result, err := provider.UploadVideo(ctx, server.URL+"/videos/source.mp4", nil)
	require.NoError(t, err)
	require.NotNil(t, result)

	content, err := fs.ReadFile(os.DirFS(storageDir), result.ProviderAssetID)
	require.NoError(t, err)
	require.Equal(t, []byte("mp4-bytes"), content)
	require.True(t, strings.HasSuffix(result.ProviderAssetID, ".mp4"))
}
