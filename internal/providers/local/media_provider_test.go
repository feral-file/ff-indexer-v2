package local_test

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/feral-file/ff-indexer-v2/internal/adapter"
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

	content, err := os.ReadFile(metadata)
	require.NoError(t, err)
	require.Equal(t, data, content)

	require.True(t, strings.HasPrefix(metadata, storageDir))
	require.Equal(t, filepath.Base(metadata), result.ProviderAssetID)
}
