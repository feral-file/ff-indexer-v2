package local

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/gabriel-vasile/mimetype"
	"go.uber.org/zap"

	"github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/downloader"
	"github.com/feral-file/ff-indexer-v2/internal/logger"
	mediaprovider "github.com/feral-file/ff-indexer-v2/internal/media/provider"
	"github.com/feral-file/ff-indexer-v2/internal/types"
)

const LOCAL_PROVIDER_NAME = "local"

// Config holds configuration for local media storage.
type Config struct {
	StorageDir string
	BaseURL    string
}

type mediaProvider struct {
	config     *Config
	downloader downloader.Downloader
	fs         adapter.FileSystem
}

// NewMediaProvider creates a new local media provider.
func NewMediaProvider(config *Config, dl downloader.Downloader, fs adapter.FileSystem) mediaprovider.Provider {
	return &mediaProvider{
		config:     config,
		downloader: dl,
		fs:         fs,
	}
}

// UploadImageFromURL downloads an image and stores it locally.
func (p *mediaProvider) UploadImageFromURL(ctx context.Context, sourceURL string, metadata map[string]interface{}) (*mediaprovider.UploadResult, error) {
	result, err := p.downloader.Download(ctx, sourceURL)
	if err != nil {
		return nil, fmt.Errorf("failed to download image: %w", err)
	}
	defer func() {
		if err := result.Close(); err != nil {
			logger.WarnCtx(ctx, "failed to close download result", zap.Error(err))
		}
	}()

	filename := filenameFromURL(sourceURL)
	return p.storeFromReader(ctx, result.Reader(), filename, result.ContentType(), "image", metadata)
}

// UploadImageFromReader stores image bytes locally.
func (p *mediaProvider) UploadImageFromReader(ctx context.Context, reader io.Reader, filename, contentType string, metadata map[string]interface{}) (*mediaprovider.UploadResult, error) {
	return p.storeFromReader(ctx, reader, filename, contentType, "image", metadata)
}

// UploadVideo downloads a video and stores it locally.
func (p *mediaProvider) UploadVideo(ctx context.Context, sourceURL string, metadata map[string]interface{}) (*mediaprovider.UploadResult, error) {
	result, err := p.downloader.Download(ctx, sourceURL)
	if err != nil {
		return nil, fmt.Errorf("failed to download video: %w", err)
	}
	defer func() {
		if err := result.Close(); err != nil {
			logger.WarnCtx(ctx, "failed to close download result", zap.Error(err))
		}
	}()

	filename := filenameFromURL(sourceURL)
	return p.storeFromReader(ctx, result.Reader(), filename, result.ContentType(), "video", metadata)
}

// Name returns the provider name.
func (p *mediaProvider) Name() string {
	return LOCAL_PROVIDER_NAME
}

func (p *mediaProvider) storeFromReader(ctx context.Context, reader io.Reader, filename, contentType, mediaType string, metadata map[string]interface{}) (*mediaprovider.UploadResult, error) {
	if p.config == nil {
		return nil, fmt.Errorf("local provider config is required")
	}
	storageDir := strings.TrimSpace(p.config.StorageDir)
	if storageDir == "" {
		return nil, fmt.Errorf("local provider storage directory is required")
	}

	if !filepath.IsAbs(storageDir) {
		abs, err := filepath.Abs(storageDir)
		if err == nil {
			storageDir = abs
		}
	}

	if err := os.MkdirAll(storageDir, 0o750); err != nil {
		return nil, fmt.Errorf("failed to create storage directory: %w", err)
	}

	finalName := buildFilename(filename, contentType)
	path := filepath.Join(storageDir, finalName)

	file, err := p.fs.Create(path)
	if err != nil {
		return nil, fmt.Errorf("failed to create local media file: %w", err)
	}
	defer func() {
		if err := file.Close(); err != nil {
			logger.WarnCtx(ctx, "failed to close local media file", zap.Error(err))
		}
	}()

	written, err := io.Copy(file, reader)
	if err != nil {
		return nil, fmt.Errorf("failed to write local media file: %w", err)
	}

	variantURL := buildVariantURL(p.config.BaseURL, path, finalName)

	providerMetadata := map[string]interface{}{
		"storage_path": path,
		"file_name":    finalName,
		"content_type": contentType,
		"size_bytes":   written,
		"media_type":   mediaType,
		"created_at":   time.Now().Format(time.RFC3339),
	}
	if p.config.BaseURL != "" {
		providerMetadata["base_url"] = p.config.BaseURL
	}

	logger.InfoCtx(ctx, "Stored media locally",
		zap.String("path", path),
		zap.Int64("bytes", written),
		zap.String("mediaType", mediaType),
	)

	return &mediaprovider.UploadResult{
		ProviderAssetID:  finalName,
		VariantURLs:      map[string]string{"original": variantURL},
		ProviderMetadata: providerMetadata,
	}, nil
}

func buildFilename(filename, contentType string) string {
	base := filepath.Base(strings.TrimSpace(filename))
	if base == "." || base == string(filepath.Separator) || base == "" {
		base = "media"
	}
	base = strings.ReplaceAll(base, " ", "_")

	ext := filepath.Ext(base)
	name := strings.TrimSuffix(base, ext)
	if ext == "" {
		ext = fileExtFromMimeType(contentType)
	}

	suffix, err := types.GenerateSecureToken(6)
	if err != nil {
		suffix = fmt.Sprintf("%d", time.Now().UnixNano())
	}

	return fmt.Sprintf("%s-%s%s", name, suffix, ext)
}

func buildVariantURL(baseURL, storagePath, filename string) string {
	if strings.TrimSpace(baseURL) == "" {
		return storagePath
	}

	trimmed := strings.TrimRight(baseURL, "/")
	return trimmed + "/" + url.PathEscape(filename)
}

func filenameFromURL(rawURL string) string {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return "media"
	}
	base := filepath.Base(parsed.Path)
	if base == "." || base == string(filepath.Separator) || base == "" {
		return "media"
	}
	return base
}

func fileExtFromMimeType(mimeType string) string {
	if mimeType == "" {
		return ""
	}
	lookup := mimetype.Lookup(mimeType)
	if lookup != nil {
		ext := lookup.Extension()
		if ext != "" {
			return ext
		}
	}

	mainType := strings.Split(mimeType, ";")[0]
	mainType = strings.TrimSpace(mainType)
	if strings.HasPrefix(mainType, "video/") {
		return ".mp4"
	}
	if strings.HasPrefix(mainType, "image/") {
		return ".jpg"
	}
	return ""
}
