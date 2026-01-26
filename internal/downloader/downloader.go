package downloader

import (
	"context"
	"fmt"
	"io"

	"go.uber.org/zap"

	"github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/logger"
)

type DownloadResult struct {
	reader      io.ReadCloser
	contentType string
	size        int64
	fs          adapter.FileSystem
}

// Reader returns the io.ReadCloser for streaming the download
func (d *DownloadResult) Reader() io.ReadCloser {
	return d.reader
}

// ContentType returns the content type of the downloaded file
func (d *DownloadResult) ContentType() string {
	return d.contentType
}

// Size returns the size of the downloaded file (may be -1 if unknown)
func (d *DownloadResult) Size() int64 {
	return d.size
}

// AsFile saves the download result to a file
func (d *DownloadResult) AsFile(path string) error {
	// Create the file
	file, err := d.fs.Create(path)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer func() {
		if err := file.Close(); err != nil {
			logger.Warn("failed to close file", zap.Error(err), zap.String("path", path))
		}
	}()

	// Copy from reader to file
	written, err := io.Copy(file, d.reader)
	if err != nil {
		return fmt.Errorf("failed to write to file: %w", err)
	}

	logger.Debug("Saved download to file",
		zap.String("path", path),
		zap.Int64("bytes", written),
	)

	return nil
}

// Close closes the underlying reader
func (d *DownloadResult) Close() error {
	if d.reader != nil {
		return d.reader.Close()
	}
	return nil
}

// Downloader defines the interface for downloading media files
//
//go:generate mockgen -source=downloader.go -destination=../mocks/downloader.go -package=mocks -mock_names=Downloader=MockDownloader
type Downloader interface {
	// Download downloads a media file from a URL and returns a streaming reader
	Download(ctx context.Context, url string) (*DownloadResult, error)
}

type downloader struct {
	httpClient adapter.HTTPClient
	fs         adapter.FileSystem
}

func NewDownloader(httpClient adapter.HTTPClient, fs adapter.FileSystem) Downloader {
	return &downloader{
		httpClient: httpClient,
		fs:         fs,
	}
}

// Download downloads a media file from a URL and returns a streaming reader
func (d *downloader) Download(ctx context.Context, url string) (*DownloadResult, error) {
	logger.Info("Downloading file", zap.String("url", url))

	// Use the injected HTTP client for streaming downloads
	resp, err := d.httpClient.GetResponse(ctx, url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to download: %w", err)
	}

	// Check status code
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		if err := resp.Body.Close(); err != nil {
			logger.Warn("failed to close response body", zap.Error(err), zap.String("url", url))
		}
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	contentType := resp.Header.Get("Content-Type")
	contentLength := resp.ContentLength

	logger.Info("Download started",
		zap.String("url", url),
		zap.String("contentType", contentType),
		zap.Int64("contentLength", contentLength),
	)

	return &DownloadResult{
		reader:      resp.Body,
		contentType: contentType,
		size:        contentLength,
		fs:          d.fs,
	}, nil
}
