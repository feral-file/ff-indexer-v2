package adapter

import (
	"io"
	"os"
)

// FileSystem defines an interface for file system operations to enable mocking
//
//go:generate mockgen -source=filesystem.go -destination=../mocks/filesystem.go -package=mocks -mock_names=FileSystem=MockFileSystem
type FileSystem interface {
	// Create creates or truncates the named file
	Create(name string) (File, error)

	// Remove removes the named file or directory
	Remove(name string) error

	// TempDir returns the default directory to use for temporary files
	TempDir() string

	// ReadFile reads the named file and returns its contents
	ReadFile(name string) ([]byte, error)
}

// File defines an interface for file operations
type File interface {
	io.Writer
	io.Closer
}

// WritableFile defines an interface for writable file operations
type WritableFile interface {
	io.Writer
	io.Closer
	Name() string
}

// ReadableFile defines an interface for readable file operations
type ReadableFile interface {
	io.ReadCloser
}

// RealFileSystem implements FileSystem using the standard os package
type RealFileSystem struct{}

// NewFileSystem creates a new real file system
func NewFileSystem() FileSystem {
	return &RealFileSystem{}
}

// Create creates or truncates the named file
func (fs *RealFileSystem) Create(name string) (File, error) {
	return os.Create(name) //nolint:gosec,G304
}

// Remove removes the named file or directory
func (fs *RealFileSystem) Remove(name string) error {
	return os.Remove(name)
}

// TempDir returns the default directory to use for temporary files
func (fs *RealFileSystem) TempDir() string {
	return os.TempDir()
}

// ReadFile reads the named file and returns its contents
func (fs *RealFileSystem) ReadFile(name string) ([]byte, error) {
	return os.ReadFile(name) //nolint:gosec,G304 // This should be a trusted file
}
