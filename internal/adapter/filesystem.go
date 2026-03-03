package adapter

import (
	"os"
)

// FileSystem defines an interface for filesystem operations to enable mocking
//
//go:generate mockgen -source=filesystem.go -destination=../mocks/filesystem.go -package=mocks -mock_names=FileSystem=MockFileSystem
type FileSystem interface {
	// Create creates a file
	Create(name string) (*os.File, error)

	// CreateTemp creates a temporary file
	CreateTemp(dir, pattern string) (*os.File, error)

	// Remove removes a file
	Remove(name string) error

	// WriteFile writes data to a file
	WriteFile(file *os.File, data []byte) (int, error)

	// Close closes a file
	Close(file *os.File) error

	// ReadFile reads a file and returns its contents
	ReadFile(filePath string) ([]byte, error)

	// TempDir returns the temporary directory
	TempDir() string
}

type RealFileSystem struct{}

func NewFileSystem() FileSystem {
	return &RealFileSystem{}
}

func (f *RealFileSystem) Create(name string) (*os.File, error) {
	return os.Create(name) //nolint:gosec // File path comes from application logic, not user input
}

func (f *RealFileSystem) CreateTemp(dir, pattern string) (*os.File, error) {
	return os.CreateTemp(dir, pattern)
}

func (f *RealFileSystem) Remove(name string) error {
	return os.Remove(name)
}

func (f *RealFileSystem) WriteFile(file *os.File, data []byte) (int, error) {
	return file.Write(data)
}

func (f *RealFileSystem) Close(file *os.File) error {
	return file.Close()
}

func (f *RealFileSystem) ReadFile(filePath string) ([]byte, error) {
	return os.ReadFile(filePath) //nolint:gosec // File path comes from application logic, not user input
}

func (f *RealFileSystem) TempDir() string {
	return os.TempDir()
}
