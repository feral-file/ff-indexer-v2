package adapter

import "io"

// IO defines an interface for IO operations to enable mocking
//
//go:generate mockgen -source=io.go -destination=../mocks/io.go -package=mocks -mock_names=IO=MockIO
type IO interface {
	ReadAll(r io.Reader) ([]byte, error)
}

// RealIO implements IO using the standard io package
type RealIO struct{}

// NewIO creates a new real IO implementation
func NewIO() IO {
	return &RealIO{}
}

func (i *RealIO) ReadAll(r io.Reader) ([]byte, error) {
	return io.ReadAll(r)
}
