package adapter

import "github.com/gowebpki/jcs"

// JCS defines an interface for JCS operations to enable mocking
//
//go:generate mockgen -source=jsc.go -destination=../mocks/jsc.go -package=mocks -mock_names=JCS=MockJCS
type JCS interface {
	Transform(data []byte) ([]byte, error)
}

// RealJCS implements JCS using the standard jcs package
type RealJCS struct{}

// NewJCS creates a new real JCS implementation
func NewJCS() JCS {
	return &RealJCS{}
}

func (j *RealJCS) Transform(data []byte) ([]byte, error) {
	return jcs.Transform(data)
}
