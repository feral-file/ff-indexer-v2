package adapter

import "encoding/base64"

// Base64 defines an interface for Base64 operations to enable mocking
//
//go:generate mockgen -source=encoding.go -destination=../mocks/encoding.go -package=mocks -mock_names=Base64=MockBase64
type Base64 interface {
	Encode(data []byte) string
	Decode(data string) ([]byte, error)
}

type RealBase64 struct{}

func NewBase64() Base64 {
	return &RealBase64{}
}

func (b *RealBase64) Encode(data []byte) string {
	return base64.StdEncoding.EncodeToString(data)
}

func (b *RealBase64) Decode(data string) ([]byte, error) {
	return base64.StdEncoding.DecodeString(data)
}
