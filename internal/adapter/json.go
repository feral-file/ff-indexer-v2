package adapter

import (
	"encoding/json"
)

// JSON defines an interface for JSON operations to enable mocking
//
//go:generate mockgen -source=json.go -destination=../mocks/json.go -package=mocks -mock_names=JSON=MockJSON
type JSON interface {
	Marshal(v interface{}) ([]byte, error)
	Unmarshal(data []byte, v interface{}) error
}

// RealJSON implements JSON using the standard encoding/json package
type RealJSON struct{}

// NewJSON creates a new real JSON implementation
func NewJSON() JSON {
	return &RealJSON{}
}

func (j *RealJSON) Marshal(v interface{}) ([]byte, error) {
	return json.Marshal(v)
}

func (j *RealJSON) Unmarshal(data []byte, v interface{}) error {
	return json.Unmarshal(data, v)
}
