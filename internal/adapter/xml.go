package adapter

import (
	"encoding/xml"
)

// XML defines an interface for XML operations to enable mocking
//
//go:generate mockgen -source=xml.go -destination=../mocks/xml.go -package=mocks -mock_names=XML=MockXML
type XML interface {
	Unmarshal(data []byte, v interface{}) error
}

type RealXML struct{}

func NewXML() XML {
	return &RealXML{}
}

func (x *RealXML) Unmarshal(data []byte, v interface{}) error {
	return xml.Unmarshal(data, v)
}
