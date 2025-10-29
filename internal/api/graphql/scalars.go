package graphql

import (
	"encoding/json"
	"fmt"
	"io"
	"strconv"
)

// JSON is a custom scalar type for arbitrary JSON data
type JSON json.RawMessage

// Implement graphql.Marshaler interface
func (j JSON) MarshalGQL(w io.Writer) {
	if j == nil {
		_, _ = w.Write([]byte("null"))
		return
	}
	_, _ = w.Write(j)
}

// Implement graphql.Unmarshaler interface
func (j *JSON) UnmarshalGQL(v interface{}) error {
	switch v := v.(type) {
	case []byte:
		*j = append((*j)[0:0], v...)
		return nil
	case string:
		*j = JSON(v)
		return nil
	case json.RawMessage:
		*j = JSON(v)
		return nil
	case map[string]interface{}, []interface{}:
		// Marshal the parsed JSON back to bytes
		b, err := json.Marshal(v)
		if err != nil {
			return fmt.Errorf("failed to marshal JSON: %w", err)
		}
		*j = JSON(b)
		return nil
	case nil:
		*j = nil
		return nil
	default:
		return fmt.Errorf("cannot unmarshal %T to JSON", v)
	}
}

// Uint64 scalar type for unsigned 64-bit integers
type Uint64 uint64

// ToNativeUint64 converts a Uint64 to a uint64, returning nil if the Uint64 is nil
func ToNativeUint64(u *Uint64) *uint64 {
	if u == nil {
		return nil
	}

	nu := uint64(*u)
	return &nu
}

// FromNativeUint64 converts a uint64 to a Uint64, returning nil if the uint64 is nil
func FromNativeUint64(u *uint64) *Uint64 {
	if u == nil {
		return nil
	}

	gu := Uint64(*u)
	return &gu
}

// MarshalGQL implements graphql.Marshaler for Uint64
func (u Uint64) MarshalGQL(w io.Writer) {
	// Write as string to avoid JavaScript number precision issues
	_, _ = io.WriteString(w, strconv.Quote(strconv.FormatUint(uint64(u), 10)))
}

// UnmarshalGQL implements graphql.Unmarshaler for Uint64
func (u *Uint64) UnmarshalGQL(v interface{}) error {
	switch v := v.(type) {
	case string:
		val, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			return fmt.Errorf("cannot parse %q as uint64: %w", v, err)
		}
		*u = Uint64(val)
		return nil
	case int:
		if v < 0 {
			return fmt.Errorf("uint64 cannot be negative: %d", v)
		}
		*u = Uint64(v)
		return nil
	case int64:
		if v < 0 {
			return fmt.Errorf("uint64 cannot be negative: %d", v)
		}
		*u = Uint64(v)
		return nil
	case uint64:
		*u = Uint64(v)
		return nil
	default:
		return fmt.Errorf("cannot unmarshal %T to Uint64", v)
	}
}
