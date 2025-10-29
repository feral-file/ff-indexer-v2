package graphql

import (
	"encoding/json"
	"fmt"
	"io"
	"math"
	"strconv"

	apierrors "github.com/feral-file/ff-indexer-v2/internal/api/shared/errors"
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
			return apierrors.NewValidationError(fmt.Sprintf("cannot parse %q as uint64: %v", v, err))
		}
		*u = Uint64(val)
		return nil
	case int:
		if v < 0 {
			return apierrors.NewValidationError(fmt.Sprintf("uint64 cannot be negative: %d", v))
		}
		*u = Uint64(v)
		return nil
	case int64:
		if v < 0 {
			return apierrors.NewValidationError(fmt.Sprintf("uint64 cannot be negative: %d", v))
		}
		*u = Uint64(v)
		return nil
	case uint64:
		*u = Uint64(v)
		return nil
	default:
		return apierrors.NewValidationError(fmt.Sprintf("cannot unmarshal %q to Uint64", v))
	}
}

// Uint8 scalar type for unsigned 8-bit integers
type Uint8 uint8

// ToNativeUint8 converts a Uint8 to a uint8, returning nil if the Uint8 is nil
func ToNativeUint8(u *Uint8) *uint8 {
	if u == nil {
		return nil
	}
	nu := uint8(*u)
	return &nu
}

// FromNativeUint8 converts a uint8 to a Uint8, returning nil if the uint8 is nil
func FromNativeUint8(u *uint8) *Uint8 {
	if u == nil {
		return nil
	}
	gu := Uint8(*u)
	return &gu
}

// MarshalGQL implements graphql.Marshaler for Uint8
// Note: Marshals as a number (not string) since uint8 max value (255) is safe in JSON/JS
func (u Uint8) MarshalGQL(w io.Writer) {
	_, _ = io.WriteString(w, strconv.Itoa(int(u)))
}

// UnmarshalGQL implements graphql.Unmarshaler for Uint8
func (u *Uint8) UnmarshalGQL(v interface{}) error {
	switch v := v.(type) {
	case int:
		// Most common case from GraphQL Int input - check first
		if v < 0 || v > math.MaxUint8 {
			return apierrors.NewValidationError(fmt.Sprintf("uint8 value out of range [0-255]: %d", v))
		}
		*u = Uint8(v)
		return nil
	case int64:
		if v < 0 || v > math.MaxUint8 {
			return apierrors.NewValidationError(fmt.Sprintf("uint8 value out of range [0-255]: %d", v))
		}
		*u = Uint8(v)
		return nil
	case string:
		val, err := strconv.ParseUint(v, 10, 8)
		if err != nil {
			return apierrors.NewValidationError(fmt.Sprintf("cannot parse %q as uint8: %v", v, err))
		}
		*u = Uint8(val)
		return nil
	case uint8:
		*u = Uint8(v)
		return nil
	case json.Number:
		val, err := v.Int64()
		if err != nil {
			return apierrors.NewValidationError(fmt.Sprintf("cannot parse %q as uint8: %v", v, err))
		}
		if val < 0 || val > math.MaxUint8 {
			return apierrors.NewValidationError(fmt.Sprintf("uint8 value out of range [0-255]: %d", val))
		}
		*u = Uint8(val)
		return nil

	default:
		return apierrors.NewValidationError(fmt.Sprintf("cannot unmarshal %q to Uint8", v))
	}
}
