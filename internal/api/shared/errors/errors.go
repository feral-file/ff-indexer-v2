package errors

import (
	"encoding/json"
	"strings"
)

// ErrorCode represents a standardized error code
type ErrorCode string

const (
	// Client errors (4xx)
	ErrCodeBadRequest       ErrorCode = "bad_request"
	ErrCodeNotFound         ErrorCode = "not_found"
	ErrCodeValidationFailed ErrorCode = "validation_failed"
	ErrCodeUnauthorized     ErrorCode = "unauthorized"
	ErrCodeForbidden        ErrorCode = "forbidden"

	// Server errors (5xx)
	ErrCodeInternalError ErrorCode = "internal_error"
	ErrCodeDatabaseError ErrorCode = "database_error"
	ErrCodeServiceError  ErrorCode = "service_error"
)

// APIError represents a structured API error that carries error code and details
// This is the shared error type used by both REST and GraphQL
type APIError struct {
	Code    ErrorCode `json:"code"`
	Message string    `json:"message"`
	Details string    `json:"details,omitempty"`
}

func (e *APIError) Error() string {
	jsonErr, _ := json.Marshal(e)
	return string(jsonErr)
}

// Error constructors for common error types
func NewBadRequestError(message string, details ...string) *APIError {
	return &APIError{
		Code:    ErrCodeBadRequest,
		Message: message,
		Details: strings.Join(details, ", "),
	}
}

func NewNotFoundError(message string, details ...string) *APIError {
	return &APIError{
		Code:    ErrCodeNotFound,
		Message: message,
		Details: strings.Join(details, ", "),
	}
}

func NewValidationError(details ...string) *APIError {
	return &APIError{
		Code:    ErrCodeValidationFailed,
		Message: "Validation failed",
		Details: strings.Join(details, ", "),
	}
}

func NewUnauthorizedError(message string, details ...string) *APIError {
	return &APIError{
		Code:    ErrCodeUnauthorized,
		Message: message,
		Details: strings.Join(details, ", "),
	}
}

func NewForbiddenError(message string, details ...string) *APIError {
	return &APIError{
		Code:    ErrCodeForbidden,
		Message: message,
		Details: strings.Join(details, ", "),
	}
}

func NewInternalError(message string, details ...string) *APIError {
	return &APIError{
		Code:    ErrCodeInternalError,
		Message: message,
		Details: strings.Join(details, ", "),
	}
}

func NewDatabaseError(message string, details ...string) *APIError {
	return &APIError{
		Code:    ErrCodeDatabaseError,
		Message: message,
		Details: strings.Join(details, ", "),
	}
}

func NewServiceError(message string, details ...string) *APIError {
	return &APIError{
		Code:    ErrCodeServiceError,
		Message: message,
		Details: strings.Join(details, ", "),
	}
}
