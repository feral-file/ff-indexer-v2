package rest

import (
	"net/http"

	logger "github.com/bitmark-inc/autonomy-logger"
	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

// ErrorCode represents a standardized error code
type ErrorCode string

const (
	// Client errors (4xx)
	errCodeBadRequest       ErrorCode = "bad_request"
	errCodeNotFound         ErrorCode = "not_found"
	errCodeValidationFailed ErrorCode = "validation_failed"
	errCodeUnauthorized     ErrorCode = "unauthorized"
	errCodeForbidden        ErrorCode = "forbidden"

	// Server errors (5xx)
	errCodeInternalError ErrorCode = "internal_error"
	errCodeDatabaseError ErrorCode = "database_error"
	errCodeServiceError  ErrorCode = "service_error"
)

// errorResponse represents a standardized error response
type errorResponse struct {
	Error errorDetail `json:"error"`
}

// errorDetail contains error information
type errorDetail struct {
	Code    ErrorCode `json:"code"`
	Message string    `json:"message"`
	Details string    `json:"details,omitempty"`
}

// respondWithError sends a standardized error response
func respondWithError(c *gin.Context, statusCode int, code ErrorCode, message string, details ...string) {
	response := errorResponse{
		Error: errorDetail{
			Code:    code,
			Message: message,
		},
	}

	if len(details) > 0 {
		response.Error.Details = details[0]
	}

	c.JSON(statusCode, response)
}

// respondBadRequest sends a 400 Bad Request response
func respondBadRequest(c *gin.Context, message string, details ...string) {
	respondWithError(c, http.StatusBadRequest, errCodeBadRequest, message, details...)
}

// respondNotFound sends a 404 Not Found response
func respondNotFound(c *gin.Context, message string, details ...string) {
	respondWithError(c, http.StatusNotFound, errCodeNotFound, message, details...)
}

// respondValidationError sends a 400 Bad Request with validation error
func respondValidationError(c *gin.Context, details string) {
	respondWithError(c, http.StatusBadRequest, errCodeValidationFailed, "Validation failed", details)
}

// respondInternalError sends a 500 Internal Server Error response and logs the error
func respondInternalError(c *gin.Context, err error, message string, fields ...zap.Field) {
	logger.Error(err, fields...)
	respondWithError(c, http.StatusInternalServerError, errCodeInternalError, message)
}
