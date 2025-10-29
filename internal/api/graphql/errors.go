package graphql

import (
	"context"
	"errors"
	"fmt"

	logger "github.com/bitmark-inc/autonomy-logger"
	"github.com/vektah/gqlparser/v2/gqlerror"
	"go.uber.org/zap"

	apierrors "github.com/feral-file/ff-indexer-v2/internal/api/shared/errors"
)

// ErrorPresenter formats errors in a consistent way matching the REST API format
// This is the GraphQL adapter for shared APIError
// This function is called by gqlgen for every error
func ErrorPresenter(ctx context.Context, err error) *gqlerror.Error {
	// Convert to gqlerror if not already
	var gqlErr *gqlerror.Error
	if !errors.As(err, &gqlErr) {
		gqlErr = &gqlerror.Error{
			Message: err.Error(),
		}
	}

	// Check if it's our shared APIError type
	var apiErr *apierrors.APIError
	if errors.As(err, &apiErr) {
		switch apiErr.Code {
		case apierrors.ErrCodeInternalError, apierrors.ErrCodeServiceError, apierrors.ErrCodeDatabaseError:
			return handleInternalError(err)
		case apierrors.ErrCodeNotFound:
			return handleNotFoundError(err)
		default:
			gqlErr.Message = apiErr.Message
			gqlErr.Extensions = map[string]interface{}{
				"code":    string(apiErr.Code),
				"message": apiErr.Message,
			}
			if apiErr.Details != "" {
				gqlErr.Extensions["details"] = apiErr.Details
			}
		}
	} else {
		// For unknown errors, log them and return a generic internal error
		return handleInternalError(err)
	}

	return gqlErr
}

// handleInternalError handles internal errors and returns a gqlerror.Error
func handleInternalError(err error) *gqlerror.Error {
	logger.Error(err, zap.String("error", "Unhandled GraphQL error"))
	return &gqlerror.Error{
		Message: "Internal server error",
		Extensions: map[string]interface{}{
			"code":    string(apierrors.ErrCodeInternalError),
			"message": "Internal server error",
		},
	}
}

// handleNotFoundError handles not found errors and returns a gqlerror.Error
func handleNotFoundError(err error) *gqlerror.Error {
	return &gqlerror.Error{
		Message: "Not found",
		Extensions: map[string]interface{}{
			"code":    string(apierrors.ErrCodeNotFound),
			"message": err.Error(),
		},
	}
}

// RecoverFunc handles panics in resolvers
func RecoverFunc(ctx context.Context, err interface{}) error {
	logger.Error(fmt.Errorf("panic: %v", err), zap.Any("panic", err))
	return apierrors.NewInternalError("Internal server error")
}
