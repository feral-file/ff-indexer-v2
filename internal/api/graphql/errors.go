package graphql

import (
	"context"
	"errors"

	"github.com/99designs/gqlgen/graphql/errcode"
	"github.com/vektah/gqlparser/v2/gqlerror"
	"go.uber.org/zap"

	apierrors "github.com/feral-file/ff-indexer-v2/internal/api/shared/errors"
	"github.com/feral-file/ff-indexer-v2/internal/logger"
)

// ErrorPresenter formats errors in a consistent way matching the REST API format
// This is the GraphQL adapter for shared APIError
// This function is called by gqlgen for every error
func ErrorPresenter(ctx context.Context, err error) *gqlerror.Error {
	// Convert to gqlerror if not already
	var gqlErr *gqlerror.Error
	if errors.As(err, &gqlErr) {
		if presented, ok := presentProtocolError(gqlErr); ok {
			return presented
		}
	} else {
		gqlErr = &gqlerror.Error{
			Message: err.Error(),
		}
	}

	// Check if it's our shared APIError type
	var apiErr *apierrors.APIError
	if errors.As(err, &apiErr) {
		switch apiErr.Code {
		case apierrors.ErrCodeInternalError, apierrors.ErrCodeServiceError, apierrors.ErrCodeDatabaseError:
			return handleInternalError(ctx, err)
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
		return handleInternalError(ctx, err)
	}

	return gqlErr
}

// presentProtocolError presents an error gqlgen raised before execution: a
// query that fails to parse or validate against the schema (served as HTTP
// 422), or one tagged by a handler extension such as APQ's
// PERSISTED_QUERY_NOT_FOUND.
//
// Reason: these are the client's mistake and their message is the only
// diagnosis it gets ("Cannot query field \"token_cid\" on type \"TokenList\"").
// Masking them as internal_error made a malformed query look like a backend
// failure (issue #82) and logged every one at ERROR. Parse and validation
// failures map to validation_failed, the shared code for a well-formed request
// that breaks the contract; any other gqlgen code is kept verbatim because it
// is a protocol signal its client acts on (APQ re-sends the full query on
// PERSISTED_QUERY_NOT_FOUND). Constraints: the error is copied, never
// mutated — gqlgen reads the original's code after presentation to pick the
// HTTP status and the websocket message type, and rewriting it in place would
// turn the 422 into a 200. Errors carrying no code (resolver failures) return
// ok=false and keep the existing APIError handling.
func presentProtocolError(gqlErr *gqlerror.Error) (*gqlerror.Error, bool) {
	code, _ := gqlErr.Extensions["code"].(string)
	if code == "" {
		return nil, false
	}
	if code == errcode.ParseFailed || code == errcode.ValidationFailed {
		code = string(apierrors.ErrCodeValidationFailed)
	}
	return &gqlerror.Error{
		Message:   gqlErr.Message,
		Path:      gqlErr.Path,
		Locations: gqlErr.Locations,
		Rule:      gqlErr.Rule,
		Extensions: map[string]interface{}{
			"code":    code,
			"message": gqlErr.Message,
		},
	}, true
}

// handleInternalError handles internal errors and returns a gqlerror.Error
func handleInternalError(ctx context.Context, err error) *gqlerror.Error {
	logger.ErrorCtx(ctx, errors.New("unhandled GraphQL error"), zap.Error(err))
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
	logger.ErrorCtx(ctx, errors.New("graphQL panic recovered"), zap.Any("error", err))
	return apierrors.NewInternalError("Internal server error")
}
