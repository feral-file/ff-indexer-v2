package rest

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/feral-file/ff-indexer-v2/internal/api/shared/errors"
)

// respondBadRequest responds with a bad request error
func respondBadRequest(c *gin.Context, message string, details ...string) {
	c.JSON(http.StatusBadRequest, errors.NewBadRequestError(message, details...))
}

// respondNotFound responds with a not found error
func respondNotFound(c *gin.Context, message string, details ...string) {
	c.JSON(http.StatusNotFound, errors.NewNotFoundError(message, details...))
}

// respondValidationError responds with a validation error
func respondValidationError(c *gin.Context, message string) {
	c.JSON(http.StatusUnprocessableEntity, errors.NewValidationError(message))
}

// respondInternalError responds with an internal server error
func respondInternalError(c *gin.Context, err error, message string, details ...string) {
	c.JSON(http.StatusInternalServerError, errors.NewInternalError(message, details...))
}
