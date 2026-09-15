package middleware

import (
	"errors"
	"time"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"

	"github.com/feral-file/ff-indexer-v2/internal/logger"
)

// RequestContextComponent wraps the request context with logger.WithComponent so each
// handler and downstream logger.InfoCtx/ErrorCtx call includes the component field.
func RequestContextComponent(component string) gin.HandlerFunc {
	return func(c *gin.Context) {
		ctx := logger.WithComponent(c.Request.Context(), component)
		c.Request = c.Request.WithContext(ctx)
		c.Next()
	}
}

// Logger returns a gin middleware for structured logging using zap
func Logger() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		path := c.Request.URL.Path
		query := c.Request.URL.RawQuery

		c.Next()

		duration := time.Since(start)

		logger.InfoCtx(c.Request.Context(), "API request",
			zap.String("method", c.Request.Method),
			zap.String("path", path),
			zap.String("query", query),
			zap.Int("status", c.Writer.Status()),
			zap.Duration("duration", duration),
			zap.String("client_ip", c.ClientIP()),
			zap.String("user_agent", c.Request.UserAgent()),
		)
	}
}

// Recovery returns a gin middleware for panic recovery with logging
func Recovery() gin.HandlerFunc {
	return func(c *gin.Context) {
		defer func() {
			if err := recover(); err != nil {
				logger.ErrorCtx(c.Request.Context(), errors.New("API panic recovered"), zap.Any("error", err))
				c.AbortWithStatusJSON(500, gin.H{
					"error": "Internal server error",
				})
			}
		}()
		c.Next()
	}
}
