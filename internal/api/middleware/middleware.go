package middleware

import (
	"fmt"
	"time"

	"github.com/getsentry/sentry-go"
	"github.com/gin-gonic/gin"
	"go.uber.org/zap"

	"github.com/feral-file/ff-indexer-v2/internal/logger"
)

// SentryMiddleware creates a sentry hub and attaches it to the context
// This enables sentry scope tracking per request
func SentryMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		// Create a new hub for this request
		hub := sentry.CurrentHub().Clone()

		// Attach request context to hub
		hub.Scope().SetRequest(c.Request)

		// Set user context if available
		hub.Scope().SetContext("http", map[string]interface{}{
			"method":      c.Request.Method,
			"url":         c.Request.URL.String(),
			"remote_addr": c.ClientIP(),
		})

		// Push hub to context
		ctx := sentry.SetHubOnContext(c.Request.Context(), hub)
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

		// Use context-aware logger for breadcrumbs (Info level)
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
				// Use context-aware logger for errors (Error level sends to sentry)
				logger.ErrorCtx(c.Request.Context(), fmt.Errorf("panic recovered: %v", err),
					zap.String("path", c.Request.URL.Path),
				)
				c.AbortWithStatusJSON(500, gin.H{
					"error": "Internal server error",
				})
			}
		}()
		c.Next()
	}
}
