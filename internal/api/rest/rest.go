package rest

import (
	"github.com/gin-gonic/gin"

	"github.com/feral-file/ff-indexer-v2/internal/api/middleware"
)

// SetupRoutes configures all REST API routes
func SetupRoutes(router *gin.Engine, handler Handler, authCfg middleware.AuthConfig) {
	// Health check endpoint (no auth, no version prefix)
	router.GET("/health", handler.HealthCheck)

	// API v1 routes
	v1 := router.Group("/api/v1")
	{
		// Token endpoints (public read access)
		v1.GET("/tokens/:cid", handler.GetToken)
		v1.GET("/tokens", handler.ListTokens)

		// Token indexing by CIDs (open, no authentication required)
		v1.POST("/tokens/index", handler.TriggerTokenIndexing)

		// Token indexing by owner addresses (requires authentication)
		v1.POST("/tokens/owners/index", middleware.Auth(authCfg), handler.TriggerOwnerIndexing)

		// Token metadata refresh by IDs or CIDs (open, no authentication required)
		v1.POST("/tokens/metadata/index", handler.TriggerMetadataIndexing)

		// Changes endpoint (public read access)
		v1.GET("/changes", handler.GetChanges)

		// Workflow endpoints (public read access)
		v1.GET("/workflows/:workflow_id/runs/:run_id", handler.GetWorkflowStatus)
	}
}
