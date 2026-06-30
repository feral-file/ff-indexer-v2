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
		v1.GET("/releases/:id", handler.GetRelease)

		// Token indexing by CIDs (open, no authentication required)
		v1.POST("/tokens/index", handler.TriggerTokenIndexing)

		// Token indexing by owner addresses with job tracking (requires authentication)
		v1.POST("/tokens/addresses/index", middleware.Auth(authCfg), handler.TriggerAddressIndexing)

		// Token metadata refresh by IDs or CIDs (open, no authentication required)
		v1.POST("/tokens/metadata/index", handler.TriggerMetadataIndexing)

		// Collection sync endpoint (public read access)
		v1.GET("/collection/:address/sync", handler.SyncCollection)

		// Job status (public read access)
		v1.GET("/jobs/:job_id", handler.GetJobStatus)

		// Deprecated routes: path param workflow_id is decimal jobs.id only (not address_indexing_jobs.workflow_id).
		v1.GET("/workflows/:workflow_id/runs/:run_id", handler.GetWorkflowRun)
		v1.GET("/workflows/:workflow_id", handler.GetWorkflowRun)

		// Indexing job endpoints (public read access)
		v1.GET("/indexing/jobs/:job_id", handler.GetAddressIndexingJob)

		// Webhook endpoints (requires API key authentication only)
		v1.POST("/webhooks/clients", middleware.APIKeyAuth(authCfg), handler.CreateWebhookClient)
	}
}
