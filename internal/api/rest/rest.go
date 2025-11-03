package rest

import (
	"github.com/gin-gonic/gin"
)

// SetupRoutes configures all REST API routes
func SetupRoutes(router *gin.Engine, handler Handler) {
	// Health check endpoint (no auth, no version prefix)
	router.GET("/health", handler.HealthCheck)

	// API v1 routes
	v1 := router.Group("/api/v1")
	{
		// Token endpoints
		v1.GET("/tokens/:cid", handler.GetToken)
		v1.GET("/tokens", handler.ListTokens)
		v1.POST("/tokens/index", handler.TriggerTokenIndexing)

		// Changes endpoint
		v1.GET("/changes", handler.GetChanges)

		// Workflow endpoints
		v1.GET("/workflows/:workflow_id/runs/:run_id", handler.GetWorkflowStatus)
	}
}
