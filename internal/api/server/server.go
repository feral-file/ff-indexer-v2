package server

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"

	"github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/api/graphql"
	"github.com/feral-file/ff-indexer-v2/internal/api/middleware"
	"github.com/feral-file/ff-indexer-v2/internal/api/rest"
	"github.com/feral-file/ff-indexer-v2/internal/api/shared/executor"
	"github.com/feral-file/ff-indexer-v2/internal/logger"
	"github.com/feral-file/ff-indexer-v2/internal/providers/temporal"
	"github.com/feral-file/ff-indexer-v2/internal/registry"
	"github.com/feral-file/ff-indexer-v2/internal/store"
)

// Config holds the server configuration
type Config struct {
	Debug                 bool
	Host                  string
	Port                  int
	ReadTimeout           time.Duration
	WriteTimeout          time.Duration
	IdleTimeout           time.Duration
	OrchestratorTaskQueue string
	Auth                  middleware.AuthConfig
}

// Server wraps the HTTP server
type Server struct {
	config       Config
	store        store.Store
	orchestrator temporal.TemporalOrchestrator
	blacklist    registry.BlacklistRegistry
	httpServer   *http.Server
	json         adapter.JSON
	clock        adapter.Clock
}

// New creates a new API server
func New(cfg Config, store store.Store, orchestrator temporal.TemporalOrchestrator, blacklist registry.BlacklistRegistry, json adapter.JSON, clock adapter.Clock) *Server {
	return &Server{
		config:       cfg,
		store:        store,
		orchestrator: orchestrator,
		blacklist:    blacklist,
		json:         json,
		clock:        clock,
	}
}

// Start initializes and starts the HTTP server
func (s *Server) Start(ctx context.Context) error {
	// Set Gin mode based on debug flag
	if s.config.Debug {
		gin.SetMode(gin.DebugMode)
	} else {
		gin.SetMode(gin.ReleaseMode)
	}

	// Create Gin router
	router := gin.New()

	// Setup middleware - SentryMiddleware should be first to enable context-based logging
	router.Use(middleware.SentryMiddleware())
	router.Use(middleware.Recovery())
	router.Use(middleware.Logger())
	router.Use(middleware.SetupCORS())

	// Create shared executor (contains business logic shared between REST and GraphQL)
	exec := executor.NewExecutor(s.store, s.orchestrator, s.config.OrchestratorTaskQueue, s.blacklist, s.json, s.clock)

	// Create REST handler
	restHandler := rest.NewHandler(s.config.Debug, exec)

	// Setup REST routes
	rest.SetupRoutes(router, restHandler, s.config.Auth)

	// Create GraphQL handler with auth config
	// Authentication is handled internally for mutations only
	graphqlHandler, err := graphql.NewHandler(s.config.Debug, exec, s.config.Auth)
	if err != nil {
		return fmt.Errorf("failed to create GraphQL handler: %w", err)
	}

	// Setup GraphQL routes
	graphql.SetupRoutes(router, graphqlHandler)

	// Create HTTP server
	addr := fmt.Sprintf("%s:%d", s.config.Host, s.config.Port)
	s.httpServer = &http.Server{
		Addr:         addr,
		Handler:      router,
		ReadTimeout:  s.config.ReadTimeout,
		WriteTimeout: s.config.WriteTimeout,
		IdleTimeout:  s.config.IdleTimeout,
	}

	logger.InfoCtx(ctx, "Starting API server",
		zap.String("address", addr),
	)

	// Start server
	if err := s.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return fmt.Errorf("failed to start server: %w", err)
	}

	return nil
}

// Shutdown gracefully shuts down the server
func (s *Server) Shutdown(ctx context.Context) error {
	logger.InfoCtx(ctx, "Shutting down API server")

	if s.httpServer != nil {
		if err := s.httpServer.Shutdown(ctx); err != nil {
			return fmt.Errorf("failed to shutdown server: %w", err)
		}
	}

	return nil
}
