package server

import (
	"context"
	"fmt"
	"net/http"
	"time"

	logger "github.com/bitmark-inc/autonomy-logger"
	"github.com/gin-gonic/gin"
	"go.uber.org/zap"

	"github.com/feral-file/ff-indexer-v2/internal/api/middleware"
	"github.com/feral-file/ff-indexer-v2/internal/api/rest"
	"github.com/feral-file/ff-indexer-v2/internal/store"
)

// Config holds the server configuration
type Config struct {
	Debug        bool
	Host         string
	Port         int
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	IdleTimeout  time.Duration
}

// Server wraps the HTTP server
type Server struct {
	config     Config
	store      store.Store
	httpServer *http.Server
}

// New creates a new API server
func New(cfg Config, store store.Store) *Server {
	return &Server{
		config: cfg,
		store:  store,
	}
}

// Start initializes and starts the HTTP server
func (s *Server) Start() error {
	// Set Gin mode based on debug flag
	if s.config.Debug {
		gin.SetMode(gin.DebugMode)
	} else {
		gin.SetMode(gin.ReleaseMode)
	}

	// Create Gin router
	router := gin.New()

	// Setup middleware
	router.Use(middleware.Recovery())
	router.Use(middleware.Logger())
	router.Use(middleware.SetupCORS())

	// Create REST handler
	restHandler := rest.NewHandler(s.store)

	// Setup routes
	rest.SetupRoutes(router, restHandler)

	// Create HTTP server
	addr := fmt.Sprintf("%s:%d", s.config.Host, s.config.Port)
	s.httpServer = &http.Server{
		Addr:         addr,
		Handler:      router,
		ReadTimeout:  s.config.ReadTimeout,
		WriteTimeout: s.config.WriteTimeout,
		IdleTimeout:  s.config.IdleTimeout,
	}

	logger.Info("Starting API server",
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
	logger.Info("Shutting down API server")

	if s.httpServer != nil {
		if err := s.httpServer.Shutdown(ctx); err != nil {
			return fmt.Errorf("failed to shutdown server: %w", err)
		}
	}

	return nil
}
