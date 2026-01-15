package graphql

import (
	"context"

	"github.com/99designs/gqlgen/graphql"
	"github.com/99designs/gqlgen/graphql/handler"
	"github.com/99designs/gqlgen/graphql/playground"
	"github.com/gin-gonic/gin"
	"github.com/vektah/gqlparser/v2/ast"

	"go.uber.org/zap"

	"github.com/feral-file/ff-indexer-v2/internal/api/middleware"
	"github.com/feral-file/ff-indexer-v2/internal/api/shared/executor"
	"github.com/feral-file/ff-indexer-v2/internal/logger"
)

// Handler defines the interface for GraphQL API handlers
type Handler interface {
	// HandleGraphQL handles GraphQL requests
	HandleGraphQL(c *gin.Context)

	// HandlePlayground serves the GraphQL Playground
	HandlePlayground(c *gin.Context)
}

// gqlHandler implements the Handler interface using gqlgen
type gqlHandler struct {
	debug      bool
	server     *handler.Server
	authConfig middleware.AuthConfig
}

// NewHandler creates a new GraphQL handler with gqlgen
func NewHandler(debug bool, exec executor.Executor, authCfg middleware.AuthConfig) (Handler, error) {
	// Create resolver with executor
	resolver := NewResolver(debug, exec)

	// Create executable schema
	config := Config{Resolvers: resolver}
	schema := NewExecutableSchema(config)

	// Create gqlgen server with custom error presenter
	srv := handler.NewDefaultServer(schema)
	srv.SetErrorPresenter(ErrorPresenter)
	srv.SetRecoverFunc(RecoverFunc)

	h := &gqlHandler{
		debug:      debug,
		server:     srv,
		authConfig: authCfg,
	}

	// Add authentication middleware for mutations only
	srv.AroundOperations(h.authMiddleware)

	return h, nil
}

// authMiddleware authenticates GraphQL mutations using the shared authentication logic
// Only applies authentication to mutations that require it
func (h *gqlHandler) authMiddleware(ctx context.Context, next graphql.OperationHandler) graphql.ResponseHandler {
	opctx := graphql.GetOperationContext(ctx)

	// Only authenticate specific mutations that require authentication
	if opctx.Operation != nil && opctx.Operation.Operation == ast.Mutation {
		// Check if this is a mutation that requires authentication
		requiresAuth := false
		requiresAPIKeyOnly := false
		mutationName := ""

		if opctx.Operation.SelectionSet != nil {
			for _, selection := range opctx.Operation.SelectionSet {
				if field, ok := selection.(*ast.Field); ok {
					mutationName = field.Name
					// triggerOwnerIndexing requires authentication (JWT or API key)
					if field.Name == "triggerOwnerIndexing" {
						requiresAuth = true
						break
					}
					if field.Name == "triggerAddressIndexing" {
						requiresAuth = true
						break
					}
					// createWebhookClient requires API key only
					if field.Name == "createWebhookClient" {
						requiresAuth = true
						requiresAPIKeyOnly = true
						break
					}
				}
			}
		}

		if requiresAuth {
			// Get Authorization header from the HTTP request
			authHeader := ""
			if opctx.Headers != nil {
				authHeader = opctx.Headers.Get("Authorization")
			}

			// Authenticate using the shared authentication logic
			result := middleware.Authenticate(authHeader, h.authConfig)

			if !result.Success {
				logger.WarnCtx(ctx, "GraphQL mutation authentication failed",
					zap.Error(result.Error),
					zap.String("operation", mutationName),
				)
				return func(ctx context.Context) *graphql.Response {
					return graphql.ErrorResponse(ctx, "Authentication required for this mutation")
				}
			}

			// For API key-only mutations, verify it's not JWT
			if requiresAPIKeyOnly && result.AuthType != "apikey" {
				logger.WarnCtx(ctx, "GraphQL mutation requires API key authentication only",
					zap.String("operation", mutationName),
					zap.String("auth_type", result.AuthType),
				)
				return func(ctx context.Context) *graphql.Response {
					return graphql.ErrorResponse(ctx, "This mutation requires API key authentication only (JWT not accepted)")
				}
			}

			// Store authentication info in context for resolvers to access
			ctx = context.WithValue(ctx, middleware.AUTH_TYPE_KEY, result.AuthType)
			if result.Claims != nil {
				ctx = context.WithValue(ctx, middleware.JWT_CLAIMS_KEY, result.Claims)
			}
			if result.AuthSubject != "" {
				ctx = context.WithValue(ctx, middleware.AUTH_SUBJECT_KEY, result.AuthSubject)
			}

			logger.DebugCtx(ctx, "GraphQL mutation authentication successful",
				zap.String("operation", mutationName),
				zap.String("auth_type", result.AuthType),
			)
		}
	}

	return next(ctx)
}

// HandleGraphQL processes GraphQL queries and mutations
func (h *gqlHandler) HandleGraphQL(c *gin.Context) {
	h.server.ServeHTTP(c.Writer, c.Request)
}

// HandlePlayground serves the GraphQL Playground interface
func (h *gqlHandler) HandlePlayground(c *gin.Context) {
	playground.Handler("Indexer GraphQL Playground", "/graphql").ServeHTTP(c.Writer, c.Request)
}

// SetupRoutes configures GraphQL API routes
func SetupRoutes(router *gin.Engine, handler Handler) {
	// GraphQL endpoint (POST for queries/mutations)
	router.POST("/graphql", handler.HandleGraphQL)

	// GraphQL Playground (GET for interactive IDE)
	router.GET("/graphql", handler.HandlePlayground)
}
