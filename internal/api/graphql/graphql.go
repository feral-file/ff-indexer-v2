package graphql

import (
	"github.com/99designs/gqlgen/graphql/handler"
	"github.com/99designs/gqlgen/graphql/playground"
	"github.com/gin-gonic/gin"

	"github.com/feral-file/ff-indexer-v2/internal/api/shared/executor"
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
	server *handler.Server
}

// NewHandler creates a new GraphQL handler with gqlgen
func NewHandler(exec executor.Executor) (Handler, error) {
	// Create resolver with executor
	resolver := NewResolver(exec)

	// Create executable schema
	config := Config{Resolvers: resolver}
	schema := NewExecutableSchema(config)

	// Create gqlgen server with custom error presenter
	srv := handler.NewDefaultServer(schema)
	srv.SetErrorPresenter(ErrorPresenter)
	srv.SetRecoverFunc(RecoverFunc)

	return &gqlHandler{
		server: srv,
	}, nil
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
