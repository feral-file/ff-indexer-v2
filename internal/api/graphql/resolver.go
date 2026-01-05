package graphql

import (
	"github.com/feral-file/ff-indexer-v2/internal/api/shared/executor"
)

// Resolver is the root resolver that holds executor
type Resolver struct {
	debug    bool
	executor executor.Executor
}

// NewResolver creates a new root resolver with executor
func NewResolver(debug bool, exec executor.Executor) *Resolver {
	return &Resolver{
		debug:    debug,
		executor: exec,
	}
}
