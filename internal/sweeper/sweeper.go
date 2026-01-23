package sweeper

import (
	"context"
)

// Sweeper defines the interface for sweeper implementations
// Sweepers are long-running background tasks that perform periodic maintenance
//
//go:generate mockgen -source=sweeper.go -destination=../mocks/sweeper.go -package=mocks -mock_names=Sweeper=MockSweeper
type Sweeper interface {
	// Start begins the sweeper's main loop
	// This is a blocking call that runs until the context is canceled
	Start(ctx context.Context) error

	// Stop gracefully stops the sweeper
	// This should wait for any in-progress work to complete
	Stop(ctx context.Context) error

	// Name returns the sweeper's name for logging and identification
	Name() string
}
