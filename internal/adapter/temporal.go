package adapter

import (
	"context"

	"go.temporal.io/sdk/activity"
)

//go:generate mockgen -source=temporal.go -destination=../mocks/temporal.go -package=mocks -mock_names=Activity=MockActivity
type Activity interface {
	// GetInfo returns the activity info
	GetInfo(ctx context.Context) activity.Info
}

// RealActivity implements Activity using the standard activity package
type RealActivity struct{}

// NewActivity creates a new real activity implementation
func NewActivity() Activity {
	return &RealActivity{}
}

// GetInfo returns the activity info
func (a *RealActivity) GetInfo(ctx context.Context) activity.Info {
	return activity.GetInfo(ctx)
}
