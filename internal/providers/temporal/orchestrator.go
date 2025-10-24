package temporal

import (
	"context"

	"go.temporal.io/sdk/client"
)

//go:generate mockgen -source=orchestrator.go -destination=../../mocks/temporal_orchestrator.go -package=mocks -mock_names=TemporalOrchestrator=MockTemporalOrchestrator
type TemporalOrchestrator interface {
	ExecuteWorkflow(ctx context.Context, options client.StartWorkflowOptions, workflow interface{}, args ...interface{}) (client.WorkflowRun, error)
}
