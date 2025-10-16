package adapter

import (
	"context"
)

// WorkflowClient defines the interface for workflow orchestration client operations
// This interface matches the methods we need from Temporal/Cadence client
//
//go:generate mockgen -source=temporal.go -destination=../mocks/temporal.go -package=mocks -mock_names=WorkflowClient=MockWorkflowClient
type WorkflowClient interface {
	// ExecuteWorkflow starts a workflow execution
	ExecuteWorkflow(ctx context.Context, options WorkflowOptions, workflow interface{}, args ...interface{}) (WorkflowRun, error)
	// SignalWithStartWorkflow signals an existing workflow or starts a new one if it doesn't exist
	SignalWithStartWorkflow(ctx context.Context, workflowID string, signalName string, signalArg interface{},
		options WorkflowOptions, workflow interface{}, args ...interface{}) (WorkflowRun, error)
	// Close closes the client connection
	Close()
}

// WorkflowOptions defines options for starting a workflow
type WorkflowOptions struct {
	ID        string
	TaskQueue string
}

// WorkflowRun represents a running workflow
type WorkflowRun interface {
	GetID() string
	GetRunID() string
}

// TemporalWorkflow defines an interface for creating workflow clients
//
//go:generate mockgen -source=temporal.go -destination=../mocks/temporal.go -package=mocks -mock_names=TemporalWorkflow=MockTemporalWorkflow
type TemporalWorkflow interface {
	Connect(hostPort string, namespace string) (WorkflowClient, error)
}

// RealTemporalWorkflow implements TemporalWorkflow using the Temporal SDK
type RealTemporalWorkflow struct{}

// NewTemporalWorkflow creates a new real Temporal workflow client factory
func NewTemporalWorkflow() TemporalWorkflow {
	return &RealTemporalWorkflow{}
}

func (t *RealTemporalWorkflow) Connect(hostPort string, namespace string) (WorkflowClient, error) {
	// TODO: Import and use actual Temporal SDK
	// import "go.temporal.io/sdk/client"
	// c, err := client.Dial(client.Options{
	// 	HostPort:  hostPort,
	// 	Namespace: namespace,
	// })
	// if err != nil {
	// 	return nil, err
	// }
	// return c, nil

	return &stubClient{}, nil
}

// stubClient is a temporary placeholder until Temporal SDK is properly integrated
type stubClient struct{}

func (s *stubClient) ExecuteWorkflow(ctx context.Context, options WorkflowOptions, workflow interface{}, args ...interface{}) (WorkflowRun, error) {
	return &stubRun{id: options.ID}, nil
}

func (s *stubClient) SignalWithStartWorkflow(ctx context.Context, workflowID string, signalName string, signalArg interface{},
	options WorkflowOptions, workflow interface{}, args ...interface{}) (WorkflowRun, error) {
	return &stubRun{id: workflowID}, nil
}

func (s *stubClient) Close() {}

type stubRun struct {
	id string
}

func (s *stubRun) GetID() string    { return s.id }
func (s *stubRun) GetRunID() string { return "" }
