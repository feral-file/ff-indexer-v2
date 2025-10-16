package workflows

import (
	"github.com/feral-file/ff-indexer-v2/internal/domain"
)

const (
	// IndexMediaWorkflowName is the name of the media indexing workflow
	IndexMediaWorkflowName = "IndexMediaWorkflow"
	// MediaEventSignal is the signal name for media processing events
	MediaEventSignal = "MediaEvent"
)

// IndexMediaWorkflowInput represents the input for the media indexing workflow
type IndexMediaWorkflowInput struct {
	TokenCID        string
	Chain           domain.Chain
	ContractAddress string
	TokenNumber     string
}

// IndexMediaWorkflow handles the media processing for a single token
// TODO: Implement workflow logic
func IndexMediaWorkflow(ctx interface{}, input IndexMediaWorkflowInput) error {
	return nil
}
