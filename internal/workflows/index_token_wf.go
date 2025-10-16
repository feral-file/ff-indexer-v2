package workflows

import (
	"github.com/feral-file/ff-indexer-v2/internal/domain"
)

const (
	// IndexTokenWorkflowName is the name of the token indexing workflow
	IndexTokenWorkflowName = "IndexTokenWorkflow"
	// TokenEventSignal is the signal name for new token events
	TokenEventSignal = "TokenEvent"
)

// IndexTokenWorkflowInput represents the input for the token indexing workflow
type IndexTokenWorkflowInput struct {
	TokenCID        string
	Chain           domain.Chain
	Standard        domain.ChainStandard
	ContractAddress string
	TokenNumber     string
}

// IndexTokenWorkflow handles the indexing of a single token
// TODO: Implement workflow logic
func IndexTokenWorkflow(ctx interface{}, input IndexTokenWorkflowInput) error {
	return nil
}
