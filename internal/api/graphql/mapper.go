package graphql

import (
	"fmt"
	"time"

	"github.com/feral-file/ff-indexer-v2/internal/api/shared/types"
	"github.com/feral-file/ff-indexer-v2/internal/domain"
)

// convertExpansionStrings converts GraphQL expansion strings to shared types.Expansion
func convertExpansionStrings(gqlExpansions []string) []types.Expansion {
	if gqlExpansions == nil {
		return nil
	}

	expansions := make([]types.Expansion, len(gqlExpansions))
	for i, exp := range gqlExpansions {
		expansions[i] = types.Expansion(exp)
	}
	return expansions
}

// convertChainStrings converts GraphQL chain strings to domain.Chain
func convertChainStrings(chainStrings []string) []domain.Chain {
	if chainStrings == nil {
		return nil
	}

	chains := make([]domain.Chain, len(chainStrings))
	for i, chain := range chainStrings {
		chains[i] = domain.Chain(chain)
	}
	return chains
}

// parseSinceTimestamp parses the "since" timestamp string into a time.Time
func parseSinceTimestamp(since *string) (*time.Time, error) {
	if since == nil || *since == "" {
		return nil, nil
	}

	// Try parsing as RFC3339 first
	t, err := time.Parse(time.RFC3339, *since)
	if err != nil {
		// Try other common formats if needed
		return nil, fmt.Errorf("invalid timestamp format: %s", *since)
	}

	return &t, nil
}
