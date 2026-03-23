package graphql

import (
	"context"

	"github.com/99designs/gqlgen/graphql"

	"github.com/feral-file/ff-indexer-v2/internal/api/shared/types"
	"github.com/feral-file/ff-indexer-v2/internal/domain"
)

// autoDetectTokenExpansions automatically detects which token expansions are needed
// based on the GraphQL query fields that are being requested.
// Supports both individual item queries and list queries by recursively traversing field selections.
func autoDetectTokenExpansions(ctx context.Context) []types.Expansion {
	fc := graphql.GetFieldContext(ctx)
	if fc == nil {
		return nil
	}

	var expansions []types.Expansion
	expansionSet := make(map[types.Expansion]bool)

	// Collect all selected fields from the GraphQL query
	fields := graphql.CollectFieldsCtx(ctx, nil)

	// Get the operation context for recursive field collection
	reqCtx := graphql.GetOperationContext(ctx)

	// Recursively process fields to handle both individual and list queries
	processTokenFields(reqCtx, fields, expansionSet)

	// Convert set to slice
	for expansion := range expansionSet {
		expansions = append(expansions, expansion)
	}

	return expansions
}

// processTokenFields recursively processes GraphQL fields to detect token expansions
func processTokenFields(reqCtx *graphql.OperationContext, fields []graphql.CollectedField, expansionSet map[types.Expansion]bool) {
	for _, field := range fields {
		var expansion types.Expansion
		switch field.Name {
		case "owners":
			expansion = types.ExpansionOwners
		case "provenance_events":
			expansion = types.ExpansionProvenanceEvents
		case "owner_provenances":
			expansion = types.ExpansionOwnerProvenances
		case "metadata":
			expansion = types.ExpansionMetadata
		case "enrichment_source":
			expansion = types.ExpansionEnrichmentSource
		case "display":
			expansion = types.ExpansionDisplay
		case "media_assets":
			expansion = types.ExpansionMediaAsset
		}

		// Add to set if we found an expansion
		if expansion != "" {
			expansionSet[expansion] = true
		}

		// Recursively process nested selections (e.g., for "items" in list queries)
		if len(field.Selections) > 0 {
			nestedFields := graphql.CollectFields(reqCtx, field.Selections, nil)
			processTokenFields(reqCtx, nestedFields, expansionSet)
		}
	}
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

// convertToUint64 converts a slice of Uint64 to a slice of uint64
func convertToUint64(uints []Uint64) []uint64 {
	if uints == nil {
		return nil
	}

	us := make([]uint64, len(uints))
	for i, u := range uints {
		us[i] = uint64(u)
	}
	return us
}
