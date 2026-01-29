package graphql

import (
	"context"

	"github.com/99designs/gqlgen/graphql"

	"github.com/feral-file/ff-indexer-v2/internal/api/shared/types"
	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/store/schema"
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
		case "media_assets":
			expansion = types.ExpansionMediaAsset
		case "metadata_media_assets":
			// Deprecated: maintained for backward compatibility
			expansion = types.ExpansionMetadataMediaAsset //nolint:staticcheck // SA1019: deprecated but needed for backward compatibility
		case "enrichment_source_media_assets":
			// Deprecated: maintained for backward compatibility
			expansion = types.ExpansionEnrichmentSourceMediaAsset //nolint:staticcheck // SA1019: deprecated but needed for backward compatibility
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

// autoDetectChangeExpansions automatically detects which change expansions are needed
// based on the GraphQL query fields that are being requested.
// Supports both individual item queries and list queries by recursively traversing field selections.
func autoDetectChangeExpansions(ctx context.Context) []types.Expansion {
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
	processChangeFields(reqCtx, fields, expansionSet)

	// Convert set to slice
	for expansion := range expansionSet {
		expansions = append(expansions, expansion)
	}

	return expansions
}

// processChangeFields recursively processes GraphQL fields to detect change expansions
func processChangeFields(reqCtx *graphql.OperationContext, fields []graphql.CollectedField, expansionSet map[types.Expansion]bool) {
	for _, field := range fields {
		if field.Name == "subject" {
			expansionSet[types.ExpansionSubject] = true
		}

		// Recursively process nested selections (e.g., for "items" in list queries)
		if len(field.Selections) > 0 {
			nestedFields := graphql.CollectFields(reqCtx, field.Selections, nil)
			processChangeFields(reqCtx, nestedFields, expansionSet)
		}
	}
}

// mergeExpansions merges manual and auto-detected expansions, removing duplicates
// Manual expansions take precedence and are listed first for backward compatibility
func mergeExpansions(manual []types.Expansion, autoDetected []types.Expansion) []types.Expansion {
	if len(manual) == 0 && len(autoDetected) == 0 {
		return nil
	}

	expansionSet := make(map[types.Expansion]bool)
	var result []types.Expansion

	// Add manual expansions first (backward compatibility)
	for _, exp := range manual {
		if !expansionSet[exp] {
			expansionSet[exp] = true
			result = append(result, exp)
		}
	}

	// Add auto-detected expansions
	for _, exp := range autoDetected {
		if !expansionSet[exp] {
			expansionSet[exp] = true
			result = append(result, exp)
		}
	}

	return result
}

// convertExpansionStrings converts GraphQL expansion strings to shared types.Expansion
// Deprecated: This is maintained for backward compatibility. Auto-detection is preferred.
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

// convertSubjectTypes converts GraphQL subject types to schema.SubjectType
func convertSubjectTypes(subjectTypes []string) []schema.SubjectType {
	if subjectTypes == nil {
		return nil
	}

	stypes := make([]schema.SubjectType, len(subjectTypes))
	for i, subjectType := range subjectTypes {
		stypes[i] = schema.SubjectType(subjectType)
	}
	return stypes
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
