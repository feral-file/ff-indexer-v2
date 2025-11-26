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
func autoDetectTokenExpansions(ctx context.Context) []types.Expansion {
	fc := graphql.GetFieldContext(ctx)
	if fc == nil {
		return nil
	}

	var expansions []types.Expansion
	expansionSet := make(map[types.Expansion]bool)

	// Collect all selected fields from the GraphQL query
	fields := graphql.CollectFieldsCtx(ctx, nil)

	for _, field := range fields {
		var expansion types.Expansion
		switch field.Name {
		case "owners":
			expansion = types.ExpansionOwners
		case "provenance_events":
			expansion = types.ExpansionProvenanceEvents
		case "enrichment_source":
			expansion = types.ExpansionEnrichmentSource
		case "metadata_media_assets":
			expansion = types.ExpansionMetadataMediaAsset
		case "enrichment_source_media_assets":
			expansion = types.ExpansionEnrichmentSourceMediaAsset
		default:
			continue
		}

		// Add to set to avoid duplicates
		if !expansionSet[expansion] {
			expansionSet[expansion] = true
			expansions = append(expansions, expansion)
		}
	}

	return expansions
}

// autoDetectChangeExpansions automatically detects which change expansions are needed
// based on the GraphQL query fields that are being requested.
func autoDetectChangeExpansions(ctx context.Context) []types.Expansion {
	fc := graphql.GetFieldContext(ctx)
	if fc == nil {
		return nil
	}

	var expansions []types.Expansion

	// Collect all selected fields from the GraphQL query
	fields := graphql.CollectFieldsCtx(ctx, nil)

	for _, field := range fields {
		if field.Name == "subject" {
			expansions = append(expansions, types.ExpansionSubject)
			break
		}
	}

	return expansions
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
