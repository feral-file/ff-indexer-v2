package types

import (
	"github.com/feral-file/ff-indexer-v2/internal/store"
)

// ToStoreTokenSortBy converts API TokenSortBy to store TokenSortBy
func ToStoreTokenSortBy(sortBy TokenSortBy) store.TokenSortBy {
	switch sortBy {
	case TokenSortByCreatedAt:
		return store.TokenSortByCreatedAt
	case TokenLatestProvenance:
		return store.TokenSortByLatestProvenance
	default:
		return store.TokenSortByCreatedAt
	}
}

// ToStoreSortOrder converts API Order to store SortOrder
func ToStoreSortOrder(order Order) store.SortOrder {
	switch order {
	case OrderAsc:
		return store.SortOrderAsc
	case OrderDesc:
		return store.SortOrderDesc
	default:
		return store.SortOrderDesc
	}
}
