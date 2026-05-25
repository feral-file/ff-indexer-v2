package adapter

import (
	"context"
	"fmt"
)

// ERC721StandardAdapter delegates to the existing ERC-721 client methods.
type ERC721StandardAdapter struct {
	ops StandardOperations
}

// NewERC721StandardAdapter creates an adapter that wraps standard ERC-721 operations.
func NewERC721StandardAdapter(ops StandardOperations) *ERC721StandardAdapter {
	return &ERC721StandardAdapter{ops: ops}
}

// TokenExists checks existence via ownerOf, treating execution reverts as non-existence.
func (a *ERC721StandardAdapter) TokenExists(ctx context.Context, contractAddress, tokenNumber string) (bool, error) {
	_, err := a.ops.ERC721OwnerOf(ctx, contractAddress, tokenNumber)
	if err != nil {
		if isExecutionRevert(err) {
			return false, nil
		}
		return false, fmt.Errorf("failed to check ERC721 token existence: %w", err)
	}
	return true, nil
}

// TokenOwner returns the current ERC-721 owner.
func (a *ERC721StandardAdapter) TokenOwner(ctx context.Context, contractAddress, tokenNumber string) (string, error) {
	return a.ops.ERC721OwnerOf(ctx, contractAddress, tokenNumber)
}

// TokenURI returns the ERC-721 tokenURI value.
func (a *ERC721StandardAdapter) TokenURI(ctx context.Context, contractAddress, tokenNumber string) (string, error) {
	return a.ops.ERC721TokenURI(ctx, contractAddress, tokenNumber)
}

// SupportsProvenance reports that standard ERC-721 provenance indexing is supported.
func (a *ERC721StandardAdapter) SupportsProvenance() bool {
	return true
}

// ERC1155StandardAdapter delegates to the existing ERC-1155 client methods.
type ERC1155StandardAdapter struct {
	ops StandardOperations
}

// NewERC1155StandardAdapter creates an adapter that wraps standard ERC-1155 operations.
func NewERC1155StandardAdapter(ops StandardOperations) *ERC1155StandardAdapter {
	return &ERC1155StandardAdapter{ops: ops}
}

// TokenExists checks existence via the ERC-1155 transfer scan implemented on the client.
func (a *ERC1155StandardAdapter) TokenExists(ctx context.Context, contractAddress, tokenNumber string) (bool, error) {
	return a.ops.ERC1155TokenExists(ctx, contractAddress, tokenNumber)
}

// TokenOwner is unsupported for fungible ERC-1155 tokens.
func (a *ERC1155StandardAdapter) TokenOwner(ctx context.Context, contractAddress, tokenNumber string) (string, error) {
	return "", fmt.Errorf("ERC1155 does not support single-owner lookup")
}

// TokenURI returns the ERC-1155 uri value.
func (a *ERC1155StandardAdapter) TokenURI(ctx context.Context, contractAddress, tokenNumber string) (string, error) {
	return a.ops.ERC1155URI(ctx, contractAddress, tokenNumber)
}

// SupportsProvenance reports that standard ERC-1155 provenance indexing is supported.
func (a *ERC1155StandardAdapter) SupportsProvenance() bool {
	return true
}
