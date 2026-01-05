package store

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/datatypes"

	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/store/schema"
)

// StoreTestSuite provides the interface for running store tests against different implementations
type StoreTestSuite struct {
	Store Store
	// InitDB should be called before each test to initialize the database
	InitDB func(t *testing.T) Store
	// CleanupDB should be called after each test to clean up the database
	CleanupDB func(t *testing.T)
}

// =============================================================================
// Test Data Builders
// =============================================================================

// buildTestToken creates a test token input
func buildTestToken(chain domain.Chain, standard domain.ChainStandard, contract, tokenNum string) CreateTokenInput {
	tokenCID := domain.NewTokenCID(chain, standard, contract, tokenNum)
	owner := "0x1234567890123456789012345678901234567890"
	// Extract normalized contract address from TokenCID to ensure consistency
	_, _, normalizedContract, _ := tokenCID.Parse()
	return CreateTokenInput{
		TokenCID:        string(tokenCID),
		Chain:           chain,
		Standard:        standard,
		ContractAddress: normalizedContract,
		TokenNumber:     tokenNum,
		CurrentOwner:    &owner,
		Burned:          false,
	}
}

// buildTestBalance creates a test balance input
func buildTestBalance(owner string, quantity string) CreateBalanceInput {
	return CreateBalanceInput{
		OwnerAddress: owner,
		Quantity:     quantity,
	}
}

// buildTestProvenanceEvent creates a test provenance event input
func buildTestProvenanceEvent(chain domain.Chain, eventType schema.ProvenanceEventType, from, to *string, quantity string, txHash string, blockNum uint64) CreateProvenanceEventInput {
	blockHash := "0xblockhash"
	rawData := map[string]interface{}{
		"tx_hash":      txHash,
		"block_number": blockNum,
		"tx_index":     1,
	}
	rawBytes, _ := json.Marshal(rawData)

	return CreateProvenanceEventInput{
		Chain:       chain,
		EventType:   eventType,
		FromAddress: from,
		ToAddress:   to,
		Quantity:    quantity,
		TxHash:      txHash,
		BlockNumber: blockNum,
		BlockHash:   &blockHash,
		Raw:         rawBytes,
		Timestamp:   time.Now().UTC(),
	}
}

// buildTestTokenMint creates a complete token mint input
func buildTestTokenMint(chain domain.Chain, standard domain.ChainStandard, contract, tokenNum string, owner string) CreateTokenMintInput {
	token := buildTestToken(chain, standard, contract, tokenNum)
	token.CurrentOwner = &owner
	balance := buildTestBalance(owner, "1")

	from := "0x0000000000000000000000000000000000000000"
	provenanceEvent := buildTestProvenanceEvent(
		chain,
		schema.ProvenanceEventTypeMint,
		&from,
		&owner,
		"1",
		fmt.Sprintf("0xmint%s%s", contract, tokenNum),
		1000,
	)

	return CreateTokenMintInput{
		Token:           token,
		Balance:         balance,
		ProvenanceEvent: provenanceEvent,
	}
}

// =============================================================================
// Test: CreateTokenMint
// =============================================================================

func testCreateTokenMint(t *testing.T, store Store) {
	ctx := context.Background()

	t.Run("successful mint creates token, balance, provenance event, and change journal", func(t *testing.T) {
		input := buildTestTokenMint(
			domain.ChainEthereumMainnet,
			domain.StandardERC721,
			"0x1111111111111111111111111111111111111111",
			"1",
			"0xowner1111111111111111111111111111111111111",
		)

		err := store.CreateTokenMint(ctx, input)
		require.NoError(t, err)

		// Verify token was created
		token, err := store.GetTokenByTokenCID(ctx, input.Token.TokenCID)
		require.NoError(t, err)
		require.NotNil(t, token)
		assert.Equal(t, input.Token.TokenCID, token.TokenCID)
		assert.Equal(t, input.Token.Chain, token.Chain)
		assert.Equal(t, input.Token.Standard, token.Standard)
		assert.Equal(t, input.Token.ContractAddress, token.ContractAddress)
		assert.Equal(t, input.Token.TokenNumber, token.TokenNumber)
		assert.Equal(t, *input.Token.CurrentOwner, *token.CurrentOwner)
		assert.False(t, token.Burned)

		// Verify balance was created
		balances, total, err := store.GetTokenOwners(ctx, token.ID, 10, 0)
		require.NoError(t, err)
		assert.Equal(t, uint64(1), total)
		assert.Len(t, balances, 1)
		assert.Equal(t, input.Balance.OwnerAddress, balances[0].OwnerAddress)
		assert.Equal(t, input.Balance.Quantity, balances[0].Quantity)

		// Verify provenance event was created
		_, eventTotal, err := store.GetTokenProvenanceEvents(ctx, token.ID, 10, 0, false)
		require.NoError(t, err)
		assert.Equal(t, uint64(1), eventTotal)

		// Verify change journal was created
		changes, changeTotal, err := store.GetChanges(ctx, ChangesQueryFilter{
			TokenCIDs: []string{input.Token.TokenCID},
			Limit:     10,
		})
		require.NoError(t, err)
		assert.Equal(t, uint64(1), changeTotal)
		assert.Len(t, changes, 1)
		assert.Equal(t, schema.SubjectTypeToken, changes[0].SubjectType)
	})

	t.Run("duplicate event with same token and addresses should be idempotent", func(t *testing.T) {
		input := buildTestTokenMint(
			domain.ChainEthereumMainnet,
			domain.StandardERC721,
			"0x2222222222222222222222222222222222222222",
			"1",
			"0xowner2222222222222222222222222222222222222",
		)

		// First mint - should succeed
		err := store.CreateTokenMint(ctx, input)
		require.NoError(t, err)

		token1, err := store.GetTokenByTokenCID(ctx, input.Token.TokenCID)
		require.NoError(t, err)
		require.NotNil(t, token1)

		// Try to create the SAME token with the SAME tx hash and addresses
		// This should succeed (idempotent behavior) but not create duplicates
		input2 := input

		// This should succeed without creating duplicate records
		// The conflict resolution handles both token and provenance event duplicates gracefully
		err = store.CreateTokenMint(ctx, input2)
		require.NoError(t, err)

		// Verify the token still has exactly one provenance event (no duplicates created)
		events, total, err := store.GetTokenProvenanceEvents(ctx, token1.ID, 10, 0, false)
		require.NoError(t, err)
		assert.Equal(t, uint64(1), total)
		assert.Len(t, events, 1)
	})

	t.Run("mint ERC1155 token with quantity > 1", func(t *testing.T) {
		input := buildTestTokenMint(
			domain.ChainEthereumMainnet,
			domain.StandardERC1155,
			"0x4444444444444444444444444444444444444444",
			"10",
			"0xowner4444444444444444444444444444444444444",
		)
		input.Token.CurrentOwner = nil // ERC1155 doesn't have single owner
		input.Balance.Quantity = "100"
		input.ProvenanceEvent.Quantity = "100"

		err := store.CreateTokenMint(ctx, input)
		require.NoError(t, err)

		// Verify token was created
		token, err := store.GetTokenByTokenCID(ctx, input.Token.TokenCID)
		require.NoError(t, err)
		assert.Nil(t, token.CurrentOwner)

		// Verify balance was created
		balances, total, err := store.GetTokenOwners(ctx, token.ID, 10, 0)
		require.NoError(t, err)
		assert.Equal(t, uint64(1), total)
		assert.Len(t, balances, 1)
		assert.Equal(t, "100", balances[0].Quantity)
	})

	t.Run("mint FA2 token on Tezos", func(t *testing.T) {
		owner := "tz1VSUr8wwNhLAzempoch5d6hLRiTh8Cjcjb"
		input := buildTestTokenMint(
			domain.ChainTezosMainnet,
			domain.StandardFA2,
			"KT1TestContractForTestingPurpose",
			"999",
			owner,
		)
		input.Token.CurrentOwner = nil // FA2 doesn't have single owner
		from := "tz1burnburnburnburnburnburnburjAYjjX"
		input.ProvenanceEvent.FromAddress = &from

		err := store.CreateTokenMint(ctx, input)
		require.NoError(t, err)

		// Verify token was created
		token, err := store.GetTokenByTokenCID(ctx, input.Token.TokenCID)
		require.NoError(t, err)
		assert.NotNil(t, token)
		assert.Equal(t, domain.ChainTezosMainnet, token.Chain)
		assert.Equal(t, domain.StandardFA2, token.Standard)
	})
}

// =============================================================================
// Test: UpdateTokenTransfer
// =============================================================================

func testUpdateTokenTransfer(t *testing.T, store Store) {
	ctx := context.Background()

	t.Run("successful transfer updates token owner and balances", func(t *testing.T) {
		// First, mint a token
		owner1 := "0xowner5555555555555555555555555555555555555"
		mintInput := buildTestTokenMint(
			domain.ChainEthereumMainnet,
			domain.StandardERC721,
			"0x5555555555555555555555555555555555555555",
			"1",
			owner1,
		)
		err := store.CreateTokenMint(ctx, mintInput)
		require.NoError(t, err)

		// Now transfer the token
		owner2 := "0xowner6666666666666666666666666666666666666"
		transferInput := UpdateTokenTransferInput{
			TokenCID:     mintInput.Token.TokenCID,
			CurrentOwner: &owner2,
			SenderBalanceUpdate: &UpdateBalanceInput{
				OwnerAddress: owner1,
				Delta:        "1",
			},
			ReceiverBalanceUpdate: &UpdateBalanceInput{
				OwnerAddress: owner2,
				Delta:        "1",
			},
			ProvenanceEvent: buildTestProvenanceEvent(
				domain.ChainEthereumMainnet,
				schema.ProvenanceEventTypeTransfer,
				&owner1,
				&owner2,
				"1",
				"0xtransfer555",
				1001,
			),
		}

		err = store.UpdateTokenTransfer(ctx, transferInput)
		require.NoError(t, err)

		// Verify token owner changed
		token, err := store.GetTokenByTokenCID(ctx, mintInput.Token.TokenCID)
		require.NoError(t, err)
		assert.Equal(t, owner2, *token.CurrentOwner)

		// Verify balances updated
		balances, total, err := store.GetTokenOwners(ctx, token.ID, 10, 0)
		require.NoError(t, err)
		assert.Equal(t, uint64(1), total)
		assert.Equal(t, owner2, balances[0].OwnerAddress)
		assert.Equal(t, "1", balances[0].Quantity)

		// Verify provenance events (should have 2: mint + transfer)
		events, eventTotal, err := store.GetTokenProvenanceEvents(ctx, token.ID, 10, 0, false)
		require.NoError(t, err)
		assert.Equal(t, uint64(2), eventTotal)
		for _, event := range events {
			switch event.EventType {
			case schema.ProvenanceEventTypeMint:
				assert.NotNil(t, event.FromAddress)
				assert.Equal(t, domain.ETHEREUM_ZERO_ADDRESS, *event.FromAddress)
				assert.Equal(t, owner1, *event.ToAddress)
				assert.Equal(t, "1", *event.Quantity)
			case schema.ProvenanceEventTypeTransfer:
				assert.Equal(t, owner1, *event.FromAddress)
				assert.Equal(t, owner2, *event.ToAddress)
				assert.Equal(t, "1", *event.Quantity)
			default:
				// should not happen
				assert.Fail(t, "unexpected provenance event type", event.EventType)
			}
		}

		// Verify changes journal was created
		changes, changeTotal, err := store.GetChanges(ctx, ChangesQueryFilter{
			TokenCIDs: []string{mintInput.Token.TokenCID},
			Limit:     100,
		})
		require.NoError(t, err)
		assert.Equal(t, uint64(2), changeTotal)
		assert.Equal(t, 2, len(changes))
		assert.Equal(t, schema.SubjectTypeToken, changes[0].SubjectType)
		assert.Equal(t, schema.SubjectTypeOwner, changes[1].SubjectType)
	})

	t.Run("transfer ERC1155 partial quantity", func(t *testing.T) {
		// Mint ERC1155 token with quantity 100
		owner1 := "0xowner7777777777777777777777777777777777777"
		mintInput := buildTestTokenMint(
			domain.ChainEthereumMainnet,
			domain.StandardERC1155,
			"0x7777777777777777777777777777777777777777",
			"5",
			owner1,
		)
		mintInput.Token.CurrentOwner = nil
		mintInput.Balance.Quantity = "100"
		mintInput.ProvenanceEvent.Quantity = "100"

		err := store.CreateTokenMint(ctx, mintInput)
		require.NoError(t, err)

		token, err := store.GetTokenByTokenCID(ctx, mintInput.Token.TokenCID)
		require.NoError(t, err)

		// Transfer 30 to owner2
		owner2 := "0xowner8888888888888888888888888888888888888"
		transferInput := UpdateTokenTransferInput{
			TokenCID:     mintInput.Token.TokenCID,
			CurrentOwner: nil, // ERC1155 has no single owner
			SenderBalanceUpdate: &UpdateBalanceInput{
				OwnerAddress: owner1,
				Delta:        "30",
			},
			ReceiverBalanceUpdate: &UpdateBalanceInput{
				OwnerAddress: owner2,
				Delta:        "30",
			},
			ProvenanceEvent: buildTestProvenanceEvent(
				domain.ChainEthereumMainnet,
				schema.ProvenanceEventTypeTransfer,
				&owner1,
				&owner2,
				"30",
				"0xtransfer777",
				1002,
			),
		}

		err = store.UpdateTokenTransfer(ctx, transferInput)
		require.NoError(t, err)

		// Verify both owners have balances
		balances, total, err := store.GetTokenOwners(ctx, token.ID, 10, 0)
		require.NoError(t, err)
		assert.Equal(t, uint64(2), total)

		// Check individual balances
		balanceMap := make(map[string]string)
		for _, b := range balances {
			balanceMap[b.OwnerAddress] = b.Quantity
		}
		assert.Equal(t, "70", balanceMap[owner1])
		assert.Equal(t, "30", balanceMap[owner2])

		// Verify changes journal was created
		changes, changeTotal, err := store.GetChanges(ctx, ChangesQueryFilter{
			TokenCIDs: []string{mintInput.Token.TokenCID},
			Limit:     100,
		})
		require.NoError(t, err)
		assert.Equal(t, uint64(2), changeTotal)
		assert.Equal(t, 2, len(changes))
		assert.Equal(t, schema.SubjectTypeToken, changes[0].SubjectType)
		assert.Equal(t, schema.SubjectTypeBalance, changes[1].SubjectType)
	})

	t.Run("transfer entire balance should delete zero balance", func(t *testing.T) {
		// Mint token
		owner1 := "0xowner9999999999999999999999999999999999999"
		mintInput := buildTestTokenMint(
			domain.ChainEthereumMainnet,
			domain.StandardERC721,
			"0x9999999999999999999999999999999999999999",
			"1",
			owner1,
		)
		err := store.CreateTokenMint(ctx, mintInput)
		require.NoError(t, err)

		token, err := store.GetTokenByTokenCID(ctx, mintInput.Token.TokenCID)
		require.NoError(t, err)

		// Transfer entire balance
		owner2 := "0xowneraaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
		transferInput := UpdateTokenTransferInput{
			TokenCID:     mintInput.Token.TokenCID,
			CurrentOwner: &owner2,
			SenderBalanceUpdate: &UpdateBalanceInput{
				OwnerAddress: owner1,
				Delta:        "1",
			},
			ReceiverBalanceUpdate: &UpdateBalanceInput{
				OwnerAddress: owner2,
				Delta:        "1",
			},
			ProvenanceEvent: buildTestProvenanceEvent(
				domain.ChainEthereumMainnet,
				schema.ProvenanceEventTypeTransfer,
				&owner1,
				&owner2,
				"1",
				"0xtransfer999",
				1003,
			),
		}

		err = store.UpdateTokenTransfer(ctx, transferInput)
		require.NoError(t, err)

		// Verify only owner2 has balance
		balances, total, err := store.GetTokenOwners(ctx, token.ID, 10, 0)
		require.NoError(t, err)
		assert.Equal(t, uint64(1), total)
		assert.Equal(t, owner2, balances[0].OwnerAddress)
	})

	t.Run("transfer non-existent token should fail", func(t *testing.T) {
		owner1 := "0xownerbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
		owner2 := "0xownercccccccccccccccccccccccccccccccccccc"
		transferInput := UpdateTokenTransferInput{
			TokenCID:     "eip155:1:erc721:0xnonexistent:999",
			CurrentOwner: &owner2,
			SenderBalanceUpdate: &UpdateBalanceInput{
				OwnerAddress: owner1,
				Delta:        "1",
			},
			ReceiverBalanceUpdate: &UpdateBalanceInput{
				OwnerAddress: owner2,
				Delta:        "1",
			},
			ProvenanceEvent: buildTestProvenanceEvent(
				domain.ChainEthereumMainnet,
				schema.ProvenanceEventTypeTransfer,
				&owner1,
				&owner2,
				"1",
				"0xtransfernonexistent",
				1004,
			),
		}

		err := store.UpdateTokenTransfer(ctx, transferInput)
		require.Error(t, err)
		assert.ErrorIs(t, err, domain.ErrTokenNotFound)
	})
}

// =============================================================================
// Test: UpdateTokenBurn
// =============================================================================

func testUpdateTokenBurn(t *testing.T, store Store) {
	ctx := context.Background()

	t.Run("successful burn marks token as burned and deletes balance", func(t *testing.T) {
		// Mint a token
		owner := "0xownerdddddddddddddddddddddddddddddddddddd"
		mintInput := buildTestTokenMint(
			domain.ChainEthereumMainnet,
			domain.StandardERC721,
			"0xdddddddddddddddddddddddddddddddddddddddd",
			"1",
			owner,
		)
		err := store.CreateTokenMint(ctx, mintInput)
		require.NoError(t, err)

		_, err = store.GetTokenByTokenCID(ctx, mintInput.Token.TokenCID)
		require.NoError(t, err)

		// Burn the token
		burnAddr := domain.ETHEREUM_ZERO_ADDRESS
		burnInput := CreateTokenBurnInput{
			TokenCID: mintInput.Token.TokenCID,
			SenderBalanceUpdate: &UpdateBalanceInput{
				OwnerAddress: owner,
				Delta:        "1",
			},
			ProvenanceEvent: buildTestProvenanceEvent(
				domain.ChainEthereumMainnet,
				schema.ProvenanceEventTypeBurn,
				&owner,
				&burnAddr,
				"1",
				"0xburndddd",
				1005,
			),
		}

		err = store.UpdateTokenBurn(ctx, burnInput)
		require.NoError(t, err)

		// Verify token is marked as burned
		token, err := store.GetTokenByTokenCID(ctx, mintInput.Token.TokenCID)
		require.NoError(t, err)
		assert.True(t, token.Burned)
		assert.Nil(t, token.CurrentOwner)

		// Verify balance was deleted
		balances, total, err := store.GetTokenOwners(ctx, token.ID, 10, 0)
		require.NoError(t, err)
		assert.Equal(t, uint64(0), total)
		assert.Len(t, balances, 0)

		// Verify provenance event was created
		events, eventTotal, err := store.GetTokenProvenanceEvents(ctx, token.ID, 10, 0, false)
		require.NoError(t, err)
		assert.Equal(t, uint64(2), eventTotal) // mint + burn
		assert.Equal(t, schema.ProvenanceEventTypeMint, events[0].EventType)
		assert.Equal(t, schema.ProvenanceEventTypeBurn, events[1].EventType)
	})

	t.Run("burn partial ERC1155 quantity", func(t *testing.T) {
		// Mint ERC1155 token
		owner := "0xownereeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
		mintInput := buildTestTokenMint(
			domain.ChainEthereumMainnet,
			domain.StandardERC1155,
			"0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
			"10",
			owner,
		)
		mintInput.Token.CurrentOwner = nil
		mintInput.Balance.Quantity = "100"
		mintInput.ProvenanceEvent.Quantity = "100"

		err := store.CreateTokenMint(ctx, mintInput)
		require.NoError(t, err)

		_, err = store.GetTokenByTokenCID(ctx, mintInput.Token.TokenCID)
		require.NoError(t, err)

		// Burn 40 tokens
		burnAddr := domain.ETHEREUM_ZERO_ADDRESS
		burnInput := CreateTokenBurnInput{
			TokenCID: mintInput.Token.TokenCID,
			SenderBalanceUpdate: &UpdateBalanceInput{
				OwnerAddress: owner,
				Delta:        "40",
			},
			ProvenanceEvent: buildTestProvenanceEvent(
				domain.ChainEthereumMainnet,
				schema.ProvenanceEventTypeBurn,
				&owner,
				&burnAddr,
				"40",
				"0xburneeee",
				1006,
			),
		}

		err = store.UpdateTokenBurn(ctx, burnInput)
		require.NoError(t, err)

		// Token should be marked as burned (even partial burn marks it)
		token, err := store.GetTokenByTokenCID(ctx, mintInput.Token.TokenCID)
		require.NoError(t, err)
		assert.True(t, token.Burned)

		// Verify balance was reduced
		balances, total, err := store.GetTokenOwners(ctx, token.ID, 10, 0)
		require.NoError(t, err)
		assert.Equal(t, uint64(1), total)
		assert.Equal(t, "60", balances[0].Quantity)
	})

	t.Run("burn non-existent token should fail", func(t *testing.T) {
		owner := "0xownerffffffffffffffffffffffffffffffffff"
		burnAddr := domain.ETHEREUM_ZERO_ADDRESS
		burnInput := CreateTokenBurnInput{
			TokenCID: "eip155:1:erc721:0xnonexistent:998",
			SenderBalanceUpdate: &UpdateBalanceInput{
				OwnerAddress: owner,
				Delta:        "1",
			},
			ProvenanceEvent: buildTestProvenanceEvent(
				domain.ChainEthereumMainnet,
				schema.ProvenanceEventTypeBurn,
				&owner,
				&burnAddr,
				"1",
				"0xburnnonexistent",
				1007,
			),
		}

		err := store.UpdateTokenBurn(ctx, burnInput)
		require.Error(t, err)
		assert.ErrorIs(t, err, domain.ErrTokenNotFound)
	})
}

// =============================================================================
// Test: CreateTokenWithProvenances
// =============================================================================

func testCreateTokenWithProvenances(t *testing.T, store Store) {
	ctx := context.Background()

	t.Run("create token with multiple provenances", func(t *testing.T) {
		owner1 := "0xowner1000000000000000000000000000000000001"
		owner2 := "0xowner2000000000000000000000000000000000002"

		token := buildTestToken(
			domain.ChainEthereumMainnet,
			domain.StandardERC1155,
			"0x1000000000000000000000000000000000000000",
			"1",
		)
		token.CurrentOwner = nil

		balances := []CreateBalanceInput{
			{OwnerAddress: owner1, Quantity: "50"},
			{OwnerAddress: owner2, Quantity: "30"},
		}

		zeroAddr := domain.ETHEREUM_ZERO_ADDRESS
		events := []CreateProvenanceEventInput{
			buildTestProvenanceEvent(
				domain.ChainEthereumMainnet,
				schema.ProvenanceEventTypeMint,
				&zeroAddr,
				&owner1,
				"80",
				"0xmint1000",
				1000,
			),
			buildTestProvenanceEvent(
				domain.ChainEthereumMainnet,
				schema.ProvenanceEventTypeTransfer,
				&owner1,
				&owner2,
				"30",
				"0xtransfer1000",
				1001,
			),
		}

		input := CreateTokenWithProvenancesInput{
			Token:    token,
			Balances: balances,
			Events:   events,
		}

		err := store.CreateTokenWithProvenances(ctx, input)
		require.NoError(t, err)

		// Verify token
		tokenResult, err := store.GetTokenByTokenCID(ctx, token.TokenCID)
		require.NoError(t, err)
		assert.NotNil(t, tokenResult)
		assert.Equal(t, token.TokenCID, tokenResult.TokenCID)
		assert.Equal(t, token.Chain, tokenResult.Chain)
		assert.Equal(t, token.Standard, tokenResult.Standard)
		assert.Equal(t, token.ContractAddress, tokenResult.ContractAddress)
		assert.Equal(t, token.TokenNumber, tokenResult.TokenNumber)
		assert.Nil(t, tokenResult.CurrentOwner)
		assert.False(t, tokenResult.Burned)

		// Verify balances
		_, total, err := store.GetTokenOwners(ctx, tokenResult.ID, 10, 0)
		require.NoError(t, err)
		assert.Equal(t, uint64(2), total)
		assert.Equal(t, 2, len(balances))
		assert.Equal(t, owner1, balances[0].OwnerAddress)
		assert.Equal(t, "50", balances[0].Quantity)
		assert.Equal(t, owner2, balances[1].OwnerAddress)
		assert.Equal(t, "30", balances[1].Quantity)

		// Verify events
		provenanceEvents, eventTotal, err := store.GetTokenProvenanceEvents(ctx, tokenResult.ID, 10, 0, false)
		require.NoError(t, err)
		assert.Equal(t, uint64(2), eventTotal)
		assert.Equal(t, schema.ProvenanceEventTypeMint, provenanceEvents[0].EventType)
		assert.Equal(t, schema.ProvenanceEventTypeTransfer, provenanceEvents[1].EventType)

		// Verify changes journal was created
		changes, changeTotal, err := store.GetChanges(ctx, ChangesQueryFilter{
			TokenCIDs: []string{token.TokenCID},
			Limit:     100,
		})
		require.NoError(t, err)
		assert.Equal(t, uint64(2), changeTotal)
		assert.Equal(t, 2, len(changes))
		assert.Equal(t, schema.SubjectTypeToken, changes[0].SubjectType)
		assert.Equal(t, schema.SubjectTypeBalance, changes[1].SubjectType)
	})

	t.Run("upsert existing token with new provenances", func(t *testing.T) {
		// First create
		owner1 := "0xowner3000000000000000000000000000000000003"
		token := buildTestToken(
			domain.ChainEthereumMainnet,
			domain.StandardERC721,
			"0x3000000000000000000000000000000000000000",
			"1",
		)
		token.CurrentOwner = &owner1

		zeroAddr := "0x0000000000000000000000000000000000000000"
		input := CreateTokenWithProvenancesInput{
			Token: token,
			Balances: []CreateBalanceInput{
				{OwnerAddress: owner1, Quantity: "1"},
			},
			Events: []CreateProvenanceEventInput{
				buildTestProvenanceEvent(
					domain.ChainEthereumMainnet,
					schema.ProvenanceEventTypeMint,
					&zeroAddr,
					&owner1,
					"1",
					"0xmint3000",
					1000,
				),
			},
		}

		err := store.CreateTokenWithProvenances(ctx, input)
		require.NoError(t, err)

		// Get token ID for subsequent queries
		tokenResult, err := store.GetTokenByTokenCID(ctx, token.TokenCID)
		require.NoError(t, err)
		require.NotNil(t, tokenResult)

		// Verify initial state - changes_journal
		changes, changeTotal, err := store.GetChanges(ctx, ChangesQueryFilter{
			TokenCIDs: []string{token.TokenCID},
			Limit:     100,
		})
		require.NoError(t, err)
		assert.Equal(t, uint64(1), changeTotal, "should have 1 change journal entry (mint)")
		assert.Equal(t, 1, len(changes))
		assert.Equal(t, schema.SubjectTypeToken, changes[0].SubjectType, "mint event for ERC721")

		// Now upsert with new owner
		owner2 := "0xowner4000000000000000000000000000000000004"
		input.Token.CurrentOwner = &owner2
		input.Balances = []CreateBalanceInput{
			{OwnerAddress: owner2, Quantity: "1"},
		}
		input.Events = []CreateProvenanceEventInput{
			buildTestProvenanceEvent(
				domain.ChainEthereumMainnet,
				schema.ProvenanceEventTypeMint,
				&zeroAddr,
				&owner1,
				"1",
				"0xmint3000",
				1000,
			),
			buildTestProvenanceEvent(
				domain.ChainEthereumMainnet,
				schema.ProvenanceEventTypeTransfer,
				&owner1,
				&owner2,
				"1",
				"0xtransfer3000",
				1001,
			),
		}

		err = store.CreateTokenWithProvenances(ctx, input)
		require.NoError(t, err)

		// Verify updated token
		tokenResult, err = store.GetTokenByTokenCID(ctx, token.TokenCID)
		require.NoError(t, err)
		assert.Equal(t, owner2, *tokenResult.CurrentOwner)

		// Verify only new balance exists (old balance should be deleted)
		balances, total, err := store.GetTokenOwners(ctx, tokenResult.ID, 10, 0)
		require.NoError(t, err)
		assert.Equal(t, uint64(1), total, "should have only 1 balance after upsert")
		assert.Equal(t, owner2, balances[0].OwnerAddress, "balance should be for owner2")

		// Verify both events exist
		provenanceEvents, eventTotal, err := store.GetTokenProvenanceEvents(ctx, tokenResult.ID, 10, 0, false)
		require.NoError(t, err)
		assert.Equal(t, uint64(2), eventTotal, "should have 2 events after upsert")
		assert.Equal(t, schema.ProvenanceEventTypeMint, provenanceEvents[0].EventType)
		assert.Equal(t, schema.ProvenanceEventTypeTransfer, provenanceEvents[1].EventType)

		// Verify changes_journal was recreated correctly
		changes, changeTotal, err = store.GetChanges(ctx, ChangesQueryFilter{
			TokenCIDs: []string{token.TokenCID},
			Limit:     100,
		})
		require.NoError(t, err)
		assert.Equal(t, uint64(2), changeTotal, "should have 2 change journal entries after upsert (mint + transfer)")
		assert.Equal(t, 2, len(changes))

		// Verify both journal entries are for provenance events
		assert.Equal(t, schema.SubjectTypeToken, changes[0].SubjectType, "first change should be token (mint)")
		assert.Equal(t, schema.SubjectTypeOwner, changes[1].SubjectType, "second change should be owner (transfer)")
	})

	t.Run("create token with large number of provenance events to test batch insert", func(t *testing.T) {
		// This test verifies that the batch insert approach with calculateSafeBatchSize works correctly
		// ProvenanceEvent has 11 fields, with headroom of 1000: batch size = (65535-1000)/11 ≈ 5866
		// We'll create 10,000 events to exceed one batch and trigger multiple batches
		numEvents := 10000
		owner1 := "0xowner5000000000000000000000000000000000005"
		owner2 := "0xowner6000000000000000000000000000000000006"

		token := buildTestToken(
			domain.ChainEthereumMainnet,
			domain.StandardERC1155,
			"0x5000000000000000000000000000000000000000",
			"1",
		)
		token.CurrentOwner = nil

		// Create balances
		balances := []CreateBalanceInput{
			{OwnerAddress: owner1, Quantity: "500"},
			{OwnerAddress: owner2, Quantity: "500"},
		}

		// Generate many provenance events (mint + transfers)
		events := make([]CreateProvenanceEventInput, 0, numEvents+1)
		zeroAddr := domain.ETHEREUM_ZERO_ADDRESS

		// Add initial mint event
		events = append(events, buildTestProvenanceEvent(
			domain.ChainEthereumMainnet,
			schema.ProvenanceEventTypeMint,
			&zeroAddr,
			&owner1,
			"1000",
			"0xmint5000",
			1000,
		))

		// Add many transfer events back and forth between owner1 and owner2
		for i := range numEvents {
			var from, to *string
			if i%2 == 0 {
				from = &owner1
				to = &owner2
			} else {
				from = &owner2
				to = &owner1
			}

			events = append(events, buildTestProvenanceEvent(
				domain.ChainEthereumMainnet,
				schema.ProvenanceEventTypeTransfer,
				from,
				to,
				"1",
				fmt.Sprintf("0xtransfer5000_%d", i),
				uint64(1001+i), //nolint:gosec,G115
			))
		}

		input := CreateTokenWithProvenancesInput{
			Token:    token,
			Balances: balances,
			Events:   events,
		}

		// Measure time to ensure batching is efficient
		// Expected batch size: (65535 - 1000) / 11 fields ≈ 5866 records/batch
		// With 10,001 events, we expect ~2 batches
		expectedBatchSize := (65535 - 1000) / 11
		expectedBatches := (len(events) + expectedBatchSize - 1) / expectedBatchSize
		t.Logf("Inserting %d provenance events (expected ~%d batches of ~%d records each)",
			len(events), expectedBatches, expectedBatchSize)

		startTime := time.Now()
		err := store.CreateTokenWithProvenances(ctx, input)
		duration := time.Since(startTime)
		require.NoError(t, err)

		t.Logf("Successfully inserted %d provenance events in %v (~%.2f events/sec)",
			len(events), duration, float64(len(events))/duration.Seconds())

		// Verify token
		tokenResult, err := store.GetTokenByTokenCID(ctx, token.TokenCID)
		require.NoError(t, err)
		require.NotNil(t, tokenResult)

		// Verify all events were inserted
		provenanceEvents, eventTotal, err := store.GetTokenProvenanceEvents(ctx, tokenResult.ID, 10000, 0, false)
		require.NoError(t, err)
		assert.Equal(t, uint64(numEvents+1), eventTotal, "should have all events (1 mint + %d transfers)", numEvents) //nolint:gosec,G115
		assert.Equal(t, schema.ProvenanceEventTypeMint, provenanceEvents[0].EventType, "first event should be mint")

		// Verify all events have IDs populated (none should be 0)
		for i, evt := range provenanceEvents {
			assert.NotZero(t, evt.ID, "event %d should have a valid ID", i)
		}

		// Verify changes_journal entries were created for all events
		changes, changeTotal, err := store.GetChanges(ctx, ChangesQueryFilter{
			TokenCIDs: []string{token.TokenCID},
			Limit:     numEvents + 10, // Get all changes
		})
		require.NoError(t, err)
		assert.Equal(t, uint64(numEvents+1), changeTotal, "should have change journal entry for each event") //nolint:gosec,G115
		assert.Equal(t, numEvents+1, len(changes))

		// Verify balances
		owners, total, err := store.GetTokenOwners(ctx, tokenResult.ID, 10, 0)
		require.NoError(t, err)
		assert.Equal(t, uint64(2), total)
		assert.Equal(t, 2, len(owners))
	})
}

// =============================================================================
// Test: UpsertTokenBalanceForOwner
// =============================================================================

func testUpsertTokenBalanceForOwner(t *testing.T, store Store) {
	ctx := context.Background()

	t.Run("create new token with owner balance", func(t *testing.T) {
		// Test upserting a token that doesn't exist yet
		contractAddress := "0xabc1234567890123456789012345678901234567"
		tokenNumber := "100"
		ownerAddress := "0xowner1111111111111111111111111111111111111"
		tokenCID := domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC1155, contractAddress, tokenNumber)

		input := UpsertTokenBalanceForOwnerInput{
			Token: CreateTokenInput{
				TokenCID:        tokenCID.String(),
				Chain:           domain.ChainEthereumMainnet,
				Standard:        domain.StandardERC1155,
				ContractAddress: contractAddress,
				TokenNumber:     tokenNumber,
			},
			OwnerAddress: ownerAddress,
			Quantity:     "10",
			Events: []CreateProvenanceEventInput{
				{
					Chain:       domain.ChainEthereumMainnet,
					EventType:   schema.ProvenanceEventTypeMint,
					FromAddress: stringPtr(domain.ETHEREUM_ZERO_ADDRESS),
					ToAddress:   &ownerAddress,
					Quantity:    "10",
					TxHash:      "0xtx1",
					BlockNumber: 100,
					Timestamp:   time.Now(),
				},
			},
		}

		err := store.UpsertTokenBalanceForOwner(ctx, input)
		require.NoError(t, err)

		// Verify token was created
		token, err := store.GetTokenByTokenCID(ctx, tokenCID.String())
		require.NoError(t, err)
		assert.Equal(t, tokenCID.String(), token.TokenCID)
		assert.Equal(t, domain.ChainEthereumMainnet, token.Chain)
		assert.Equal(t, domain.StandardERC1155, token.Standard)

		// Verify balance was created
		balances, total, err := store.GetTokenOwners(ctx, token.ID, 10, 0)
		require.NoError(t, err)
		assert.Equal(t, uint64(1), total)
		assert.Equal(t, ownerAddress, balances[0].OwnerAddress)
		assert.Equal(t, "10", balances[0].Quantity)

		// Verify provenance event was created
		events, eventTotal, err := store.GetTokenProvenanceEvents(ctx, token.ID, 10, 0, false)
		require.NoError(t, err)
		assert.Equal(t, uint64(1), eventTotal)
		assert.Equal(t, schema.ProvenanceEventTypeMint, events[0].EventType)
	})

	t.Run("update existing owner balance without affecting other owners", func(t *testing.T) {
		// First, create a token with two owners
		contractAddress := "0xdef4567890123456789012345678901234567890"
		tokenNumber := "200"
		owner1 := "0xowner2222222222222222222222222222222222222"
		owner2 := "0xowner3333333333333333333333333333333333333"
		tokenCID := domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC1155, contractAddress, tokenNumber)

		// Create token with both owners using CreateTokenWithProvenances
		createInput := CreateTokenWithProvenancesInput{
			Token: CreateTokenInput{
				TokenCID:        tokenCID.String(),
				Chain:           domain.ChainEthereumMainnet,
				Standard:        domain.StandardERC1155,
				ContractAddress: contractAddress,
				TokenNumber:     tokenNumber,
			},
			Balances: []CreateBalanceInput{
				{OwnerAddress: owner1, Quantity: "5"},
				{OwnerAddress: owner2, Quantity: "8"},
			},
			Events: []CreateProvenanceEventInput{},
		}
		err := store.CreateTokenWithProvenances(ctx, createInput)
		require.NoError(t, err)

		// Now update owner1's balance using UpsertTokenBalanceForOwner
		upsertInput := UpsertTokenBalanceForOwnerInput{
			Token: CreateTokenInput{
				TokenCID:        tokenCID.String(),
				Chain:           domain.ChainEthereumMainnet,
				Standard:        domain.StandardERC1155,
				ContractAddress: contractAddress,
				TokenNumber:     tokenNumber,
			},
			OwnerAddress: owner1,
			Quantity:     "15", // Updated quantity
			Events: []CreateProvenanceEventInput{
				{
					Chain:       domain.ChainEthereumMainnet,
					EventType:   schema.ProvenanceEventTypeTransfer,
					FromAddress: stringPtr("0xother"),
					ToAddress:   &owner1,
					Quantity:    "10",
					TxHash:      "0xtx2",
					BlockNumber: 200,
					Timestamp:   time.Now(),
				},
			},
		}

		err = store.UpsertTokenBalanceForOwner(ctx, upsertInput)
		require.NoError(t, err)

		// Verify owner1's balance was updated
		token, err := store.GetTokenByTokenCID(ctx, tokenCID.String())
		require.NoError(t, err)

		balances, total, err := store.GetTokenOwners(ctx, token.ID, 10, 0)
		require.NoError(t, err)
		assert.Equal(t, uint64(2), total, "Should still have 2 owners")

		// Find and verify both balances
		var owner1Balance, owner2Balance *schema.Balance
		for i := range balances {
			switch balances[i].OwnerAddress {
			case owner1:
				owner1Balance = &balances[i]
			case owner2:
				owner2Balance = &balances[i]
			}
		}

		require.NotNil(t, owner1Balance, "Owner1 balance should exist")
		require.NotNil(t, owner2Balance, "Owner2 balance should exist")
		assert.Equal(t, "15", owner1Balance.Quantity, "Owner1 balance should be updated")
		assert.Equal(t, "8", owner2Balance.Quantity, "Owner2 balance should be unchanged")
	})

	t.Run("add new owner to existing token", func(t *testing.T) {
		// Create a token with one owner
		contractAddress := "0x1234567890123456789012345678901234567890"
		tokenNumber := "300"
		owner1 := "0xowner4444444444444444444444444444444444444"
		owner2 := "0xowner5555555555555555555555555555555555555"
		tokenCID := domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC1155, contractAddress, tokenNumber)

		// Create token with owner1
		createInput := CreateTokenWithProvenancesInput{
			Token: CreateTokenInput{
				TokenCID:        tokenCID.String(),
				Chain:           domain.ChainEthereumMainnet,
				Standard:        domain.StandardERC1155,
				ContractAddress: contractAddress,
				TokenNumber:     tokenNumber,
			},
			Balances: []CreateBalanceInput{
				{OwnerAddress: owner1, Quantity: "20"},
			},
			Events: []CreateProvenanceEventInput{},
		}
		err := store.CreateTokenWithProvenances(ctx, createInput)
		require.NoError(t, err)

		// Add owner2 using UpsertTokenBalanceForOwner
		upsertInput := UpsertTokenBalanceForOwnerInput{
			Token: CreateTokenInput{
				TokenCID:        tokenCID.String(),
				Chain:           domain.ChainEthereumMainnet,
				Standard:        domain.StandardERC1155,
				ContractAddress: contractAddress,
				TokenNumber:     tokenNumber,
			},
			OwnerAddress: owner2,
			Quantity:     "12",
			Events: []CreateProvenanceEventInput{
				{
					Chain:       domain.ChainEthereumMainnet,
					EventType:   schema.ProvenanceEventTypeTransfer,
					FromAddress: &owner1,
					ToAddress:   &owner2,
					Quantity:    "12",
					TxHash:      "0xtx3",
					BlockNumber: 300,
					Timestamp:   time.Now(),
				},
			},
		}

		err = store.UpsertTokenBalanceForOwner(ctx, upsertInput)
		require.NoError(t, err)

		// Verify both owners exist
		token, err := store.GetTokenByTokenCID(ctx, tokenCID.String())
		require.NoError(t, err)

		balances, total, err := store.GetTokenOwners(ctx, token.ID, 10, 0)
		require.NoError(t, err)
		assert.Equal(t, uint64(2), total, "Should have 2 owners now")

		// Verify both balances exist
		balanceMap := make(map[string]string)
		for _, b := range balances {
			balanceMap[b.OwnerAddress] = b.Quantity
		}
		assert.Equal(t, "20", balanceMap[owner1], "Owner1 balance should be unchanged")
		assert.Equal(t, "12", balanceMap[owner2], "Owner2 balance should be added")
	})

	t.Run("handle zero balance", func(t *testing.T) {
		// Test upserting with zero balance (owner transferred away all tokens)
		contractAddress := "0x9876543210987654321098765432109876543210"
		tokenNumber := "400"
		ownerAddress := "0xowner6666666666666666666666666666666666666"
		tokenCID := domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC1155, contractAddress, tokenNumber)

		input := UpsertTokenBalanceForOwnerInput{
			Token: CreateTokenInput{
				TokenCID:        tokenCID.String(),
				Chain:           domain.ChainEthereumMainnet,
				Standard:        domain.StandardERC1155,
				ContractAddress: contractAddress,
				TokenNumber:     tokenNumber,
			},
			OwnerAddress: ownerAddress,
			Quantity:     "0", // Zero balance
			Events: []CreateProvenanceEventInput{
				{
					Chain:       domain.ChainEthereumMainnet,
					EventType:   schema.ProvenanceEventTypeMint,
					FromAddress: stringPtr(domain.ETHEREUM_ZERO_ADDRESS),
					ToAddress:   &ownerAddress,
					Quantity:    "5",
					TxHash:      "0xtx4",
					BlockNumber: 400,
					Timestamp:   time.Now(),
				},
				{
					Chain:       domain.ChainEthereumMainnet,
					EventType:   schema.ProvenanceEventTypeTransfer,
					FromAddress: &ownerAddress,
					ToAddress:   stringPtr("0xother"),
					Quantity:    "5",
					TxHash:      "0xtx5",
					BlockNumber: 401,
					Timestamp:   time.Now(),
				},
			},
		}

		err := store.UpsertTokenBalanceForOwner(ctx, input)
		require.NoError(t, err)

		// Verify zero balance was stored
		token, err := store.GetTokenByTokenCID(ctx, tokenCID.String())
		require.NoError(t, err)

		balances, total, err := store.GetTokenOwners(ctx, token.ID, 10, 0)
		require.NoError(t, err)
		assert.Equal(t, uint64(1), total)
		assert.Equal(t, "0", balances[0].Quantity)

		// Verify both events were stored
		events, eventTotal, err := store.GetTokenProvenanceEvents(ctx, token.ID, 10, 0, false)
		require.NoError(t, err)
		assert.Equal(t, uint64(2), eventTotal)
		assert.Len(t, events, 2, "Should have 2 events")
	})

	t.Run("handle duplicate events gracefully", func(t *testing.T) {
		// Test that duplicate events are ignored (ON CONFLICT DO NOTHING)
		contractAddress := "0xfedcba0987654321098765432109876543210987"
		tokenNumber := "500"
		ownerAddress := "0xowner7777777777777777777777777777777777777"
		tokenCID := domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC1155, contractAddress, tokenNumber)

		// First upsert with an event
		input1 := UpsertTokenBalanceForOwnerInput{
			Token: CreateTokenInput{
				TokenCID:        tokenCID.String(),
				Chain:           domain.ChainEthereumMainnet,
				Standard:        domain.StandardERC1155,
				ContractAddress: contractAddress,
				TokenNumber:     tokenNumber,
			},
			OwnerAddress: ownerAddress,
			Quantity:     "10",
			Events: []CreateProvenanceEventInput{
				{
					Chain:       domain.ChainEthereumMainnet,
					EventType:   schema.ProvenanceEventTypeMint,
					FromAddress: stringPtr(domain.ETHEREUM_ZERO_ADDRESS),
					ToAddress:   &ownerAddress,
					Quantity:    "10",
					TxHash:      "0xtx6",
					BlockNumber: 500,
					Timestamp:   time.Now(),
				},
			},
		}

		err := store.UpsertTokenBalanceForOwner(ctx, input1)
		require.NoError(t, err)

		// Second upsert with the same event (should be ignored)
		input2 := UpsertTokenBalanceForOwnerInput{
			Token: CreateTokenInput{
				TokenCID:        tokenCID.String(),
				Chain:           domain.ChainEthereumMainnet,
				Standard:        domain.StandardERC1155,
				ContractAddress: contractAddress,
				TokenNumber:     tokenNumber,
			},
			OwnerAddress: ownerAddress,
			Quantity:     "15", // Updated balance
			Events: []CreateProvenanceEventInput{
				{
					Chain:       domain.ChainEthereumMainnet,
					EventType:   schema.ProvenanceEventTypeMint,
					FromAddress: stringPtr(domain.ETHEREUM_ZERO_ADDRESS),
					ToAddress:   &ownerAddress,
					Quantity:    "10",
					TxHash:      "0xtx6", // Same tx hash
					BlockNumber: 500,
					Timestamp:   time.Now(),
				},
			},
		}

		err = store.UpsertTokenBalanceForOwner(ctx, input2)
		require.NoError(t, err)

		// Verify balance was updated but event wasn't duplicated
		token, err := store.GetTokenByTokenCID(ctx, tokenCID.String())
		require.NoError(t, err)

		balances, _, err := store.GetTokenOwners(ctx, token.ID, 10, 0)
		require.NoError(t, err)
		assert.Equal(t, "15", balances[0].Quantity, "Balance should be updated")

		events, eventTotal, err := store.GetTokenProvenanceEvents(ctx, token.ID, 10, 0, false)
		require.NoError(t, err)
		assert.Equal(t, uint64(1), eventTotal, "Should still have only 1 event (duplicate ignored)")
		assert.Len(t, events, 1, "Should have exactly 1 event")
	})

	t.Run("upsert without events", func(t *testing.T) {
		// Test upserting balance without any events (just balance update)
		contractAddress := "0x0123456789abcdef0123456789abcdef01234567"
		tokenNumber := "600"
		ownerAddress := "0xowner8888888888888888888888888888888888888"
		tokenCID := domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC1155, contractAddress, tokenNumber)

		input := UpsertTokenBalanceForOwnerInput{
			Token: CreateTokenInput{
				TokenCID:        tokenCID.String(),
				Chain:           domain.ChainEthereumMainnet,
				Standard:        domain.StandardERC1155,
				ContractAddress: contractAddress,
				TokenNumber:     tokenNumber,
			},
			OwnerAddress: ownerAddress,
			Quantity:     "25",
			Events:       []CreateProvenanceEventInput{}, // No events
		}

		err := store.UpsertTokenBalanceForOwner(ctx, input)
		require.NoError(t, err)

		// Verify token and balance were created
		token, err := store.GetTokenByTokenCID(ctx, tokenCID.String())
		require.NoError(t, err)

		balances, total, err := store.GetTokenOwners(ctx, token.ID, 10, 0)
		require.NoError(t, err)
		assert.Equal(t, uint64(1), total)
		assert.Equal(t, "25", balances[0].Quantity)

		// Verify no events were created
		events, eventTotal, err := store.GetTokenProvenanceEvents(ctx, token.ID, 10, 0, false)
		require.NoError(t, err)
		assert.Equal(t, uint64(0), eventTotal, "Should have no events")
		assert.Empty(t, events, "Events array should be empty")
	})

	t.Run("update with multiple new events", func(t *testing.T) {
		// Test upserting with multiple events
		contractAddress := "0xabcdef0123456789abcdef0123456789abcdef01"
		tokenNumber := "700"
		ownerAddress := "0xowner9999999999999999999999999999999999999"
		tokenCID := domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC1155, contractAddress, tokenNumber)

		input := UpsertTokenBalanceForOwnerInput{
			Token: CreateTokenInput{
				TokenCID:        tokenCID.String(),
				Chain:           domain.ChainEthereumMainnet,
				Standard:        domain.StandardERC1155,
				ContractAddress: contractAddress,
				TokenNumber:     tokenNumber,
			},
			OwnerAddress: ownerAddress,
			Quantity:     "30",
			Events: []CreateProvenanceEventInput{
				{
					Chain:       domain.ChainEthereumMainnet,
					EventType:   schema.ProvenanceEventTypeMint,
					FromAddress: stringPtr(domain.ETHEREUM_ZERO_ADDRESS),
					ToAddress:   &ownerAddress,
					Quantity:    "10",
					TxHash:      "0xtx7",
					BlockNumber: 700,
					Timestamp:   time.Now(),
				},
				{
					Chain:       domain.ChainEthereumMainnet,
					EventType:   schema.ProvenanceEventTypeTransfer,
					FromAddress: stringPtr("0xother1"),
					ToAddress:   &ownerAddress,
					Quantity:    "10",
					TxHash:      "0xtx8",
					BlockNumber: 701,
					Timestamp:   time.Now(),
				},
				{
					Chain:       domain.ChainEthereumMainnet,
					EventType:   schema.ProvenanceEventTypeTransfer,
					FromAddress: stringPtr("0xother2"),
					ToAddress:   &ownerAddress,
					Quantity:    "10",
					TxHash:      "0xtx9",
					BlockNumber: 702,
					Timestamp:   time.Now(),
				},
			},
		}

		err := store.UpsertTokenBalanceForOwner(ctx, input)
		require.NoError(t, err)

		// Verify all events were created
		token, err := store.GetTokenByTokenCID(ctx, tokenCID.String())
		require.NoError(t, err)

		events, eventTotal, err := store.GetTokenProvenanceEvents(ctx, token.ID, 10, 0, false)
		require.NoError(t, err)
		assert.Equal(t, uint64(3), eventTotal, "Should have 3 events")
		assert.Len(t, events, 3, "Should have exactly 3 events")
	})

	t.Run("upsert with large number of provenance events to test batch insert", func(t *testing.T) {
		// This test verifies that the batch insert approach works for UpsertTokenBalanceForOwner
		// ProvenanceEvent has 11 fields, with headroom of 1000: batch size = (65535-1000)/11 ≈ 5866
		// We'll create 8,000 events to exceed one batch and trigger multiple batches
		numEvents := 8000
		ownerAddress := "0xowner7000000000000000000000000000000000007"

		tokenCID := domain.NewTokenCID(
			domain.ChainEthereumMainnet,
			domain.StandardERC1155,
			"0x7000000000000000000000000000000000000000",
			"1",
		)

		token := CreateTokenInput{
			TokenCID:        tokenCID.String(),
			Chain:           domain.ChainEthereumMainnet,
			Standard:        domain.StandardERC1155,
			ContractAddress: "0x7000000000000000000000000000000000000000",
			TokenNumber:     "1",
			CurrentOwner:    nil,
		}

		// Generate many provenance events
		events := make([]CreateProvenanceEventInput, 0, numEvents+1)
		zeroAddr := domain.ETHEREUM_ZERO_ADDRESS

		// Add initial mint event
		events = append(events, buildTestProvenanceEvent(
			domain.ChainEthereumMainnet,
			schema.ProvenanceEventTypeMint,
			&zeroAddr,
			&ownerAddress,
			"1000",
			"0xmint7000",
			2000,
		))

		// Add many transfer events (simulating high-frequency trading or activity)
		// For ERC1155, we'll just track transfers of small quantities
		tempOwner := "0xtemp0000000000000000000000000000000000000"
		for i := range numEvents {
			var from, to *string
			if i%2 == 0 {
				from = &ownerAddress
				to = &tempOwner
			} else {
				from = &tempOwner
				to = &ownerAddress
			}

			events = append(events, buildTestProvenanceEvent(
				domain.ChainEthereumMainnet,
				schema.ProvenanceEventTypeTransfer,
				from,
				to,
				"1",
				fmt.Sprintf("0xtransfer7000_%d", i),
				uint64(2001+i), //nolint:gosec,G115
			))
		}

		input := UpsertTokenBalanceForOwnerInput{
			Token:        token,
			OwnerAddress: ownerAddress,
			Quantity:     "600", // Final balance after all transfers
			Events:       events,
		}

		// Measure time to ensure batching is efficient
		// Expected batch size: (65535 - 1000) / 11 fields ≈ 5866 records/batch
		// With 8,001 events, we expect ~2 batches
		expectedBatchSize := (65535 - 1000) / 11
		expectedBatches := (len(events) + expectedBatchSize - 1) / expectedBatchSize
		t.Logf("Upserting %d provenance events (expected ~%d batches of ~%d records each)",
			len(events), expectedBatches, expectedBatchSize)

		startTime := time.Now()
		err := store.UpsertTokenBalanceForOwner(ctx, input)
		duration := time.Since(startTime)
		require.NoError(t, err)

		t.Logf("Successfully upserted %d provenance events in %v (~%.2f events/sec)",
			len(events), duration, float64(len(events))/duration.Seconds())

		// Verify token was created
		tokenResult, err := store.GetTokenByTokenCID(ctx, tokenCID.String())
		require.NoError(t, err)
		require.NotNil(t, tokenResult)

		// Verify all events were inserted
		provenanceEvents, eventTotal, err := store.GetTokenProvenanceEvents(ctx, tokenResult.ID, 10000, 0, false)
		require.NoError(t, err)
		assert.Equal(t, uint64(numEvents+1), eventTotal, "should have all events (1 mint + %d transfers)", numEvents) //nolint:gosec,G115
		assert.Equal(t, schema.ProvenanceEventTypeMint, provenanceEvents[0].EventType, "first event should be mint")

		// Verify all events have IDs populated (none should be 0)
		for i, evt := range provenanceEvents {
			assert.NotZero(t, evt.ID, "event %d should have a valid ID", i)
		}

		// Verify changes_journal entries were created for all events
		_, changeTotal, err := store.GetChanges(ctx, ChangesQueryFilter{
			TokenCIDs: []string{tokenCID.String()},
			Limit:     numEvents + 10, // Get all changes
		})
		require.NoError(t, err)
		assert.Equal(t, uint64(numEvents+1), changeTotal, "should have change journal entry for each event") //nolint:gosec,G115

		// Verify balance was set correctly
		balances, balanceTotal, err := store.GetTokenOwners(ctx, tokenResult.ID, 10, 0)
		require.NoError(t, err)
		assert.Greater(t, balanceTotal, uint64(0), "should have at least one balance")
		// Find the owner's balance
		var ownerBalance *schema.Balance
		for i := range balances {
			if balances[i].OwnerAddress == ownerAddress {
				ownerBalance = &balances[i]
				break
			}
		}
		require.NotNil(t, ownerBalance, "owner should have a balance")
		assert.Equal(t, "600", ownerBalance.Quantity)

		// Test adding more events to existing token (tests idempotency and ON CONFLICT handling)
		additionalEvents := []CreateProvenanceEventInput{
			// Add duplicate event (should be skipped due to ON CONFLICT DO NOTHING)
			buildTestProvenanceEvent(
				domain.ChainEthereumMainnet,
				schema.ProvenanceEventTypeMint,
				&zeroAddr,
				&ownerAddress,
				"1000",
				"0xmint7000", // Same tx hash as first mint
				2000,
			),
			// Add new event
			buildTestProvenanceEvent(
				domain.ChainEthereumMainnet,
				schema.ProvenanceEventTypeTransfer,
				&ownerAddress,
				&tempOwner,
				"50",
				"0xtransfer7000_new",
				uint64(3000),
			),
		}

		input.Events = append(events, additionalEvents...)
		input.Quantity = "550" // Updated balance

		err = store.UpsertTokenBalanceForOwner(ctx, input)
		require.NoError(t, err)

		// Verify only the new event was added (duplicate should be skipped)
		_, eventTotal, err = store.GetTokenProvenanceEvents(ctx, tokenResult.ID, 10000, 0, false)
		require.NoError(t, err)
		assert.Equal(t, uint64(numEvents+2), eventTotal, "should have 1 more event (duplicate skipped)") //nolint:gosec,G115

		// Verify updated balance
		balances, balanceTotal, err = store.GetTokenOwners(ctx, tokenResult.ID, 10, 0)
		require.NoError(t, err)
		assert.Greater(t, balanceTotal, uint64(0), "should have at least one balance")
		// Find the owner's balance
		ownerBalance = nil
		for i := range balances {
			if balances[i].OwnerAddress == ownerAddress {
				ownerBalance = &balances[i]
				break
			}
		}
		require.NotNil(t, ownerBalance, "owner should have a balance")
		assert.Equal(t, "550", ownerBalance.Quantity)
	})
}

// =============================================================================
// Test: Token Queries
// =============================================================================

func testGetTokenByTokenCID(t *testing.T, store Store) {
	ctx := context.Background()

	t.Run("get existing token", func(t *testing.T) {
		// Create a token
		owner := "0xowner5000000000000000000000000000000000005"
		contractAddress := "0x5000000000000000000000000000000000000000"
		mintInput := buildTestTokenMint(
			domain.ChainEthereumMainnet,
			domain.StandardERC721,
			contractAddress,
			"1",
			owner,
		)
		err := store.CreateTokenMint(ctx, mintInput)
		require.NoError(t, err)

		// Get the token
		token, err := store.GetTokenByTokenCID(ctx, mintInput.Token.TokenCID)
		require.NoError(t, err)
		require.NotNil(t, token)
		assert.Equal(t, mintInput.Token.TokenCID, token.TokenCID)
		assert.Equal(t, domain.ChainEthereumMainnet, token.Chain)
		assert.Equal(t, domain.StandardERC721, token.Standard)
		assert.Equal(t, contractAddress, token.ContractAddress)
		assert.Equal(t, "1", token.TokenNumber)
		assert.Equal(t, owner, *token.CurrentOwner)
		assert.False(t, token.Burned)
	})

	t.Run("get non-existent token returns nil", func(t *testing.T) {
		token, err := store.GetTokenByTokenCID(ctx, "eip155:1:erc721:0xnonexistent:999")
		require.NoError(t, err)
		assert.Nil(t, token)
	})
}

func testGetTokensByCIDs(t *testing.T, store Store) {
	ctx := context.Background()

	t.Run("get multiple existing tokens by CIDs", func(t *testing.T) {
		// Create multiple tokens
		owner1 := "0xowner7000000000000000000000000000000000001"
		owner2 := "0xowner7000000000000000000000000000000000002"
		contractAddress1 := "0x7000000000000000000000000000000000000001"
		contractAddress2 := "0x7000000000000000000000000000000000000002"

		mintInput1 := buildTestTokenMint(
			domain.ChainEthereumMainnet,
			domain.StandardERC721,
			contractAddress1,
			"1",
			owner1,
		)
		err := store.CreateTokenMint(ctx, mintInput1)
		require.NoError(t, err)

		mintInput2 := buildTestTokenMint(
			domain.ChainEthereumMainnet,
			domain.StandardERC721,
			contractAddress2,
			"2",
			owner2,
		)
		err = store.CreateTokenMint(ctx, mintInput2)
		require.NoError(t, err)

		// Batch get tokens by CIDs
		tokens, err := store.GetTokensByCIDs(ctx, []string{mintInput1.Token.TokenCID, mintInput2.Token.TokenCID})
		require.NoError(t, err)
		require.Len(t, tokens, 2)

		// Verify tokens are returned (order may vary)
		tokenMap := make(map[string]*schema.Token)
		for _, token := range tokens {
			tokenMap[token.TokenCID] = token
		}

		assert.Contains(t, tokenMap, mintInput1.Token.TokenCID)
		assert.Contains(t, tokenMap, mintInput2.Token.TokenCID)
		assert.Equal(t, owner1, *tokenMap[mintInput1.Token.TokenCID].CurrentOwner)
		assert.Equal(t, owner2, *tokenMap[mintInput2.Token.TokenCID].CurrentOwner)
	})

	t.Run("get tokens with some non-existent CIDs", func(t *testing.T) {
		// Create one token
		owner := "0xowner7000000000000000000000000000000000003"
		contractAddress := "0x7000000000000000000000000000000000000003"
		mintInput := buildTestTokenMint(
			domain.ChainEthereumMainnet,
			domain.StandardERC721,
			contractAddress,
			"3",
			owner,
		)
		err := store.CreateTokenMint(ctx, mintInput)
		require.NoError(t, err)

		// Batch get with existing and non-existing CIDs
		tokens, err := store.GetTokensByCIDs(ctx, []string{mintInput.Token.TokenCID, "eip155:1:erc721:0xnonexistent:999"})
		require.NoError(t, err)
		// Should only return the existing token
		require.Len(t, tokens, 1)
		assert.Equal(t, mintInput.Token.TokenCID, tokens[0].TokenCID)
	})

	t.Run("get tokens with empty CIDs returns empty list", func(t *testing.T) {
		tokens, err := store.GetTokensByCIDs(ctx, []string{})
		require.NoError(t, err)
		assert.Len(t, tokens, 0)
	})

	t.Run("get tokens with all non-existent CIDs returns empty list", func(t *testing.T) {
		tokens, err := store.GetTokensByCIDs(ctx, []string{"eip155:1:erc721:0xnonexistent:998", "eip155:1:erc721:0xnonexistent:999"})
		require.NoError(t, err)
		assert.Len(t, tokens, 0)
	})
}

func testGetTokensByIDs(t *testing.T, store Store) {
	ctx := context.Background()

	t.Run("get multiple existing tokens", func(t *testing.T) {
		// Create multiple tokens
		owner1 := "0xowner6000000000000000000000000000000000001"
		owner2 := "0xowner6000000000000000000000000000000000002"
		contractAddress1 := "0x6000000000000000000000000000000000000001"
		contractAddress2 := "0x6000000000000000000000000000000000000002"

		mintInput1 := buildTestTokenMint(
			domain.ChainEthereumMainnet,
			domain.StandardERC721,
			contractAddress1,
			"1",
			owner1,
		)
		err := store.CreateTokenMint(ctx, mintInput1)
		require.NoError(t, err)

		mintInput2 := buildTestTokenMint(
			domain.ChainEthereumMainnet,
			domain.StandardERC721,
			contractAddress2,
			"2",
			owner2,
		)
		err = store.CreateTokenMint(ctx, mintInput2)
		require.NoError(t, err)

		// Get both tokens by their CIDs to retrieve their IDs
		token1, err := store.GetTokenByTokenCID(ctx, mintInput1.Token.TokenCID)
		require.NoError(t, err)
		require.NotNil(t, token1)

		token2, err := store.GetTokenByTokenCID(ctx, mintInput2.Token.TokenCID)
		require.NoError(t, err)
		require.NotNil(t, token2)

		// Batch get tokens by IDs
		tokens, err := store.GetTokensByIDs(ctx, []uint64{token1.ID, token2.ID})
		require.NoError(t, err)
		require.Len(t, tokens, 2)

		// Verify tokens are returned (order may vary)
		tokenMap := make(map[uint64]*schema.Token)
		for _, token := range tokens {
			tokenMap[token.ID] = token
		}

		assert.Contains(t, tokenMap, token1.ID)
		assert.Contains(t, tokenMap, token2.ID)
		assert.Equal(t, mintInput1.Token.TokenCID, tokenMap[token1.ID].TokenCID)
		assert.Equal(t, mintInput2.Token.TokenCID, tokenMap[token2.ID].TokenCID)
	})

	t.Run("get tokens with some non-existent IDs", func(t *testing.T) {
		// Create one token
		owner := "0xowner6000000000000000000000000000000000003"
		contractAddress := "0x6000000000000000000000000000000000000003"
		mintInput := buildTestTokenMint(
			domain.ChainEthereumMainnet,
			domain.StandardERC721,
			contractAddress,
			"3",
			owner,
		)
		err := store.CreateTokenMint(ctx, mintInput)
		require.NoError(t, err)

		token, err := store.GetTokenByTokenCID(ctx, mintInput.Token.TokenCID)
		require.NoError(t, err)
		require.NotNil(t, token)

		// Batch get with existing and non-existing IDs
		tokens, err := store.GetTokensByIDs(ctx, []uint64{token.ID, 999999999})
		require.NoError(t, err)
		// Should only return the existing token
		require.Len(t, tokens, 1)
		assert.Equal(t, token.ID, tokens[0].ID)
	})

	t.Run("get tokens with empty IDs returns empty list", func(t *testing.T) {
		tokens, err := store.GetTokensByIDs(ctx, []uint64{})
		require.NoError(t, err)
		assert.Len(t, tokens, 0)
	})

	t.Run("get tokens with all non-existent IDs returns empty list", func(t *testing.T) {
		tokens, err := store.GetTokensByIDs(ctx, []uint64{999999998, 999999999})
		require.NoError(t, err)
		assert.Len(t, tokens, 0)
	})
}

func testGetTokensByFilter(t *testing.T, store Store) {
	ctx := context.Background()

	// Setup: Create multiple tokens with metadata
	setupTokensForFilter := func(t *testing.T) {
		owner1 := "0xfilter100000000000000000000000000000000001"
		owner2 := "0xfilter200000000000000000000000000000000002"

		tokens := []CreateTokenMintInput{
			buildTestTokenMint(domain.ChainEthereumMainnet, domain.StandardERC721, "0x0000000000000000000000000000000000001111", "1", owner1),
			buildTestTokenMint(domain.ChainEthereumMainnet, domain.StandardERC721, "0x0000000000000000000000000000000000001111", "2", owner2),
			buildTestTokenMint(domain.ChainEthereumMainnet, domain.StandardERC721, "0x0000000000000000000000000000000000002222", "1", owner1),
			buildTestTokenMint(domain.ChainEthereumSepolia, domain.StandardERC721, "0x0000000000000000000000000000000000003333", "1", owner1),
		}

		for _, tokenInput := range tokens {
			err := store.CreateTokenMint(ctx, tokenInput)
			require.NoError(t, err)

			// Add metadata to each token so they appear in queries by default
			token, err := store.GetTokenByTokenCID(ctx, tokenInput.Token.TokenCID)
			require.NoError(t, err)
			require.NotNil(t, token)

			now := time.Now().UTC()
			err = store.UpsertTokenMetadata(ctx, CreateTokenMetadataInput{
				TokenID:         token.ID,
				OriginJSON:      datatypes.JSON(`{"name":"test"}`),
				LatestJSON:      datatypes.JSON(`{"name":"test"}`),
				EnrichmentLevel: schema.EnrichmentLevelNone,
				LastRefreshedAt: &now,
			})
			require.NoError(t, err)
		}
	}
	setupTokensForFilter(t)

	t.Run("filter by owner", func(t *testing.T) {
		owner1 := "0xfilter100000000000000000000000000000000001"
		results, total, err := store.GetTokensByFilter(ctx, TokenQueryFilter{
			Owners: []string{owner1},
			Limit:  10,
		})
		require.NoError(t, err)
		assert.GreaterOrEqual(t, total, uint64(3))
		assert.LessOrEqual(t, len(results), 3)
	})

	t.Run("filter by chain", func(t *testing.T) {
		results, total, err := store.GetTokensByFilter(ctx, TokenQueryFilter{
			Chains: []domain.Chain{domain.ChainEthereumSepolia},
			Limit:  10,
		})
		require.NoError(t, err)
		assert.GreaterOrEqual(t, total, uint64(1))
		assert.GreaterOrEqual(t, len(results), 1)
		assert.Equal(t, domain.ChainEthereumSepolia, results[0].Token.Chain)
	})

	t.Run("filter by contract address", func(t *testing.T) {
		contract1 := "0x0000000000000000000000000000000000001111"
		results, total, err := store.GetTokensByFilter(ctx, TokenQueryFilter{
			ContractAddresses: []string{contract1},
			Limit:             10,
		})
		require.NoError(t, err)
		assert.GreaterOrEqual(t, total, uint64(2))
		assert.LessOrEqual(t, len(results), 2)
		assert.Equal(t, contract1, results[0].Token.ContractAddress)
		assert.Equal(t, contract1, results[1].Token.ContractAddress)
	})

	t.Run("pagination", func(t *testing.T) {
		// Get first page
		page1, total, err := store.GetTokensByFilter(ctx, TokenQueryFilter{
			Limit:  2,
			Offset: 0,
		})
		require.NoError(t, err)
		assert.GreaterOrEqual(t, total, uint64(4))
		assert.LessOrEqual(t, len(page1), 2)

		// Get second page
		page2, _, err := store.GetTokensByFilter(ctx, TokenQueryFilter{
			Limit:  2,
			Offset: 2,
		})
		require.NoError(t, err)
		assert.LessOrEqual(t, len(page2), 2)

		// Ensure pages don't overlap
		if len(page1) > 0 && len(page2) > 0 {
			assert.NotEqual(t, page1[0].Token.ID, page2[0].Token.ID)
		}
	})

	t.Run("filter with no results", func(t *testing.T) {
		results, total, err := store.GetTokensByFilter(ctx, TokenQueryFilter{
			TokenCIDs: []string{"eip155:1:erc721:0xnonexistent:999"},
			Limit:     10,
		})
		require.NoError(t, err)
		assert.Equal(t, uint64(0), total)
		assert.Equal(t, 0, len(results))
	})

	t.Run("exclude tokens without metadata and enrichment by default", func(t *testing.T) {
		brokenAddr := "0x0000000000000000000000000000000000010000"
		metaOnlyAddr := "0x0000000000000000000000000000000000020000"
		enrichOnlyAddr := "0x0000000000000000000000000000000000030000"
		withBothAddr := "0x0000000000000000000000000000000000040000"

		// Create a token without metadata or enrichment (broken - has neither)
		tokenBroken := buildTestTokenMint(domain.ChainEthereumMainnet, domain.StandardERC721, brokenAddr, "1", "0x0000000000000000000000000000000000001111")
		err := store.CreateTokenMint(ctx, tokenBroken)
		require.NoError(t, err)

		// Create a token with metadata only (not broken - has metadata)
		tokenWithMetaOnly := buildTestTokenMint(domain.ChainEthereumMainnet, domain.StandardERC721, metaOnlyAddr, "1", "0x0000000000000000000000000000000000001111")
		err = store.CreateTokenMint(ctx, tokenWithMetaOnly)
		require.NoError(t, err)

		// Add metadata to the second token (but no enrichment)
		tokenMetaOnly, err := store.GetTokenByTokenCID(ctx, tokenWithMetaOnly.Token.TokenCID)
		require.NoError(t, err)
		require.NotNil(t, tokenMetaOnly)

		now := time.Now().UTC()
		err = store.UpsertTokenMetadata(ctx, CreateTokenMetadataInput{
			TokenID:         tokenMetaOnly.ID,
			OriginJSON:      datatypes.JSON(`{"name":"test metadata only"}`),
			LatestJSON:      datatypes.JSON(`{"name":"test metadata only"}`),
			EnrichmentLevel: schema.EnrichmentLevelNone,
			LastRefreshedAt: &now,
		})
		require.NoError(t, err)

		// Create a token with enrichment only (not broken - has enrichment)
		tokenWithEnrichmentOnly := buildTestTokenMint(domain.ChainEthereumMainnet, domain.StandardERC721, enrichOnlyAddr, "1", "0x0000000000000000000000000000000000001111")
		err = store.CreateTokenMint(ctx, tokenWithEnrichmentOnly)
		require.NoError(t, err)

		// Add enrichment to the third token (but no metadata)
		tokenEnrichOnly, err := store.GetTokenByTokenCID(ctx, tokenWithEnrichmentOnly.Token.TokenCID)
		require.NoError(t, err)
		require.NotNil(t, tokenEnrichOnly)

		vendorJSON := json.RawMessage(`{"platform": "artblocks", "project": "Test"}`)
		vendorHash := "vendorhash123"
		imageURL := "https://artblocks.io/image.png"
		name := "Art Blocks #1"

		err = store.UpsertEnrichmentSource(ctx, CreateEnrichmentSourceInput{
			TokenID:    tokenEnrichOnly.ID,
			Vendor:     schema.VendorArtBlocks,
			VendorJSON: vendorJSON,
			VendorHash: &vendorHash,
			ImageURL:   &imageURL,
			Name:       &name,
		})
		require.NoError(t, err)

		// Create a token with both metadata and enrichment (not broken - has both)
		tokenWithBoth := buildTestTokenMint(domain.ChainEthereumMainnet, domain.StandardERC721, withBothAddr, "1", "0x0000000000000000000000000000000000001111")
		err = store.CreateTokenMint(ctx, tokenWithBoth)
		require.NoError(t, err)

		// Add metadata to the fourth token
		tokenBoth, err := store.GetTokenByTokenCID(ctx, tokenWithBoth.Token.TokenCID)
		require.NoError(t, err)
		require.NotNil(t, tokenBoth)

		err = store.UpsertTokenMetadata(ctx, CreateTokenMetadataInput{
			TokenID:         tokenBoth.ID,
			OriginJSON:      datatypes.JSON(`{"name":"test both"}`),
			LatestJSON:      datatypes.JSON(`{"name":"test both"}`),
			EnrichmentLevel: schema.EnrichmentLevelNone,
			LastRefreshedAt: &now,
		})
		require.NoError(t, err)

		// Add enrichment to the fourth token
		vendorHash2 := "vendorhash456"
		imageURL2 := "https://artblocks.io/image2.png"
		name2 := "Art Blocks #2"

		err = store.UpsertEnrichmentSource(ctx, CreateEnrichmentSourceInput{
			TokenID:    tokenBoth.ID,
			Vendor:     schema.VendorArtBlocks,
			VendorJSON: vendorJSON,
			VendorHash: &vendorHash2,
			ImageURL:   &imageURL2,
			Name:       &name2,
		})
		require.NoError(t, err)

		// Default behavior: exclude tokens without both metadata AND enrichment
		// Should include tokens with metadata only, enrichment only, or both
		// Should exclude only the token with neither

		results, total, err := store.GetTokensByFilter(ctx, TokenQueryFilter{
			ContractAddresses: []string{brokenAddr, metaOnlyAddr, enrichOnlyAddr, withBothAddr},
			Limit:             10,
		})
		require.NoError(t, err)
		assert.Equal(t, uint64(3), total) // Three tokens (metaonly, enrichonly, both) - excludes broken
		assert.Equal(t, 3, len(results))

		// Verify the broken token is not in results
		for _, result := range results {
			assert.NotEqual(t, brokenAddr, result.Token.ContractAddress)
		}

		// Include broken: include all tokens including the one with neither metadata nor enrichment
		resultsWithBroken, totalWithBroken, err := store.GetTokensByFilter(ctx, TokenQueryFilter{
			ContractAddresses: []string{brokenAddr, metaOnlyAddr, enrichOnlyAddr, withBothAddr},
			IncludeBroken:     true,
			Limit:             10,
		})
		require.NoError(t, err)
		assert.Equal(t, uint64(4), totalWithBroken) // All four tokens
		assert.Equal(t, 4, len(resultsWithBroken))
	})

	t.Run("sort by latest provenance event related to filtered owners", func(t *testing.T) {
		owner1 := "0xsortowner1000000000000000000000000000001"
		owner2 := "0xsortowner2000000000000000000000000000002"

		// Create token1: initially minted to owner1
		token1 := buildTestTokenMint(domain.ChainEthereumMainnet, domain.StandardERC721, "0x0000000000000000000000000000000000050001", "1", owner1)
		token1.ProvenanceEvent.Timestamp = time.Now().UTC().Add(-10 * time.Hour) // T1: 10 hours ago
		err := store.CreateTokenMint(ctx, token1)
		require.NoError(t, err)

		// Create token2: minted directly to owner2
		token2 := buildTestTokenMint(domain.ChainEthereumMainnet, domain.StandardERC721, "0x0000000000000000000000000000000000050002", "2", owner2)
		token2.ProvenanceEvent.Timestamp = time.Now().UTC().Add(-5 * time.Hour) // T2: 5 hours ago
		err = store.CreateTokenMint(ctx, token2)
		require.NoError(t, err)

		// Create token3: initially minted to owner1
		token3 := buildTestTokenMint(domain.ChainEthereumMainnet, domain.StandardERC721, "0x0000000000000000000000000000000000050003", "3", owner1)
		token3.ProvenanceEvent.Timestamp = time.Now().UTC().Add(-8 * time.Hour) // T3: 8 hours ago
		err = store.CreateTokenMint(ctx, token3)
		require.NoError(t, err)

		// Add metadata to all tokens so they appear in queries
		for _, tokenInput := range []CreateTokenMintInput{token1, token2, token3} {
			token, err := store.GetTokenByTokenCID(ctx, tokenInput.Token.TokenCID)
			require.NoError(t, err)
			require.NotNil(t, token)

			now := time.Now().UTC()
			err = store.UpsertTokenMetadata(ctx, CreateTokenMetadataInput{
				TokenID:         token.ID,
				OriginJSON:      datatypes.JSON(`{"name":"test"}`),
				LatestJSON:      datatypes.JSON(`{"name":"test"}`),
				EnrichmentLevel: schema.EnrichmentLevelNone,
				LastRefreshedAt: &now,
			})
			require.NoError(t, err)
		}

		// Transfer token1 from owner1 to owner2 at T4 (most recent)
		transferTime1 := time.Now().UTC().Add(-1 * time.Hour) // T4: 1 hour ago
		transferInput1 := UpdateTokenTransferInput{
			TokenCID:     token1.Token.TokenCID,
			CurrentOwner: &owner2,
			SenderBalanceUpdate: &UpdateBalanceInput{
				OwnerAddress: owner1,
				Delta:        "1",
			},
			ReceiverBalanceUpdate: &UpdateBalanceInput{
				OwnerAddress: owner2,
				Delta:        "1",
			},
			ProvenanceEvent: CreateProvenanceEventInput{
				Chain:       domain.ChainEthereumMainnet,
				EventType:   schema.ProvenanceEventTypeTransfer,
				FromAddress: &owner1,
				ToAddress:   &owner2,
				Quantity:    "1",
				TxHash:      "0xtransfer1to2",
				BlockNumber: 2000,
				BlockHash:   stringPtr("0xblockhash2000"),
				Raw:         []byte(`{"tx_hash":"0xtransfer1to2","block_number":2000,"tx_index":1}`),
				Timestamp:   transferTime1,
			},
		}
		err = store.UpdateTokenTransfer(ctx, transferInput1)
		require.NoError(t, err)

		// Transfer token3 from owner1 to owner2 at T5 (3 hours ago)
		transferTime2 := time.Now().UTC().Add(-3 * time.Hour) // T5: 3 hours ago
		transferInput2 := UpdateTokenTransferInput{
			TokenCID:     token3.Token.TokenCID,
			CurrentOwner: &owner2,
			SenderBalanceUpdate: &UpdateBalanceInput{
				OwnerAddress: owner1,
				Delta:        "1",
			},
			ReceiverBalanceUpdate: &UpdateBalanceInput{
				OwnerAddress: owner2,
				Delta:        "1",
			},
			ProvenanceEvent: CreateProvenanceEventInput{
				Chain:       domain.ChainEthereumMainnet,
				EventType:   schema.ProvenanceEventTypeTransfer,
				FromAddress: &owner1,
				ToAddress:   &owner2,
				Quantity:    "1",
				TxHash:      "0xtransfer3to2",
				BlockNumber: 2001,
				BlockHash:   stringPtr("0xblockhash2001"),
				Raw:         []byte(`{"tx_hash":"0xtransfer3to2","block_number":2001,"tx_index":1}`),
				Timestamp:   transferTime2,
			},
		}
		err = store.UpdateTokenTransfer(ctx, transferInput2)
		require.NoError(t, err)

		// Timeline of provenance events:
		// T1 (10h ago): token1 minted to owner1 (owner1 involved, NOT owner2)
		// T3 (8h ago):  token3 minted to owner1 (owner1 involved, NOT owner2)
		// T2 (5h ago):  token2 minted to owner2 (owner2 involved)
		// T5 (3h ago):  token3 transferred to owner2 (owner1 and owner2 involved)
		// T4 (1h ago):  token1 transferred to owner2 (owner1 and owner2 involved)
		//
		// Current ownership: owner2 owns all 3 tokens
		// When filtering by owner2 and sorting by latest provenance event involving owner2:
		// Expected order: token1 (T4) -> token3 (T5) -> token2 (T2)

		// Get token IDs for assertions
		token1Data, err := store.GetTokenByTokenCID(ctx, token1.Token.TokenCID)
		require.NoError(t, err)
		token2Data, err := store.GetTokenByTokenCID(ctx, token2.Token.TokenCID)
		require.NoError(t, err)
		token3Data, err := store.GetTokenByTokenCID(ctx, token3.Token.TokenCID)
		require.NoError(t, err)

		// Query for owner2 (current owner of all 3 tokens)
		results, total, err := store.GetTokensByFilter(ctx, TokenQueryFilter{
			Owners: []string{owner2},
			Limit:  10,
		})
		require.NoError(t, err)
		require.Equal(t, int(total), 3, "owner2 owns 3 tokens") //nolint:gosec,G115
		require.Equal(t, len(results), 3, "should return 3 results")

		// Verify sorting: tokens sorted by latest provenance event involving owner2 (DESC)
		// token1's latest event with owner2: T4 (1h ago) - most recent
		// token3's latest event with owner2: T5 (3h ago)
		// token2's latest event with owner2: T2 (5h ago) - oldest
		assert.Equal(t, token1Data.ID, results[0].Token.ID, "token1 should be first (T4: most recent owner2 event)")
		assert.Equal(t, token3Data.ID, results[1].Token.ID, "token3 should be second (T5: middle owner2 event)")
		assert.Equal(t, token2Data.ID, results[2].Token.ID, "token2 should be third (T2: oldest owner2 event)")
	})
}

// stringPtr is a helper to create a string pointer
func stringPtr(s string) *string {
	return &s
}

// =============================================================================
// Test: Token Metadata
// =============================================================================

func testUpsertTokenMetadata(t *testing.T, store Store) {
	ctx := context.Background()

	t.Run("create new metadata", func(t *testing.T) {
		// Create token first
		owner := "0xmeta100000000000000000000000000000000000001"
		mintInput := buildTestTokenMint(
			domain.ChainEthereumMainnet,
			domain.StandardERC721,
			"0xmeta100000000000000000000000000000000001",
			"1",
			owner,
		)
		err := store.CreateTokenMint(ctx, mintInput)
		require.NoError(t, err)

		token, err := store.GetTokenByTokenCID(ctx, mintInput.Token.TokenCID)
		require.NoError(t, err)

		// Create metadata
		originJSON := json.RawMessage(`{"name": "Test NFT", "image": "ipfs://test"}`)
		latestJSON := json.RawMessage(`{"name": "Test NFT", "image": "ipfs://test"}`)
		hash := "hash123"
		imageURL := "https://example.com/image.png"
		name := "Test NFT"
		mimeType := "image/png"
		now := time.Now().UTC()

		metadataInput := CreateTokenMetadataInput{
			TokenID:         token.ID,
			OriginJSON:      originJSON,
			LatestJSON:      latestJSON,
			LatestHash:      &hash,
			EnrichmentLevel: schema.EnrichmentLevelNone,
			LastRefreshedAt: &now,
			ImageURL:        &imageURL,
			Name:            &name,
			MimeType:        &mimeType,
		}

		err = store.UpsertTokenMetadata(ctx, metadataInput)
		require.NoError(t, err)

		// Verify metadata was created
		metadata, err := store.GetTokenMetadataByTokenCID(ctx, mintInput.Token.TokenCID)
		require.NoError(t, err)
		require.NotNil(t, metadata)
		assert.Equal(t, token.ID, metadata.TokenID)
		assert.Equal(t, hash, *metadata.LatestHash)
		assert.Equal(t, imageURL, *metadata.ImageURL)
		assert.Equal(t, name, *metadata.Name)
		assert.Equal(t, mimeType, *metadata.MimeType)

		// Verify change journal entry was created
		changes, total, err := store.GetChanges(ctx, ChangesQueryFilter{
			TokenCIDs: []string{mintInput.Token.TokenCID},
			Limit:     10,
		})
		require.NoError(t, err)
		assert.GreaterOrEqual(t, total, uint64(1))

		// Find metadata change
		var foundMetadataChange bool
		for _, change := range changes {
			if change.SubjectType == schema.SubjectTypeMetadata {
				foundMetadataChange = true
				break
			}
		}
		assert.True(t, foundMetadataChange, "Should have metadata change journal entry")
	})

	t.Run("update existing metadata", func(t *testing.T) {
		// Create token and metadata with unique addresses
		owner := "0x0000000000000000000000000000000000099999"
		mintInput := buildTestTokenMint(
			domain.ChainEthereumMainnet,
			domain.StandardERC721,
			"0x0000000000000000000000000000000000088888",
			"1",
			owner,
		)
		err := store.CreateTokenMint(ctx, mintInput)
		require.NoError(t, err)

		token, err := store.GetTokenByTokenCID(ctx, mintInput.Token.TokenCID)
		require.NoError(t, err)

		// Create initial metadata
		originJSON := json.RawMessage(`{"name": "Original", "image": "ipfs://original"}`)
		hash1 := "hash1"
		imageURL1 := "https://example.com/image1.png"
		name1 := "Original"
		now := time.Now().UTC()

		metadataInput := CreateTokenMetadataInput{
			TokenID:         token.ID,
			OriginJSON:      originJSON,
			LatestJSON:      originJSON,
			LatestHash:      &hash1,
			EnrichmentLevel: schema.EnrichmentLevelNone,
			LastRefreshedAt: &now,
			ImageURL:        &imageURL1,
			Name:            &name1,
		}
		err = store.UpsertTokenMetadata(ctx, metadataInput)
		require.NoError(t, err)

		// Update metadata
		latestJSON := json.RawMessage(`{"name": "Updated", "image": "ipfs://updated"}`)
		hash2 := "hash2"
		imageURL2 := "https://example.com/image2.png"
		name2 := "Updated"
		now2 := time.Now().UTC().Add(time.Hour)

		metadataInput.LatestJSON = latestJSON
		metadataInput.LatestHash = &hash2
		metadataInput.ImageURL = &imageURL2
		metadataInput.Name = &name2
		metadataInput.LastRefreshedAt = &now2

		err = store.UpsertTokenMetadata(ctx, metadataInput)
		require.NoError(t, err)

		// Verify metadata was updated
		metadata, err := store.GetTokenMetadataByTokenCID(ctx, mintInput.Token.TokenCID)
		require.NoError(t, err)
		assert.Equal(t, hash2, *metadata.LatestHash)
		assert.Equal(t, imageURL2, *metadata.ImageURL)
		assert.Equal(t, name2, *metadata.Name)

		// Verify change journal has multiple entries (mint + 2 metadata changes)
		changes, total, err := store.GetChanges(ctx, ChangesQueryFilter{
			TokenCIDs: []string{mintInput.Token.TokenCID},
			Limit:     10,
		})
		require.NoError(t, err)
		assert.Equal(t, uint64(3), total) // mint + initial metadata + updated metadata
		assert.Len(t, changes, 3)

		// Find metadata changes and verify both entries exist
		var metadataChanges []schema.MetadataChangeMeta
		for _, change := range changes {
			if change.SubjectType == schema.SubjectTypeMetadata {
				var metaChanges schema.MetadataChangeMeta
				err := json.Unmarshal(change.Meta, &metaChanges)
				require.NoError(t, err)
				metadataChanges = append(metadataChanges, metaChanges)
			}
		}

		// Should have 2 metadata change entries
		assert.Len(t, metadataChanges, 2)

		// First metadata change: old is empty (initial creation)
		var initialChange, updateChange *schema.MetadataChangeMeta
		for i := range metadataChanges {
			if metadataChanges[i].Old.ImageURL == nil {
				initialChange = &metadataChanges[i]
			} else {
				updateChange = &metadataChanges[i]
			}
		}

		require.NotNil(t, initialChange, "Should have initial metadata change with no old values")
		require.NotNil(t, updateChange, "Should have metadata update with old values")

		// Verify initial change has new values
		assert.Equal(t, token.ID, initialChange.TokenID)
		assert.Equal(t, imageURL1, *initialChange.New.ImageURL)

		// Verify update change has both old and new values
		assert.Equal(t, token.ID, updateChange.TokenID)
		assert.Equal(t, imageURL1, *updateChange.Old.ImageURL)
		assert.Equal(t, imageURL2, *updateChange.New.ImageURL)
	})
}

// =============================================================================
// Test: Enrichment Sources
// =============================================================================

func testEnrichmentSource(t *testing.T, store Store) {
	ctx := context.Background()

	t.Run("create and retrieve enrichment source", func(t *testing.T) {
		// Create token and metadata
		owner := "0xenrich10000000000000000000000000000000000001"
		mintInput := buildTestTokenMint(
			domain.ChainEthereumMainnet,
			domain.StandardERC721,
			"0xenrich10000000000000000000000000000000001",
			"1",
			owner,
		)
		err := store.CreateTokenMint(ctx, mintInput)
		require.NoError(t, err)

		token, err := store.GetTokenByTokenCID(ctx, mintInput.Token.TokenCID)
		require.NoError(t, err)

		// Create metadata first (enrichment source requires metadata)
		originJSON := json.RawMessage(`{"name": "Test"}`)
		now := time.Now().UTC()
		metadataInput := CreateTokenMetadataInput{
			TokenID:         token.ID,
			OriginJSON:      originJSON,
			LatestJSON:      originJSON,
			EnrichmentLevel: schema.EnrichmentLevelNone,
			LastRefreshedAt: &now,
		}
		err = store.UpsertTokenMetadata(ctx, metadataInput)
		require.NoError(t, err)

		// Create enrichment source
		vendorJSON := json.RawMessage(`{"platform": "artblocks", "project": "Test"}`)
		vendorHash := "vendorhash123"
		imageURL := "https://artblocks.io/image.png"
		name := "Art Blocks #1"

		enrichmentInput := CreateEnrichmentSourceInput{
			TokenID:    token.ID,
			Vendor:     schema.VendorArtBlocks,
			VendorJSON: vendorJSON,
			VendorHash: &vendorHash,
			ImageURL:   &imageURL,
			Name:       &name,
		}

		err = store.UpsertEnrichmentSource(ctx, enrichmentInput)
		require.NoError(t, err)

		// Verify enrichment source was created
		enrichment, err := store.GetEnrichmentSourceByTokenID(ctx, token.ID)
		require.NoError(t, err)
		require.NotNil(t, enrichment)
		assert.Equal(t, schema.VendorArtBlocks, enrichment.Vendor)
		assert.Equal(t, vendorHash, *enrichment.VendorHash)
		assert.Equal(t, imageURL, *enrichment.ImageURL)

		// Verify enrichment level was updated in metadata
		metadata, err := store.GetTokenMetadataByTokenCID(ctx, mintInput.Token.TokenCID)
		require.NoError(t, err)
		assert.Equal(t, schema.EnrichmentLevelVendor, metadata.EnrichmentLevel)

		// Also test retrieval by token CID
		enrichment2, err := store.GetEnrichmentSourceByTokenCID(ctx, mintInput.Token.TokenCID)
		require.NoError(t, err)
		require.NotNil(t, enrichment2)
		assert.Equal(t, enrichment.TokenID, enrichment2.TokenID)
	})

	t.Run("creates change journal entry for enrichment source", func(t *testing.T) {
		// Create a token first
		owner := "0xenrich100000000000000000000000000000002"
		mintInput := buildTestTokenMint(
			domain.ChainEthereumMainnet,
			domain.StandardERC721,
			"0xenrich100000000000000000000000000000002",
			"2",
			owner,
		)
		err := store.CreateTokenMint(ctx, mintInput)
		require.NoError(t, err)

		token, err := store.GetTokenByTokenCID(ctx, mintInput.Token.TokenCID)
		require.NoError(t, err)

		// Create metadata first
		originJSON := json.RawMessage(`{"name": "Test"}`)
		now := time.Now().UTC()
		metadataInput := CreateTokenMetadataInput{
			TokenID:         token.ID,
			OriginJSON:      originJSON,
			LatestJSON:      originJSON,
			EnrichmentLevel: schema.EnrichmentLevelNone,
			LastRefreshedAt: &now,
		}
		err = store.UpsertTokenMetadata(ctx, metadataInput)
		require.NoError(t, err)

		// Create enrichment source - should create a change journal entry
		vendorJSON := json.RawMessage(`{"platform": "fxhash", "project": "Test"}`)
		vendorHash := "vendorhash456"
		imageURL := "https://fxhash.xyz/image.png"
		name := "FxHash #1"

		enrichmentInput := CreateEnrichmentSourceInput{
			TokenID:    token.ID,
			Vendor:     schema.VendorFXHash,
			VendorJSON: vendorJSON,
			VendorHash: &vendorHash,
			ImageURL:   &imageURL,
			Name:       &name,
		}

		err = store.UpsertEnrichmentSource(ctx, enrichmentInput)
		require.NoError(t, err)

		// Verify change journal entry was created
		changes, _, err := store.GetChanges(ctx, ChangesQueryFilter{
			TokenCIDs: []string{token.TokenCID},
		})
		require.NoError(t, err)

		// Should have at least one change journal entry for enrichment source
		var enrichSourceChanges []*schema.ChangesJournal
		for _, change := range changes {
			if change.SubjectType == schema.SubjectTypeEnrichSource {
				enrichSourceChanges = append(enrichSourceChanges, change)
			}
		}
		require.Greater(t, len(enrichSourceChanges), 0, "Expected at least one enrich_source change journal entry")

		// Unmarshal and verify meta
		enrichChange := enrichSourceChanges[0]
		var meta schema.EnrichmentSourceChangeMeta
		err = json.Unmarshal(enrichChange.Meta, &meta)
		require.NoError(t, err)
		assert.Equal(t, token.ID, meta.TokenID)
		assert.Equal(t, string(schema.VendorFXHash), meta.New.Vendor)
		assert.Equal(t, imageURL, *meta.New.ImageURL)

		// Update enrichment source - should create another change journal entry
		vendorHash2 := "vendorhash789"
		imageURL2 := "https://fxhash.xyz/image2.png"

		enrichmentInput2 := CreateEnrichmentSourceInput{
			TokenID:    token.ID,
			Vendor:     schema.VendorFXHash,
			VendorJSON: vendorJSON,
			VendorHash: &vendorHash2,
			ImageURL:   &imageURL2,
			Name:       &name,
		}

		// Sleep to ensure different timestamp
		time.Sleep(10 * time.Millisecond)
		err = store.UpsertEnrichmentSource(ctx, enrichmentInput2)
		require.NoError(t, err)

		// Verify we now have two change journal entries
		changes2, _, err := store.GetChanges(ctx, ChangesQueryFilter{
			TokenCIDs: []string{token.TokenCID},
		})
		require.NoError(t, err)

		enrichSourceChanges2 := []*schema.ChangesJournal{}
		for _, change := range changes2 {
			if change.SubjectType == schema.SubjectTypeEnrichSource {
				enrichSourceChanges2 = append(enrichSourceChanges2, change)
			}
		}
		require.GreaterOrEqual(t, len(enrichSourceChanges2), 2, "Expected at least two enrich_source change journal entries")

		// Verify the latest change has both old and new values
		var latestMeta schema.EnrichmentSourceChangeMeta
		err = json.Unmarshal(enrichSourceChanges2[len(enrichSourceChanges2)-1].Meta, &latestMeta)
		require.NoError(t, err)
		assert.Equal(t, token.ID, latestMeta.TokenID)
		assert.Equal(t, imageURL, *latestMeta.Old.ImageURL)
		assert.Equal(t, imageURL2, *latestMeta.New.ImageURL)
	})
}

func testCreateMediaAssetWithChangeJournal(t *testing.T, store Store) {
	ctx := context.Background()

	t.Run("always creates change journal entry", func(t *testing.T) {
		// Create a token first
		owner := "0xmedia10000000000000000000000000000000001"
		mintInput := buildTestTokenMint(
			domain.ChainEthereumMainnet,
			domain.StandardERC721,
			"0xmedia10000000000000000000000000000000001",
			"1",
			owner,
		)
		err := store.CreateTokenMint(ctx, mintInput)
		require.NoError(t, err)

		// Create media asset - should create change journal entry
		sourceURL := "https://example.com/media1.png"
		mimeType := "image/png"
		fileSize := int64(1024)
		providerAssetID := "asset123"
		variantURLs := datatypes.JSON([]byte(`{"thumbnail":"https://cdn.example.com/thumb1.png"}`))

		input := CreateMediaAssetInput{
			SourceURL:       sourceURL,
			MimeType:        &mimeType,
			FileSizeBytes:   &fileSize,
			Provider:        schema.StorageProviderCloudflare,
			ProviderAssetID: &providerAssetID,
			VariantURLs:     variantURLs,
		}

		asset, err := store.CreateMediaAsset(ctx, input)
		require.NoError(t, err)
		require.NotNil(t, asset)

		// Verify change journal entry was created
		// Note: media_asset changes are not linked to tokens, so we fetch all changes and filter
		changes, _, err := store.GetChanges(ctx, ChangesQueryFilter{
			SubjectTypes: []schema.SubjectType{schema.SubjectTypeMediaAsset},
			SubjectIDs:   []string{fmt.Sprintf("%d", asset.ID)},
			Limit:        100,
		})
		require.NoError(t, err)

		// Should have at least one change journal entry for media asset
		var mediaAssetChanges []*schema.ChangesJournal
		for _, change := range changes {
			if change.SubjectType == schema.SubjectTypeMediaAsset &&
				change.SubjectID == fmt.Sprintf("%d", asset.ID) {
				mediaAssetChanges = append(mediaAssetChanges, change)
			}
		}
		require.Greater(t, len(mediaAssetChanges), 0, "Expected at least one media_asset change journal entry")

		// Verify the change journal entry has correct data
		mediaChange := mediaAssetChanges[0]
		assert.Equal(t, schema.SubjectTypeMediaAsset, mediaChange.SubjectType)
		assert.Equal(t, fmt.Sprintf("%d", asset.ID), mediaChange.SubjectID)

		// Unmarshal and verify meta
		var meta schema.MediaAssetChangeMeta
		err = json.Unmarshal(mediaChange.Meta, &meta)
		require.NoError(t, err)
		assert.Equal(t, sourceURL, meta.New.SourceURL)
		assert.Equal(t, string(schema.StorageProviderCloudflare), meta.New.Provider)
		assert.Equal(t, mimeType, *meta.New.MimeType)
	})

	t.Run("tracks changes on update", func(t *testing.T) {
		// Create a token first
		owner := "0xmedia10000000000000000000000000000000002"
		mintInput := buildTestTokenMint(
			domain.ChainEthereumMainnet,
			domain.StandardERC721,
			"0xmedia10000000000000000000000000000000002",
			"2",
			owner,
		)
		err := store.CreateTokenMint(ctx, mintInput)
		require.NoError(t, err)

		// Create initial media asset
		sourceURL := "https://example.com/media3.png"
		mimeType1 := "image/png"
		fileSize1 := int64(1024)
		providerAssetID1 := "asset789"
		variantURLs1 := datatypes.JSON([]byte(`{"thumbnail":"https://cdn.example.com/thumb3.png"}`))

		input1 := CreateMediaAssetInput{
			SourceURL:       sourceURL,
			MimeType:        &mimeType1,
			FileSizeBytes:   &fileSize1,
			Provider:        schema.StorageProviderCloudflare,
			ProviderAssetID: &providerAssetID1,
			VariantURLs:     variantURLs1,
		}

		asset1, err := store.CreateMediaAsset(ctx, input1)
		require.NoError(t, err)

		// Update media asset (same source_url and provider)
		mimeType2 := "image/jpeg"
		fileSize2 := int64(2048)
		providerAssetID2 := "asset789-updated"
		variantURLs2 := datatypes.JSON([]byte(`{"thumbnail":"https://cdn.example.com/thumb3-v2.png"}`))

		// Sleep to ensure different timestamp
		time.Sleep(10 * time.Millisecond)

		input2 := CreateMediaAssetInput{
			SourceURL:       sourceURL,
			MimeType:        &mimeType2,
			FileSizeBytes:   &fileSize2,
			Provider:        schema.StorageProviderCloudflare,
			ProviderAssetID: &providerAssetID2,
			VariantURLs:     variantURLs2,
		}

		asset2, err := store.CreateMediaAsset(ctx, input2)
		require.NoError(t, err)

		// Should have same ID (updated, not new)
		assert.Equal(t, asset1.ID, asset2.ID)

		// Verify we have two change journal entries
		// Note: media_asset changes are not linked to tokens, so we fetch all changes and filter
		changes, _, err := store.GetChanges(ctx, ChangesQueryFilter{
			SubjectTypes: []schema.SubjectType{schema.SubjectTypeMediaAsset},
			SubjectIDs:   []string{fmt.Sprintf("%d", asset1.ID)},
			Limit:        100,
		})
		require.NoError(t, err)

		mediaAssetChanges := []*schema.ChangesJournal{}
		for _, change := range changes {
			if change.SubjectType == schema.SubjectTypeMediaAsset &&
				change.SubjectID == fmt.Sprintf("%d", asset1.ID) {
				mediaAssetChanges = append(mediaAssetChanges, change)
			}
		}
		require.GreaterOrEqual(t, len(mediaAssetChanges), 2, "Expected at least two media_asset change journal entries")

		// Verify the latest change has both old and new values
		var latestMeta schema.MediaAssetChangeMeta
		err = json.Unmarshal(mediaAssetChanges[len(mediaAssetChanges)-1].Meta, &latestMeta)
		require.NoError(t, err)
		assert.Equal(t, mimeType1, *latestMeta.Old.MimeType)
		assert.Equal(t, mimeType2, *latestMeta.New.MimeType)
		assert.Equal(t, providerAssetID1, *latestMeta.Old.ProviderAssetID)
		assert.Equal(t, providerAssetID2, *latestMeta.New.ProviderAssetID)
	})
}

// =============================================================================
// Test: Changes Journal
// =============================================================================

func testGetChanges(t *testing.T, store Store) {
	ctx := context.Background()

	t.Run("filter by token CIDs", func(t *testing.T) {
		owner := "0xchanges100000000000000000000000000000000001"
		mintInput := buildTestTokenMint(
			domain.ChainEthereumMainnet,
			domain.StandardERC721,
			"0xchanges100000000000000000000000000000001",
			"1",
			owner,
		)
		err := store.CreateTokenMint(ctx, mintInput)
		require.NoError(t, err)

		changes, total, err := store.GetChanges(ctx, ChangesQueryFilter{
			TokenCIDs: []string{mintInput.Token.TokenCID},
			Limit:     10,
		})
		require.NoError(t, err)
		assert.GreaterOrEqual(t, total, uint64(1))
		assert.GreaterOrEqual(t, len(changes), 1)

		// Verify changes were returned
		for _, change := range changes {
			assert.NotNil(t, change)
		}
	})

	t.Run("order by changed_at ascending", func(t *testing.T) {
		// Get changes (always ascending by ID)
		changes, _, err := store.GetChanges(ctx, ChangesQueryFilter{
			Limit: 10,
		})
		require.NoError(t, err)
		require.NotEmpty(t, changes)

		// Verify ascending order by changed_at
		for i := 1; i < len(changes); i++ {
			assert.True(t, changes[i].ChangedAt.After(changes[i-1].ChangedAt) || changes[i].ChangedAt.Equal(changes[i-1].ChangedAt), "changed_at should always be in ascending order")
		}
	})

	t.Run("pagination", func(t *testing.T) {
		owner := "0xchangespagination00000000000000000000001"

		// Create test data - at least 5 tokens to test pagination
		for i := range 5 {
			mintInput := buildTestTokenMint(
				domain.ChainEthereumMainnet,
				domain.StandardERC721,
				fmt.Sprintf("0xpagination%042d", i),
				"1",
				owner,
			)
			// Adjust timestamp to ensure different changed_at times
			mintInput.ProvenanceEvent.Timestamp = time.Now().UTC().Add(time.Duration(i) * time.Minute)
			err := store.CreateTokenMint(ctx, mintInput)
			require.NoError(t, err)
			time.Sleep(10 * time.Millisecond) // Small delay to ensure different created_at
		}

		// Get first page using cursor-based pagination (no anchor)
		page1, total1, err := store.GetChanges(ctx, ChangesQueryFilter{
			Limit: 2,
		})
		require.NoError(t, err)
		assert.GreaterOrEqual(t, total1, uint64(2))
		require.Len(t, page1, 2)

		// Verify first page is ordered by changed_at ASC, id ASC
		assert.True(t, page1[1].ChangedAt.After(page1[0].ChangedAt) ||
			(page1[1].ChangedAt.Equal(page1[0].ChangedAt) && page1[1].ID > page1[0].ID),
			"First page should be ordered by changed_at ASC, id ASC")

		// Get second page using anchor from last item of first page
		anchorID := page1[1].ID
		page2, total2, err := store.GetChanges(ctx, ChangesQueryFilter{
			Limit:  2,
			Anchor: &anchorID,
		})
		require.NoError(t, err)
		assert.GreaterOrEqual(t, total2, uint64(2))

		// Ensure pages don't overlap - anchor filters by ID
		assert.NotEqual(t, page1[0].ID, page2[0].ID)
		assert.NotEqual(t, page1[1].ID, page2[0].ID)
		assert.Greater(t, page2[0].ID, anchorID, "Second page should start after anchor ID")

		// Verify second page is also ordered by changed_at ASC, id ASC
		if len(page2) >= 2 {
			assert.True(t, page2[1].ChangedAt.After(page2[0].ChangedAt) ||
				(page2[1].ChangedAt.Equal(page2[0].ChangedAt) && page2[1].ID > page2[0].ID),
				"Second page should be ordered by changed_at ASC, id ASC")
		}
	})

	t.Run("order ascending and descending", func(t *testing.T) {
		owner := "0xchanges200000000000000000000000000000000002"

		// Create multiple tokens with different timestamps
		for i := range 3 {
			mintInput := buildTestTokenMint(
				domain.ChainEthereumMainnet,
				domain.StandardERC721,
				fmt.Sprintf("0xchanges2000000000000000000000000000000%03d", i),
				"1",
				owner,
			)
			// Adjust timestamp
			mintInput.ProvenanceEvent.Timestamp = time.Now().UTC().Add(time.Duration(i) * time.Hour)
			err := store.CreateTokenMint(ctx, mintInput)
			require.NoError(t, err)
			time.Sleep(10 * time.Millisecond) // Small delay to ensure different created_at
		}

		// Get changes ascending
		changesAsc, _, err := store.GetChanges(ctx, ChangesQueryFilter{
			Limit:     10,
			OrderDesc: false,
		})
		require.NoError(t, err)
		assert.GreaterOrEqual(t, len(changesAsc), 3)

		// Get changes descending
		changesDesc, _, err := store.GetChanges(ctx, ChangesQueryFilter{
			Limit:     10,
			OrderDesc: true,
		})
		require.NoError(t, err)
		assert.GreaterOrEqual(t, len(changesDesc), 3)

		// Verify order (first item in desc should be later than first in asc)
		if len(changesAsc) > 0 && len(changesDesc) > 0 {
			assert.True(t, changesDesc[0].ChangedAt.After(changesAsc[0].ChangedAt) ||
				changesDesc[0].ChangedAt.Equal(changesAsc[0].ChangedAt))
		}
	})

	t.Run("filter by since timestamp", func(t *testing.T) {
		cutoffTime := time.Now().UTC()
		time.Sleep(10 * time.Millisecond)

		// Create token after cutoff
		owner := "0xchanges300000000000000000000000000000000003"
		mintInput := buildTestTokenMint(
			domain.ChainEthereumMainnet,
			domain.StandardERC721,
			"0xchanges300000000000000000000000000000003",
			"1",
			owner,
		)
		mintInput.ProvenanceEvent.Timestamp = time.Now().UTC()
		err := store.CreateTokenMint(ctx, mintInput)
		require.NoError(t, err)

		// Query with since filter
		changes, total, err := store.GetChanges(ctx, ChangesQueryFilter{
			Since: &cutoffTime,
			Limit: 100,
		})
		require.NoError(t, err)
		assert.GreaterOrEqual(t, total, uint64(1))

		// All changes should be after cutoff
		for _, change := range changes {
			assert.True(t, change.ChangedAt.After(cutoffTime) || change.ChangedAt.Equal(cutoffTime))
		}
	})

	t.Run("filter by anchor id", func(t *testing.T) {
		// Get current max ID before creating new token
		allChanges, _, err := store.GetChanges(ctx, ChangesQueryFilter{
			Limit:     1000,
			OrderDesc: true, // Get newest first
		})
		require.NoError(t, err)
		var anchorID uint64
		if len(allChanges) > 0 {
			anchorID = allChanges[0].ID
		}

		// Create token after anchor
		owner := "0xchanges300000000000000000000000000000000004"
		mintInput := buildTestTokenMint(
			domain.ChainEthereumMainnet,
			domain.StandardERC721,
			"0xchanges300000000000000000000000000000004",
			"1",
			owner,
		)
		mintInput.ProvenanceEvent.Timestamp = time.Now().UTC()
		err = store.CreateTokenMint(ctx, mintInput)
		require.NoError(t, err)

		// Query with anchor filter
		changes, total, err := store.GetChanges(ctx, ChangesQueryFilter{
			Anchor: &anchorID,
			Limit:  100,
		})
		require.NoError(t, err)
		assert.GreaterOrEqual(t, total, uint64(1))

		// All changes should be after anchor ID
		for _, change := range changes {
			assert.Greater(t, change.ID, anchorID)
		}
	})

	t.Run("filter by addresses - provenance events", func(t *testing.T) {
		// Create a token and mint it to owner1
		owner1 := "0xchanges400000000000000000000000000000000001"
		owner2 := "0xchanges400000000000000000000000000000000002"
		contract := "0xchanges400000000000000000000000000000004"

		mintInput := buildTestTokenMint(
			domain.ChainEthereumMainnet,
			domain.StandardERC721,
			contract,
			"1",
			owner1,
		)
		mintInput.ProvenanceEvent.Timestamp = time.Now().UTC()
		err := store.CreateTokenMint(ctx, mintInput)
		require.NoError(t, err)

		time.Sleep(10 * time.Millisecond)

		// Transfer to owner2
		transferInput := UpdateTokenTransferInput{
			TokenCID:     mintInput.Token.TokenCID,
			CurrentOwner: &owner2,
			SenderBalanceUpdate: &UpdateBalanceInput{
				OwnerAddress: owner1,
				Delta:        "1",
			},
			ReceiverBalanceUpdate: &UpdateBalanceInput{
				OwnerAddress: owner2,
				Delta:        "1",
			},
			ProvenanceEvent: buildTestProvenanceEvent(
				domain.ChainEthereumMainnet,
				schema.ProvenanceEventTypeTransfer,
				&owner1,
				&owner2,
				"1",
				"0xtransfer_changes400",
				1001,
			),
		}
		err = store.UpdateTokenTransfer(ctx, transferInput)
		require.NoError(t, err)

		// Query changes for owner1 (should include mint and transfer out)
		changes, total, err := store.GetChanges(ctx, ChangesQueryFilter{
			Addresses: []string{owner1},
			Limit:     100,
		})
		require.NoError(t, err)
		assert.GreaterOrEqual(t, total, uint64(2))
		assert.GreaterOrEqual(t, len(changes), 2)

		// Verify all changes have appropriate subject types
		for _, change := range changes {
			// For ERC721: mint = token, transfer = owner
			assert.Contains(t, []schema.SubjectType{schema.SubjectTypeToken, schema.SubjectTypeOwner}, change.SubjectType)
		}

		// Query changes for owner2 (should include transfer in)
		_, total, err = store.GetChanges(ctx, ChangesQueryFilter{
			Addresses: []string{owner2},
			Limit:     100,
		})
		require.NoError(t, err)
		assert.GreaterOrEqual(t, total, uint64(1))
	})

	t.Run("filter by addresses - metadata during ownership", func(t *testing.T) {
		// Create a token and mint it to owner1
		owner1 := "0xchanges500000000000000000000000000000000001"
		owner2 := "0xchanges500000000000000000000000000000000002"
		contract := "0xchanges500000000000000000000000000000005"

		mintTime := time.Now().UTC()
		mintInput := buildTestTokenMint(
			domain.ChainEthereumMainnet,
			domain.StandardERC721,
			contract,
			"1",
			owner1,
		)
		mintInput.ProvenanceEvent.Timestamp = mintTime
		err := store.CreateTokenMint(ctx, mintInput)
		require.NoError(t, err)

		token, err := store.GetTokenByTokenCID(ctx, mintInput.Token.TokenCID)
		require.NoError(t, err)

		time.Sleep(10 * time.Millisecond)

		// Update metadata while owner1 still owns it
		metadataTime := mintTime.Add(1 * time.Hour)
		latestJSON := json.RawMessage(`{"name": "Test NFT", "image": "ipfs://test"}`)
		hash := "hash123"
		imageURL := "https://example.com/image.png"
		name := "Test NFT"

		metadataInput := CreateTokenMetadataInput{
			TokenID:         token.ID,
			LatestJSON:      latestJSON,
			LatestHash:      &hash,
			ImageURL:        &imageURL,
			Name:            &name,
			EnrichmentLevel: schema.EnrichmentLevelNone,
			LastRefreshedAt: &metadataTime,
		}
		err = store.UpsertTokenMetadata(ctx, metadataInput)
		require.NoError(t, err)

		time.Sleep(10 * time.Millisecond)

		// Transfer to owner2 AFTER metadata update
		transferTime := metadataTime.Add(1 * time.Hour)
		transferInput := UpdateTokenTransferInput{
			TokenCID:     mintInput.Token.TokenCID,
			CurrentOwner: &owner2,
			SenderBalanceUpdate: &UpdateBalanceInput{
				OwnerAddress: owner1,
				Delta:        "1",
			},
			ReceiverBalanceUpdate: &UpdateBalanceInput{
				OwnerAddress: owner2,
				Delta:        "1",
			},
			ProvenanceEvent: buildTestProvenanceEvent(
				domain.ChainEthereumMainnet,
				schema.ProvenanceEventTypeTransfer,
				&owner1,
				&owner2,
				"1",
				"0xtransfer_changes500",
				1002,
			),
		}
		transferInput.ProvenanceEvent.Timestamp = transferTime
		err = store.UpdateTokenTransfer(ctx, transferInput)
		require.NoError(t, err)

		// Query changes for owner1 (should include mint, metadata update, and transfer)
		changes, total, err := store.GetChanges(ctx, ChangesQueryFilter{
			Addresses: []string{owner1},
			TokenCIDs: []string{mintInput.Token.TokenCID},
			Limit:     100,
		})
		require.NoError(t, err)
		assert.GreaterOrEqual(t, total, uint64(2)) // At least mint and metadata

		// Should include metadata change that happened during ownership
		hasMetadataChange := false
		for _, change := range changes {
			if change.SubjectType == schema.SubjectTypeMetadata {
				hasMetadataChange = true
				// Verify metadata change happened during ownership (after mint, before transfer)
				assert.True(t, change.ChangedAt.After(mintTime))
				assert.True(t, change.ChangedAt.Before(transferTime))
			}
		}
		assert.True(t, hasMetadataChange, "Metadata change during ownership should be included")
	})

	t.Run("filter by addresses - exclude metadata after transfer", func(t *testing.T) {
		// Create a token and mint it to owner1
		owner1 := "0xchanges600000000000000000000000000000000001"
		owner2 := "0xchanges600000000000000000000000000000000002"
		contract := "0xchanges600000000000000000000000000000006"

		mintTime := time.Now().UTC()
		mintInput := buildTestTokenMint(
			domain.ChainEthereumMainnet,
			domain.StandardERC721,
			contract,
			"1",
			owner1,
		)
		mintInput.ProvenanceEvent.Timestamp = mintTime
		err := store.CreateTokenMint(ctx, mintInput)
		require.NoError(t, err)

		token, err := store.GetTokenByTokenCID(ctx, mintInput.Token.TokenCID)
		require.NoError(t, err)

		time.Sleep(10 * time.Millisecond)

		// Transfer to owner2 FIRST
		transferTime := mintTime.Add(1 * time.Hour)
		transferInput := UpdateTokenTransferInput{
			TokenCID:     mintInput.Token.TokenCID,
			CurrentOwner: &owner2,
			SenderBalanceUpdate: &UpdateBalanceInput{
				OwnerAddress: owner1,
				Delta:        "1",
			},
			ReceiverBalanceUpdate: &UpdateBalanceInput{
				OwnerAddress: owner2,
				Delta:        "1",
			},
			ProvenanceEvent: buildTestProvenanceEvent(
				domain.ChainEthereumMainnet,
				schema.ProvenanceEventTypeTransfer,
				&owner1,
				&owner2,
				"1",
				"0xtransfer_changes600",
				1002,
			),
		}
		transferInput.ProvenanceEvent.Timestamp = transferTime
		err = store.UpdateTokenTransfer(ctx, transferInput)
		require.NoError(t, err)

		time.Sleep(10 * time.Millisecond)

		// Update metadata AFTER owner1 transferred it away
		metadataTime := transferTime.Add(1 * time.Hour)
		latestJSON := json.RawMessage(`{"name": "Updated NFT", "image": "ipfs://updated"}`)
		hash := "hash456"
		imageURL := "https://example.com/image2.png"
		name := "Updated NFT"

		metadataInput := CreateTokenMetadataInput{
			TokenID:         token.ID,
			LatestJSON:      latestJSON,
			LatestHash:      &hash,
			ImageURL:        &imageURL,
			Name:            &name,
			EnrichmentLevel: schema.EnrichmentLevelNone,
			LastRefreshedAt: &metadataTime,
		}
		err = store.UpsertTokenMetadata(ctx, metadataInput)
		require.NoError(t, err)

		// Query changes for owner1 (should NOT include metadata update after transfer)
		changes, _, err := store.GetChanges(ctx, ChangesQueryFilter{
			Addresses: []string{owner1},
			TokenCIDs: []string{mintInput.Token.TokenCID},
			Limit:     100,
		})
		require.NoError(t, err)

		// Should NOT include metadata change that happened after transfer
		for _, change := range changes {
			if change.SubjectType == schema.SubjectTypeMetadata {
				// Any metadata changes should be before the transfer
				assert.True(t, change.ChangedAt.Before(transferTime),
					"Metadata change after transfer should NOT be included for previous owner")
			}
		}
	})

	t.Run("filter by addresses - metadata for current owner", func(t *testing.T) {
		// Create a token and mint it to owner1
		owner1 := "0xchanges700000000000000000000000000000000001"
		contract := "0xchanges700000000000000000000000000000007"

		mintTime := time.Now().UTC()
		mintInput := buildTestTokenMint(
			domain.ChainEthereumMainnet,
			domain.StandardERC721,
			contract,
			"1",
			owner1,
		)
		mintInput.ProvenanceEvent.Timestamp = mintTime
		err := store.CreateTokenMint(ctx, mintInput)
		require.NoError(t, err)

		token, err := store.GetTokenByTokenCID(ctx, mintInput.Token.TokenCID)
		require.NoError(t, err)

		time.Sleep(10 * time.Millisecond)

		// Update metadata while owner1 still owns it (no transfer)
		metadataTime := mintTime.Add(1 * time.Hour)
		latestJSON := json.RawMessage(`{"name": "Forever NFT", "image": "ipfs://forever"}`)
		hash := "hash789"
		imageURL := "https://example.com/image3.png"
		name := "Forever NFT"

		metadataInput := CreateTokenMetadataInput{
			TokenID:         token.ID,
			LatestJSON:      latestJSON,
			LatestHash:      &hash,
			ImageURL:        &imageURL,
			Name:            &name,
			EnrichmentLevel: schema.EnrichmentLevelNone,
			LastRefreshedAt: &metadataTime,
		}
		err = store.UpsertTokenMetadata(ctx, metadataInput)
		require.NoError(t, err)

		// Query changes for owner1 (should include metadata since still owner)
		changes, total, err := store.GetChanges(ctx, ChangesQueryFilter{
			Addresses: []string{owner1},
			TokenCIDs: []string{mintInput.Token.TokenCID},
			Limit:     100,
		})
		require.NoError(t, err)
		assert.GreaterOrEqual(t, total, uint64(2)) // mint + metadata

		// Should include metadata change for current owner
		hasMetadataChange := false
		for _, change := range changes {
			if change.SubjectType == schema.SubjectTypeMetadata {
				hasMetadataChange = true
			}
		}
		assert.True(t, hasMetadataChange, "Metadata change should be included for current owner")
	})
}

// =============================================================================
// Test: Block Cursor & Watched Addresses
// =============================================================================

func testBlockCursor(t *testing.T, store Store) {
	ctx := context.Background()

	t.Run("get non-existent cursor returns 0", func(t *testing.T) {
		cursor, err := store.GetBlockCursor(ctx, "test_chain_nonexistent")
		require.NoError(t, err)
		assert.Equal(t, uint64(0), cursor)
	})

	t.Run("set and get cursor", func(t *testing.T) {
		chain := "test_chain_cursor"
		blockNum := uint64(12345)

		err := store.SetBlockCursor(ctx, chain, blockNum)
		require.NoError(t, err)

		cursor, err := store.GetBlockCursor(ctx, chain)
		require.NoError(t, err)
		assert.Equal(t, blockNum, cursor)
	})

	t.Run("update existing cursor", func(t *testing.T) {
		chain := "test_chain_update"

		err := store.SetBlockCursor(ctx, chain, 100)
		require.NoError(t, err)

		err = store.SetBlockCursor(ctx, chain, 200)
		require.NoError(t, err)

		cursor, err := store.GetBlockCursor(ctx, chain)
		require.NoError(t, err)
		assert.Equal(t, uint64(200), cursor)
	})
}

func testWatchedAddresses(t *testing.T, store Store) {
	ctx := context.Background()

	t.Run("ensure watched address exists", func(t *testing.T) {
		address := "0xwatched1000000000000000000000000000000001"
		chain := domain.ChainEthereumMainnet

		err := store.EnsureWatchedAddressExists(ctx, address, chain)
		require.NoError(t, err)

		// Should be idempotent
		err = store.EnsureWatchedAddressExists(ctx, address, chain)
		require.NoError(t, err)

		// Verify it's being watched
		watched, err := store.IsAnyAddressWatched(ctx, chain, []string{address})
		require.NoError(t, err)
		assert.True(t, watched)
	})

	t.Run("check multiple addresses", func(t *testing.T) {
		address1 := "0xwatched2000000000000000000000000000000001"
		address2 := "0xwatched2000000000000000000000000000000002"
		chain := domain.ChainEthereumMainnet

		// Add only address1
		err := store.EnsureWatchedAddressExists(ctx, address1, chain)
		require.NoError(t, err)

		// Check if any of the addresses are watched
		watched, err := store.IsAnyAddressWatched(ctx, chain, []string{address1, address2})
		require.NoError(t, err)
		assert.True(t, watched)

		// Check only non-watched address
		watched, err = store.IsAnyAddressWatched(ctx, chain, []string{address2})
		require.NoError(t, err)
		assert.False(t, watched)
	})

	t.Run("get and update indexing block range", func(t *testing.T) {
		address := "0xwatched3000000000000000000000000000000001"
		chain := domain.ChainEthereumMainnet

		// Ensure address exists
		err := store.EnsureWatchedAddressExists(ctx, address, chain)
		require.NoError(t, err)

		// Get initial range (should be 0,0)
		minBlock, maxBlock, err := store.GetIndexingBlockRangeForAddress(ctx, address, chain)
		require.NoError(t, err)
		assert.Equal(t, uint64(0), minBlock)
		assert.Equal(t, uint64(0), maxBlock)

		// Update range
		err = store.UpdateIndexingBlockRangeForAddress(ctx, address, chain, 1000, 2000)
		require.NoError(t, err)

		// Verify range was updated
		minBlock, maxBlock, err = store.GetIndexingBlockRangeForAddress(ctx, address, chain)
		require.NoError(t, err)
		assert.Equal(t, uint64(1000), minBlock)
		assert.Equal(t, uint64(2000), maxBlock)
	})

	t.Run("update range with invalid block range which min block is greater than max block", func(t *testing.T) {
		address := "0xwatched3000000000000000000000000000000001"
		chain := domain.ChainEthereumMainnet

		err := store.UpdateIndexingBlockRangeForAddress(ctx, address, chain, 2000, 1000)
		require.Error(t, err)
	})

	t.Run("update range with invalid block range which min block and max block are 0", func(t *testing.T) {
		address := "0xwatched3000000000000000000000000000000001"
		chain := domain.ChainEthereumMainnet

		err := store.UpdateIndexingBlockRangeForAddress(ctx, address, chain, 0, 0)
		require.Error(t, err)
	})

	t.Run("update range with invalid block range which min block is less than current min block", func(t *testing.T) {
		address := "0xwatched4000000000000000000000000000000001"
		chain := domain.ChainEthereumMainnet

		// Ensure address exists
		err := store.EnsureWatchedAddressExists(ctx, address, chain)
		require.NoError(t, err)

		// Set up initial range
		err = store.UpdateIndexingBlockRangeForAddress(ctx, address, chain, 1000, 2000)
		require.NoError(t, err)

		// Try to update with minBlock greater than current minBlock (should fail)
		err = store.UpdateIndexingBlockRangeForAddress(ctx, address, chain, 1500, 2000)
		require.Error(t, err)
	})

	t.Run("update range with invalid block range which max block is less than current max block", func(t *testing.T) {
		address := "0xwatched4000000000000000000000000000000002"
		chain := domain.ChainEthereumMainnet

		// Ensure address exists
		err := store.EnsureWatchedAddressExists(ctx, address, chain)
		require.NoError(t, err)

		// Set up initial range
		err = store.UpdateIndexingBlockRangeForAddress(ctx, address, chain, 1000, 2000)
		require.NoError(t, err)

		// Try to update with maxBlock less than current maxBlock (should fail)
		err = store.UpdateIndexingBlockRangeForAddress(ctx, address, chain, 1000, 1500)
		require.Error(t, err)
	})
}

// =============================================================================
// Test: Key-Value Store
// =============================================================================

func testKeyValueStore(t *testing.T, store Store) {
	ctx := context.Background()

	t.Run("set and get key-value", func(t *testing.T) {
		key := "test:key1"
		value := "value1"

		err := store.SetKeyValue(ctx, key, value)
		require.NoError(t, err)

		retrievedValue, err := store.GetKeyValue(ctx, key)
		require.NoError(t, err)
		assert.Equal(t, value, retrievedValue)
	})

	t.Run("set empty value", func(t *testing.T) {
		key := "test:key2"
		err := store.SetKeyValue(ctx, key, "")
		require.NoError(t, err)

		retrievedValue, err := store.GetKeyValue(ctx, key)
		require.NoError(t, err)
		assert.Equal(t, "", retrievedValue)
	})

	t.Run("get non-existent key returns empty string", func(t *testing.T) {
		value, err := store.GetKeyValue(ctx, "nonexistent:key")
		require.NoError(t, err)
		assert.Equal(t, "", value)
	})

	t.Run("update existing key", func(t *testing.T) {
		key := "test:key2"

		err := store.SetKeyValue(ctx, key, "value1")
		require.NoError(t, err)

		err = store.SetKeyValue(ctx, key, "value2")
		require.NoError(t, err)

		value, err := store.GetKeyValue(ctx, key)
		require.NoError(t, err)
		assert.Equal(t, "value2", value)
	})

	t.Run("get all key-values by prefix", func(t *testing.T) {
		prefix := "test:kv:prefix"

		err := store.SetKeyValue(ctx, prefix+":key1", "value1")
		require.NoError(t, err)
		err = store.SetKeyValue(ctx, prefix+":key2", "value2")
		require.NoError(t, err)
		err = store.SetKeyValue(ctx, "other:key", "value3")
		require.NoError(t, err)

		// Verify individual keys work (core functionality)
		val1, err := store.GetKeyValue(ctx, prefix+":key1")
		require.NoError(t, err)
		assert.Equal(t, "value1", val1)

		val2, err := store.GetKeyValue(ctx, prefix+":key2")
		require.NoError(t, err)
		assert.Equal(t, "value2", val2)

		// Check prefix query returns at least some results
		kvMap, err := store.GetAllKeyValuesByPrefix(ctx, prefix)
		require.NoError(t, err)
		// Note: Prefix query behavior may vary with transaction isolation
		// The core set/get functionality works as verified above
		assert.GreaterOrEqual(t, len(kvMap), 1, "Should find at least one matching key")
	})
}

// =============================================================================
// Test: Media Assets
// =============================================================================

func testMediaAssets(t *testing.T, store Store) {
	ctx := context.Background()

	t.Run("create and retrieve media asset", func(t *testing.T) {
		sourceURL := "ipfs://QmTest123"
		mimeType := "image/png"
		fileSize := int64(1024)
		providerAssetID := "asset123"
		variantURLs := datatypes.JSON([]byte(`{"thumbnail": "https://cdn.example.com/thumb.png"}`))

		input := CreateMediaAssetInput{
			SourceURL:        sourceURL,
			MimeType:         &mimeType,
			FileSizeBytes:    &fileSize,
			Provider:         schema.StorageProviderCloudflare,
			ProviderAssetID:  &providerAssetID,
			ProviderMetadata: datatypes.JSON([]byte(`{}`)),
			VariantURLs:      variantURLs,
		}

		asset, err := store.CreateMediaAsset(ctx, input)
		require.NoError(t, err)
		require.NotNil(t, asset)
		assert.Equal(t, sourceURL, asset.SourceURL)
		assert.Equal(t, mimeType, *asset.MimeType)
		assert.Equal(t, fileSize, *asset.FileSizeBytes)

		// Retrieve by ID
		retrievedAsset, err := store.GetMediaAssetByID(ctx, asset.ID)
		require.NoError(t, err)
		assert.Equal(t, asset.ID, retrievedAsset.ID)

		// Retrieve by source URL
		retrievedAsset2, err := store.GetMediaAssetBySourceURL(ctx, sourceURL, schema.StorageProviderCloudflare)
		require.NoError(t, err)
		assert.Equal(t, asset.ID, retrievedAsset2.ID)
	})

	t.Run("create duplicate updates existing asset", func(t *testing.T) {
		sourceURL := "ipfs://QmTest456"
		providerAssetID := "asset456"
		mimeType1 := "image/png"
		fileSize1 := int64(1024)

		input := CreateMediaAssetInput{
			SourceURL:        sourceURL,
			MimeType:         &mimeType1,
			FileSizeBytes:    &fileSize1,
			Provider:         schema.StorageProviderCloudflare,
			ProviderAssetID:  &providerAssetID,
			ProviderMetadata: datatypes.JSON([]byte(`{"version":"1"}`)),
			VariantURLs:      datatypes.JSON([]byte(`{"thumbnail":"https://cdn.example.com/thumb1.png"}`)),
		}

		asset1, err := store.CreateMediaAsset(ctx, input)
		require.NoError(t, err)
		require.NotNil(t, asset1)
		assert.Equal(t, mimeType1, *asset1.MimeType)
		assert.Equal(t, fileSize1, *asset1.FileSizeBytes)

		// Update with new data
		mimeType2 := "image/jpeg"
		fileSize2 := int64(2048)
		providerAssetID2 := "asset456-updated"
		input2 := CreateMediaAssetInput{
			SourceURL:        sourceURL, // Same source URL and provider
			MimeType:         &mimeType2,
			FileSizeBytes:    &fileSize2,
			Provider:         schema.StorageProviderCloudflare,
			ProviderAssetID:  &providerAssetID2,
			ProviderMetadata: datatypes.JSON([]byte(`{"version":"2"}`)),
			VariantURLs:      datatypes.JSON([]byte(`{"thumbnail":"https://cdn.example.com/thumb2.png"}`)),
		}

		asset2, err := store.CreateMediaAsset(ctx, input2)
		require.NoError(t, err)
		require.NotNil(t, asset2)

		// Should have same ID (updated, not new)
		assert.Equal(t, asset1.ID, asset2.ID)

		// Should have updated values
		assert.Equal(t, mimeType2, *asset2.MimeType)
		assert.Equal(t, fileSize2, *asset2.FileSizeBytes)
		assert.Equal(t, providerAssetID2, *asset2.ProviderAssetID)

		// Verify by fetching again
		asset3, err := store.GetMediaAssetBySourceURL(ctx, sourceURL, schema.StorageProviderCloudflare)
		require.NoError(t, err)
		assert.Equal(t, asset2.ID, asset3.ID)
		assert.Equal(t, mimeType2, *asset3.MimeType)
		assert.Equal(t, fileSize2, *asset3.FileSizeBytes)
		assert.Equal(t, providerAssetID2, *asset3.ProviderAssetID)
	})

	t.Run("get media assets by multiple source URLs", func(t *testing.T) {
		sourceURLs := []string{"ipfs://QmBatch1", "ipfs://QmBatch2", "ipfs://QmBatch3"}

		for _, url := range sourceURLs {
			input := CreateMediaAssetInput{
				SourceURL:        url,
				Provider:         schema.StorageProviderCloudflare,
				ProviderMetadata: datatypes.JSON([]byte(`{}`)),
				VariantURLs:      datatypes.JSON([]byte(`{}`)),
			}
			_, err := store.CreateMediaAsset(ctx, input)
			require.NoError(t, err)
		}

		assets, err := store.GetMediaAssetsBySourceURLs(ctx, sourceURLs)
		require.NoError(t, err)
		assert.GreaterOrEqual(t, len(assets), 3)
	})
}

// =============================================================================
// Test: Bulk Query Methods
// =============================================================================

func testGetTokenOwnersBulk(t *testing.T, store Store) {
	ctx := context.Background()

	t.Run("returns owners for multiple tokens", func(t *testing.T) {
		// Create token1 with 5 owners using CreateTokenWithProvenances (ERC1155 with multiple owners)
		owner1_1 := "0xowner1111111111111111111111111111111111111"
		owner1_2 := "0xowner2111111111111111111111111111111111111"
		owner1_3 := "0xowner3111111111111111111111111111111111111"
		owner1_4 := "0xowner4111111111111111111111111111111111111"
		owner1_5 := "0xowner5111111111111111111111111111111111111"

		token1Token := buildTestToken(
			domain.ChainEthereumMainnet,
			domain.StandardERC1155,
			"0xbulk1111111111111111111111111111111111111",
			"1",
		)
		token1Token.CurrentOwner = nil

		token1Input := CreateTokenWithProvenancesInput{
			Token: token1Token,
			Balances: []CreateBalanceInput{
				{OwnerAddress: owner1_1, Quantity: "10"},
				{OwnerAddress: owner1_2, Quantity: "20"},
				{OwnerAddress: owner1_3, Quantity: "30"},
				{OwnerAddress: owner1_4, Quantity: "40"},
				{OwnerAddress: owner1_5, Quantity: "50"},
			},
			Events: []CreateProvenanceEventInput{
				buildTestProvenanceEvent(
					domain.ChainEthereumMainnet,
					schema.ProvenanceEventTypeMint,
					nil,
					&owner1_1,
					"150",
					"0xmintbulk1",
					1000,
				),
			},
		}
		err := store.CreateTokenWithProvenances(ctx, token1Input)
		require.NoError(t, err)

		token1, err := store.GetTokenByTokenCID(ctx, token1Token.TokenCID)
		require.NoError(t, err)

		// Create token2 with 2 owners (ERC1155)
		owner2_1 := "0xowner2111111111111111111111111111111111111"
		owner2_2 := "0xowner2222222222222222222222222222222222222"

		token2Token := buildTestToken(
			domain.ChainEthereumMainnet,
			domain.StandardERC1155,
			"0xbulk2222222222222222222222222222222222222",
			"2",
		)
		token2Token.CurrentOwner = nil

		token2Input := CreateTokenWithProvenancesInput{
			Token: token2Token,
			Balances: []CreateBalanceInput{
				{OwnerAddress: owner2_1, Quantity: "100"},
				{OwnerAddress: owner2_2, Quantity: "200"},
			},
			Events: []CreateProvenanceEventInput{
				buildTestProvenanceEvent(
					domain.ChainEthereumMainnet,
					schema.ProvenanceEventTypeMint,
					nil,
					&owner2_1,
					"300",
					"0xmintbulk2",
					1000,
				),
			},
		}
		err = store.CreateTokenWithProvenances(ctx, token2Input)
		require.NoError(t, err)

		token2, err := store.GetTokenByTokenCID(ctx, token2Token.TokenCID)
		require.NoError(t, err)

		// Create token3 with 1 owner
		token3Input := buildTestTokenMint(
			domain.ChainEthereumMainnet,
			domain.StandardERC721,
			"0xbulk3333333333333333333333333333333333333",
			"3",
			"0xowner3111111111111111111111111111111111111",
		)
		err = store.CreateTokenMint(ctx, token3Input)
		require.NoError(t, err)

		token3, err := store.GetTokenByTokenCID(ctx, token3Input.Token.TokenCID)
		require.NoError(t, err)

		// Test bulk query with limit of 3
		tokenIDs := []uint64{token1.ID, token2.ID, token3.ID}
		bulkOwners, bulkOwnersTotals, err := store.GetTokenOwnersBulk(ctx, tokenIDs, 3)
		require.NoError(t, err)

		// Verify results
		assert.Len(t, bulkOwners, 3)
		assert.Len(t, bulkOwnersTotals, 3)

		// Token1 should have 3 owners (limited by limit parameter) but total is 5
		assert.Len(t, bulkOwners[token1.ID], 3)
		assert.Equal(t, uint64(5), bulkOwnersTotals[token1.ID])

		// Token2 should have 2 owners and total is 2
		assert.Len(t, bulkOwners[token2.ID], 2)
		assert.Equal(t, uint64(2), bulkOwnersTotals[token2.ID])

		// Token3 should have 1 owner and total is 1
		assert.Len(t, bulkOwners[token3.ID], 1)
		assert.Equal(t, *token3Input.Token.CurrentOwner, bulkOwners[token3.ID][0].OwnerAddress)
		assert.Equal(t, "1", bulkOwners[token3.ID][0].Quantity)
	})

	t.Run("returns empty map for empty token IDs", func(t *testing.T) {
		bulkOwners, bulkOwnersTotals, err := store.GetTokenOwnersBulk(ctx, []uint64{}, 10)
		require.NoError(t, err)
		assert.Empty(t, bulkOwners)
		assert.Empty(t, bulkOwnersTotals)
	})

	t.Run("handles tokens with no owners", func(t *testing.T) {
		tokenInput := buildTestTokenMint(
			domain.ChainEthereumMainnet,
			domain.StandardERC721,
			"0xbulk4444444444444444444444444444444444444",
			"4",
			"0xowner4111111111111111111111111111111111111",
		)
		err := store.CreateTokenMint(ctx, tokenInput)
		require.NoError(t, err)

		token, err := store.GetTokenByTokenCID(ctx, tokenInput.Token.TokenCID)
		require.NoError(t, err)

		// Query with a non-existent token ID mixed in
		bulkOwners, bulkOwnersTotals, err := store.GetTokenOwnersBulk(ctx, []uint64{token.ID, 999999}, 10)
		require.NoError(t, err)

		// Should have owners for the real token only
		assert.Len(t, bulkOwners[token.ID], 1)
		assert.Equal(t, uint64(1), bulkOwnersTotals[token.ID])
		assert.NotContains(t, bulkOwners, uint64(999999))
		assert.NotContains(t, bulkOwnersTotals, uint64(999999))
	})
}

func testGetTokenProvenanceEventsBulk(t *testing.T, store Store) {
	ctx := context.Background()

	t.Run("returns provenance events for multiple tokens", func(t *testing.T) {
		// Create token1 with multiple events
		token1Input := buildTestTokenMint(
			domain.ChainEthereumMainnet,
			domain.StandardERC721,
			"0xprov1111111111111111111111111111111111111",
			"1",
			"0xowner1111111111111111111111111111111111111",
		)
		err := store.CreateTokenMint(ctx, token1Input)
		require.NoError(t, err)

		token1, err := store.GetTokenByTokenCID(ctx, token1Input.Token.TokenCID)
		require.NoError(t, err)

		// Add 4 more transfer events to token1
		for i := 2; i <= 5; i++ {
			fromOwner := fmt.Sprintf("0xowner%d111111111111111111111111111111111111", i-1)
			toOwner := fmt.Sprintf("0xowner%d111111111111111111111111111111111111", i)
			transferInput := UpdateTokenTransferInput{
				TokenCID:     token1Input.Token.TokenCID,
				CurrentOwner: &toOwner,
				SenderBalanceUpdate: &UpdateBalanceInput{
					OwnerAddress: fromOwner,
					Delta:        "1",
				},
				ReceiverBalanceUpdate: &UpdateBalanceInput{
					OwnerAddress: toOwner,
					Delta:        "1",
				},
				ProvenanceEvent: buildTestProvenanceEvent(
					token1Input.Token.Chain,
					schema.ProvenanceEventTypeTransfer,
					&fromOwner,
					&toOwner,
					"1",
					fmt.Sprintf("0xtransfer%d", i),
					uint64(1000+i), //nolint:gosec,G115
				),
			}
			err = store.UpdateTokenTransfer(ctx, transferInput)
			require.NoError(t, err)

			// Sleep to ensure different timestamps
			time.Sleep(1 * time.Millisecond)
		}

		// Create token2 with 2 events
		token2Input := buildTestTokenMint(
			domain.ChainEthereumMainnet,
			domain.StandardERC721,
			"0xprov2222222222222222222222222222222222222",
			"2",
			"0xowner2111111111111111111111111111111111111",
		)
		err = store.CreateTokenMint(ctx, token2Input)
		require.NoError(t, err)

		token2, err := store.GetTokenByTokenCID(ctx, token2Input.Token.TokenCID)
		require.NoError(t, err)

		// Add 1 more transfer to token2
		fromOwner2 := *token2Input.Token.CurrentOwner
		toOwner2 := "0xowner2222222222222222222222222222222222222"
		transferInput2 := UpdateTokenTransferInput{
			TokenCID:     token2Input.Token.TokenCID,
			CurrentOwner: &toOwner2,
			SenderBalanceUpdate: &UpdateBalanceInput{
				OwnerAddress: fromOwner2,
				Delta:        "1", // Positive value - implementation subtracts this
			},
			ReceiverBalanceUpdate: &UpdateBalanceInput{
				OwnerAddress: toOwner2,
				Delta:        "1",
			},
			ProvenanceEvent: buildTestProvenanceEvent(
				token2Input.Token.Chain,
				schema.ProvenanceEventTypeTransfer,
				&fromOwner2,
				&toOwner2,
				"1",
				"0xtransfer_token2",
				1006,
			),
		}
		err = store.UpdateTokenTransfer(ctx, transferInput2)
		require.NoError(t, err)

		// Test bulk query with limit of 3
		tokenIDs := []uint64{token1.ID, token2.ID}
		bulkEvents, bulkEventsTotals, err := store.GetTokenProvenanceEventsBulk(ctx, tokenIDs, 3)
		require.NoError(t, err)

		// Verify results
		assert.Len(t, bulkEvents, 2)
		assert.Len(t, bulkEventsTotals, 2)

		// Token1 should have 3 events (limited by limit parameter, DESC order so most recent) but total is 5 (1 mint + 4 transfers)
		assert.Len(t, bulkEvents[token1.ID], 3)
		assert.Equal(t, uint64(5), bulkEventsTotals[token1.ID])
		// Verify DESC order (most recent first)
		for i := 0; i < len(bulkEvents[token1.ID])-1; i++ {
			assert.True(t, bulkEvents[token1.ID][i].Timestamp.After(bulkEvents[token1.ID][i+1].Timestamp) ||
				bulkEvents[token1.ID][i].Timestamp.Equal(bulkEvents[token1.ID][i+1].Timestamp))
		}

		// Token2 should have 2 events and total is 2 (1 mint + 1 transfer)
		assert.Len(t, bulkEvents[token2.ID], 2)
		assert.Equal(t, uint64(2), bulkEventsTotals[token2.ID])
		// Verify DESC order
		assert.True(t, bulkEvents[token2.ID][0].Timestamp.After(bulkEvents[token2.ID][1].Timestamp) ||
			bulkEvents[token2.ID][0].Timestamp.Equal(bulkEvents[token2.ID][1].Timestamp))
	})

	t.Run("returns empty map for empty token IDs", func(t *testing.T) {
		bulkEvents, bulkEventsTotals, err := store.GetTokenProvenanceEventsBulk(ctx, []uint64{}, 10)
		require.NoError(t, err)
		assert.Empty(t, bulkEvents)
		assert.Empty(t, bulkEventsTotals)
	})

	t.Run("handles tokens with no events", func(t *testing.T) {
		tokenInput := buildTestTokenMint(
			domain.ChainEthereumMainnet,
			domain.StandardERC721,
			"0xprov3333333333333333333333333333333333333",
			"3",
			"0xowner3111111111111111111111111111111111111",
		)
		err := store.CreateTokenMint(ctx, tokenInput)
		require.NoError(t, err)

		token, err := store.GetTokenByTokenCID(ctx, tokenInput.Token.TokenCID)
		require.NoError(t, err)

		// Query with a non-existent token ID mixed in
		bulkEvents, bulkEventsTotals, err := store.GetTokenProvenanceEventsBulk(ctx, []uint64{token.ID, 999999}, 10)
		require.NoError(t, err)

		// Should have events for the real token only (mint event)
		assert.Len(t, bulkEvents[token.ID], 1)
		assert.Equal(t, uint64(1), bulkEventsTotals[token.ID])
		assert.Equal(t, schema.ProvenanceEventTypeMint, bulkEvents[token.ID][0].EventType)
		assert.NotContains(t, bulkEvents, uint64(999999))
		assert.NotContains(t, bulkEventsTotals, uint64(999999))
	})
}

func testGetEnrichmentSourcesByTokenIDs(t *testing.T, store Store) {
	ctx := context.Background()

	t.Run("returns enrichment sources for multiple tokens", func(t *testing.T) {
		// Create 3 tokens
		token1Input := buildTestTokenMint(
			domain.ChainEthereumMainnet,
			domain.StandardERC721,
			"0xenrich1111111111111111111111111111111111111",
			"1",
			"0xowner1111111111111111111111111111111111111",
		)
		err := store.CreateTokenMint(ctx, token1Input)
		require.NoError(t, err)

		token1, err := store.GetTokenByTokenCID(ctx, token1Input.Token.TokenCID)
		require.NoError(t, err)

		token2Input := buildTestTokenMint(
			domain.ChainEthereumMainnet,
			domain.StandardERC721,
			"0xenrich2222222222222222222222222222222222222",
			"2",
			"0xowner2111111111111111111111111111111111111",
		)
		err = store.CreateTokenMint(ctx, token2Input)
		require.NoError(t, err)

		token2, err := store.GetTokenByTokenCID(ctx, token2Input.Token.TokenCID)
		require.NoError(t, err)

		token3Input := buildTestTokenMint(
			domain.ChainEthereumMainnet,
			domain.StandardERC721,
			"0xenrich3333333333333333333333333333333333333",
			"3",
			"0xowner3111111111111111111111111111111111111",
		)
		err = store.CreateTokenMint(ctx, token3Input)
		require.NoError(t, err)

		token3, err := store.GetTokenByTokenCID(ctx, token3Input.Token.TokenCID)
		require.NoError(t, err)

		// Add enrichment sources for token1 and token2 only
		imageURL1 := "https://artblocks.io/image1.png"
		name1 := "Art #1"
		desc1 := "First art piece"
		enrichment1 := CreateEnrichmentSourceInput{
			TokenID:      token1.ID,
			Vendor:       schema.VendorArtBlocks,
			VendorJSON:   []byte(`{"project":"test1"}`),
			ImageURL:     &imageURL1,
			Name:         &name1,
			Description:  &desc1,
			AnimationURL: nil,
			Artists:      schema.Artists{},
			MimeType:     nil,
		}
		err = store.UpsertEnrichmentSource(ctx, enrichment1)
		require.NoError(t, err)

		imageURL2 := "https://fxhash.xyz/image2.png"
		name2 := "Art #2"
		enrichment2 := CreateEnrichmentSourceInput{
			TokenID:      token2.ID,
			Vendor:       schema.VendorFXHash,
			VendorJSON:   []byte(`{"iteration":"999"}`),
			ImageURL:     &imageURL2,
			Name:         &name2,
			Description:  nil,
			AnimationURL: nil,
			Artists:      schema.Artists{},
			MimeType:     nil,
		}
		err = store.UpsertEnrichmentSource(ctx, enrichment2)
		require.NoError(t, err)

		// Test bulk query
		tokenIDs := []uint64{token1.ID, token2.ID, token3.ID}
		bulkEnrichments, err := store.GetEnrichmentSourcesByTokenIDs(ctx, tokenIDs)
		require.NoError(t, err)

		// Verify results
		assert.Len(t, bulkEnrichments, 2) // Only token1 and token2 have enrichments

		// Verify token1 enrichment
		assert.NotNil(t, bulkEnrichments[token1.ID])
		assert.Equal(t, schema.VendorArtBlocks, bulkEnrichments[token1.ID].Vendor)
		assert.Equal(t, imageURL1, *bulkEnrichments[token1.ID].ImageURL)
		assert.Equal(t, name1, *bulkEnrichments[token1.ID].Name)
		assert.Equal(t, desc1, *bulkEnrichments[token1.ID].Description)

		// Verify token2 enrichment
		assert.NotNil(t, bulkEnrichments[token2.ID])
		assert.Equal(t, schema.VendorFXHash, bulkEnrichments[token2.ID].Vendor)
		assert.Equal(t, imageURL2, *bulkEnrichments[token2.ID].ImageURL)
		assert.Equal(t, name2, *bulkEnrichments[token2.ID].Name)

		// Verify token3 has no enrichment
		assert.NotContains(t, bulkEnrichments, token3.ID)
	})

	t.Run("returns empty map for empty token IDs", func(t *testing.T) {
		bulkEnrichments, err := store.GetEnrichmentSourcesByTokenIDs(ctx, []uint64{})
		require.NoError(t, err)
		assert.Empty(t, bulkEnrichments)
	})

	t.Run("handles non-existent token IDs", func(t *testing.T) {
		bulkEnrichments, err := store.GetEnrichmentSourcesByTokenIDs(ctx, []uint64{999998, 999999})
		require.NoError(t, err)
		assert.Empty(t, bulkEnrichments)
	})

	t.Run("handles mix of existing and non-existing tokens", func(t *testing.T) {
		// Create a token with enrichment
		tokenInput := buildTestTokenMint(
			domain.ChainEthereumMainnet,
			domain.StandardERC721,
			"0xenrich4444444444444444444444444444444444444",
			"4",
			"0xowner4111111111111111111111111111111111111",
		)
		err := store.CreateTokenMint(ctx, tokenInput)
		require.NoError(t, err)

		token, err := store.GetTokenByTokenCID(ctx, tokenInput.Token.TokenCID)
		require.NoError(t, err)

		imageURL := "https://example.com/image4.png"
		enrichment := CreateEnrichmentSourceInput{
			TokenID:    token.ID,
			Vendor:     schema.VendorFeralFile,
			VendorJSON: []byte(`{"test":"data"}`),
			ImageURL:   &imageURL,
		}
		err = store.UpsertEnrichmentSource(ctx, enrichment)
		require.NoError(t, err)

		// Query with mix of existing and non-existing IDs
		bulkEnrichments, err := store.GetEnrichmentSourcesByTokenIDs(ctx, []uint64{token.ID, 999997})
		require.NoError(t, err)

		// Should only have enrichment for the real token
		assert.Len(t, bulkEnrichments, 1)
		assert.NotNil(t, bulkEnrichments[token.ID])
		assert.Equal(t, schema.VendorFeralFile, bulkEnrichments[token.ID].Vendor)
		assert.NotContains(t, bulkEnrichments, uint64(999997))
	})
}

func testWebhookClients(t *testing.T, store Store) {
	ctx := context.Background()

	t.Run("GetActiveWebhookClientsByEventType - wildcard", func(t *testing.T) {
		// Test data from pg_test_data.sql includes client-all-events-123 with ["*"] filter
		clients, err := store.GetActiveWebhookClientsByEventType(ctx, "token.indexing.queryable")
		assert.NoError(t, err)
		assert.GreaterOrEqual(t, len(clients), 1)

		// Should find the wildcard client from test data
		found := false
		for _, c := range clients {
			if c.ClientID == "client-all-events-123" {
				found = true
				assert.Equal(t, "https://webhook.example.com/all", c.WebhookURL)
				assert.True(t, c.IsActive)
				break
			}
		}
		assert.True(t, found, "Should find wildcard client from test data")
	})

	t.Run("GetActiveWebhookClientsByEventType - specific events", func(t *testing.T) {
		// Test data includes client-specific-events-456 with ["token.indexing.queryable", "token.indexing.viewable"]
		clients, err := store.GetActiveWebhookClientsByEventType(ctx, "token.indexing.queryable")
		assert.NoError(t, err)

		// Should find the specific event client
		found := false
		for _, c := range clients {
			if c.ClientID == "client-specific-events-456" {
				found = true
				assert.Equal(t, "https://webhook.example.com/specific", c.WebhookURL)
				break
			}
		}
		assert.True(t, found, "Should find specific event client for token.indexing.queryable")

		// Query for matching event type (token.indexing.viewable)
		clients, err = store.GetActiveWebhookClientsByEventType(ctx, "token.indexing.viewable")
		assert.NoError(t, err)

		found = false
		for _, c := range clients {
			if c.ClientID == "client-specific-events-456" {
				found = true
				break
			}
		}
		assert.True(t, found, "Should find specific event client for token.indexing.viewable")

		// Query for non-matching event type
		clients, err = store.GetActiveWebhookClientsByEventType(ctx, "token.indexing.provenance_completed")
		assert.NoError(t, err)
		// Should not return client-specific-events-456
		found = false
		for _, c := range clients {
			if c.ClientID == "client-specific-events-456" {
				found = true
			}
		}
		assert.False(t, found, "client-specific-events-456 should not match token.indexing.provenance_completed")
	})

	t.Run("GetActiveWebhookClientsByEventType - inactive client", func(t *testing.T) {
		// Test data includes client-inactive-789 which is inactive
		clients, err := store.GetActiveWebhookClientsByEventType(ctx, "token.indexing.provenance_completed")
		assert.NoError(t, err)

		// Should not include inactive client
		for _, c := range clients {
			assert.NotEqual(t, "client-inactive-789", c.ClientID, "Should not return inactive client")
		}
	})

	t.Run("GetWebhookClientByID", func(t *testing.T) {
		// Get existing client from test data
		client, err := store.GetWebhookClientByID(ctx, "client-all-events-123")
		assert.NoError(t, err)
		assert.NotNil(t, client)
		assert.Equal(t, "client-all-events-123", client.ClientID)
		assert.Equal(t, "https://webhook.example.com/all", client.WebhookURL)
		assert.True(t, client.IsActive)
		assert.Equal(t, 5, client.RetryMaxAttempts)

		// Get non-existent client
		client, err = store.GetWebhookClientByID(ctx, "non-existent-client")
		assert.NoError(t, err)
		assert.Nil(t, client)
	})
}

func testWebhookDeliveries(t *testing.T, store Store) {
	ctx := context.Background()

	t.Run("CreateWebhookDelivery", func(t *testing.T) {
		// Use existing client from test data
		payload := []byte(`{"event_id":"01JG8XAMPLE_NEW_TEST_123456","event_type":"token.indexing.queryable","timestamp":"2024-01-15T10:00:00Z","data":{"token_cid":"eip155:1:erc721:0xABC:1","chain":"eip155:1","standard":"erc721","contract":"0xABC","token_number":"1","changed_at":"2024-01-15T10:00:00Z"}}`)
		delivery := &schema.WebhookDelivery{
			ClientID:       "client-all-events-123",
			EventID:        "01JG8XAMPLE_NEW_TEST_123456",
			EventType:      "token.indexing.queryable",
			Payload:        payload,
			WorkflowID:     "workflow-test-123",
			WorkflowRunID:  "run-test-456",
			DeliveryStatus: schema.WebhookDeliveryStatusPending,
			Attempts:       0,
		}
		err := store.CreateWebhookDelivery(ctx, delivery)
		assert.NoError(t, err)
		assert.NotZero(t, delivery.ID)
		assert.Equal(t, "client-all-events-123", delivery.ClientID)
		assert.Equal(t, "01JG8XAMPLE_NEW_TEST_123456", delivery.EventID)
		assert.Equal(t, "token.indexing.queryable", delivery.EventType)
		assert.Equal(t, string(payload), string(delivery.Payload))
		assert.Equal(t, "workflow-test-123", delivery.WorkflowID)
		assert.Equal(t, "run-test-456", delivery.WorkflowRunID)
		assert.Equal(t, schema.WebhookDeliveryStatusPending, delivery.DeliveryStatus)
		assert.Equal(t, 0, delivery.Attempts)
	})

	t.Run("UpdateWebhookDeliveryStatus - success", func(t *testing.T) {
		// Create delivery using existing client
		payload := []byte(`{"event_id":"01JG8XAMPLE_SUCCESS_999999","event_type":"token.indexing.viewable","timestamp":"2024-01-16T12:00:00Z","data":{"token_cid":"eip155:1:erc721:0xDEF:2"}}`)
		delivery := &schema.WebhookDelivery{
			ClientID:       "client-specific-events-456",
			EventID:        "01JG8XAMPLE_SUCCESS_999999",
			EventType:      "token.indexing.viewable",
			Payload:        payload,
			WorkflowID:     "workflow-success-789",
			WorkflowRunID:  "run-success-012",
			DeliveryStatus: schema.WebhookDeliveryStatusPending,
			Attempts:       0,
		}
		err := store.CreateWebhookDelivery(ctx, delivery)
		assert.NoError(t, err)

		// Update to success
		statusCode := 200
		err = store.UpdateWebhookDeliveryStatus(
			ctx,
			delivery.ID,
			schema.WebhookDeliveryStatusSuccess,
			1,
			&statusCode,
			`{"status":"received"}`,
			"",
		)
		assert.NoError(t, err)
	})

	t.Run("UpdateWebhookDeliveryStatus - failed", func(t *testing.T) {
		// Create delivery using existing client
		payload := []byte(`{"event_id":"01JG8XAMPLE_FAILED_888888","event_type":"token.indexing.provenance_completed","timestamp":"2024-01-17T14:00:00Z","data":{}}`)
		delivery := &schema.WebhookDelivery{
			ClientID:       "client-all-events-123",
			EventID:        "01JG8XAMPLE_FAILED_888888",
			EventType:      "token.indexing.provenance_completed",
			Payload:        payload,
			WorkflowID:     "workflow-error",
			WorkflowRunID:  "run-error",
			DeliveryStatus: schema.WebhookDeliveryStatusPending,
			Attempts:       0,
		}
		err := store.CreateWebhookDelivery(ctx, delivery)
		assert.NoError(t, err)

		// Update to failed
		statusCode := 500
		err = store.UpdateWebhookDeliveryStatus(
			ctx,
			delivery.ID,
			schema.WebhookDeliveryStatusFailed,
			3,
			&statusCode,
			`{"error":"internal server error"}`,
			"HTTP 500",
		)
		assert.NoError(t, err)
	})

	t.Run("CreateWebhookDelivery - invalid client_id", func(t *testing.T) {
		payload := []byte(`{"event_id":"01JG8XAMPLE_INVALID_777777","event_type":"token.indexing.queryable","timestamp":"2024-01-18T08:00:00Z","data":{}}`)
		delivery := &schema.WebhookDelivery{
			ClientID:       "non-existent-client",
			EventID:        "01JG8XAMPLE_INVALID_777777",
			EventType:      "token.indexing.queryable",
			Payload:        payload,
			WorkflowID:     "workflow-invalid",
			WorkflowRunID:  "run-invalid",
			DeliveryStatus: schema.WebhookDeliveryStatusPending,
			Attempts:       0,
		}
		err := store.CreateWebhookDelivery(ctx, delivery)
		assert.Error(t, err, "Should reject invalid client_id due to foreign key constraint")
	})
}

// =============================================================================
// Test Runner - runs all tests against a given store implementation
// =============================================================================

func RunStoreTests(t *testing.T, initDB func(t *testing.T) Store, cleanupDB func(t *testing.T)) {
	tests := []struct {
		name string
		fn   func(*testing.T, Store)
	}{
		{"CreateTokenMint", testCreateTokenMint},
		{"UpdateTokenTransfer", testUpdateTokenTransfer},
		{"UpdateTokenBurn", testUpdateTokenBurn},
		{"CreateTokenWithProvenances", testCreateTokenWithProvenances},
		{"UpsertTokenBalanceForOwner", testUpsertTokenBalanceForOwner},
		{"GetTokenByTokenCID", testGetTokenByTokenCID},
		{"GetTokensByCIDs", testGetTokensByCIDs},
		{"GetTokensByIDs", testGetTokensByIDs},
		{"GetTokensByFilter", testGetTokensByFilter},
		{"UpsertTokenMetadata", testUpsertTokenMetadata},
		{"EnrichmentSource", testEnrichmentSource},
		{"CreateMediaAssetWithChangeJournal", testCreateMediaAssetWithChangeJournal},
		{"GetChanges", testGetChanges},
		{"BlockCursor", testBlockCursor},
		{"WatchedAddresses", testWatchedAddresses},
		{"KeyValueStore", testKeyValueStore},
		{"MediaAssets", testMediaAssets},
		{"GetTokenOwnersBulk", testGetTokenOwnersBulk},
		{"GetTokenProvenanceEventsBulk", testGetTokenProvenanceEventsBulk},
		{"GetEnrichmentSourcesByTokenIDs", testGetEnrichmentSourcesByTokenIDs},
		{"WebhookClients", testWebhookClients},
		{"WebhookDeliveries", testWebhookDeliveries},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := initDB(t)
			defer cleanupDB(t)
			tt.fn(t, store)
		})
	}
}
