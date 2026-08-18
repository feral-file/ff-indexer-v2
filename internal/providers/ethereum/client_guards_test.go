package ethereum_test

import (
	"context"
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/mocks"
	ethereum "github.com/feral-file/ff-indexer-v2/internal/providers/ethereum"
	"github.com/feral-file/ff-indexer-v2/internal/providers/ethereum/adapters"
)

// TestOwnerBalanceAndEvents_FullProvenanceDisabledSkipsHistoryWalk pins the
// credit guard: with FullProvenanceDisabled, an ERC-1155 owner lookup is one
// balanceOf eth_call and no events — the mock has no FilterLogs expectation, so
// any attempt at the four full-range history walks fails the test. The walk is
// the single most expensive per-token operation on a span-capped provider.
func TestOwnerBalanceAndEvents_FullProvenanceDisabledSkipsHistoryWalk(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mockEth := mocks.NewMockEthClient(ctrl)
	mockClock := mocks.NewMockClock(ctrl)

	// balanceOf(owner, tokenId) -> 5, ABI-encoded as one 32-byte word.
	mockEth.EXPECT().
		CallContract(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(common.LeftPadBytes(big.NewInt(5).Bytes(), 32), nil)

	client, err := ethereum.NewGuardedClient(domain.ChainEthereumMainnet, mockEth, mockClock, nil, ethereum.ClientGuards{
		FullProvenanceDisabled: true,
	})
	require.NoError(t, err)

	balance, events, err := client.OwnerBalanceAndEvents(
		context.Background(),
		"0x00000000000000000000000000000000000000bb",
		"7",
		"0x00000000000000000000000000000000000000aa",
		domain.StandardERC1155,
	)
	require.NoError(t, err)
	require.Equal(t, "5", balance)
	require.Nil(t, events, "the guard must not replay owner history")
}

// TestOwnerBalanceAndEvents_GuardResolvesAdapterBeforeShortcut pins the guard's
// routing: the adapter registry is consulted BEFORE the balance-only shortcut,
// so configured contracts keep their adapter path and the registry's
// standard-mismatch validation still runs. CryptoPunks is a configured contract
// (derived standard erc721), so an erc1155 lookup must surface
// ErrConfiguredStandardMismatch — the strict mock has no CallContract
// expectation, proving the guard did not blindly fire balanceOf first.
func TestOwnerBalanceAndEvents_GuardResolvesAdapterBeforeShortcut(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mockEth := mocks.NewMockEthClient(ctrl)
	mockClock := mocks.NewMockClock(ctrl)

	client, err := ethereum.NewGuardedClient(domain.ChainEthereumMainnet, mockEth, mockClock, nil, ethereum.ClientGuards{
		FullProvenanceDisabled: true,
	})
	require.NoError(t, err)

	const cryptopunks = "0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb"
	_, _, err = client.OwnerBalanceAndEvents(
		context.Background(),
		cryptopunks,
		"7",
		"0x00000000000000000000000000000000000000aa",
		domain.StandardERC1155,
	)
	require.ErrorIs(t, err, adapters.ErrConfiguredStandardMismatch)
}
