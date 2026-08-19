package adapters

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/stretchr/testify/require"

	ethadapter "github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/providers/ethereum/helpers"
)

// fanoutFakeEthClient is a hand-rolled fake: internal/mocks imports this package
// (ethereum_provider_client.go), so package-internal tests cannot use it without
// an import cycle. Only FilterLogs is implemented; other EthClient methods panic
// via the nil embedded interface, which is the strictness this test wants.
type fanoutFakeEthClient struct {
	ethadapter.EthClient
	filterLogs func(ctx context.Context, query ethereum.FilterQuery) ([]types.Log, error)
}

func (f *fanoutFakeEthClient) FilterLogs(ctx context.Context, query ethereum.FilterQuery) ([]types.Log, error) {
	return f.filterLogs(ctx, query)
}

// TestFilterLogsInParallel_FirstErrorCancelsSiblings pins the credit-guard
// containment: when one concurrent walk fails (e.g. a call-budget abort), the
// sibling walks must observe context cancellation instead of continuing to
// spend RPC credits under the still-live parent context until their own
// budgets or deadlines.
//
// Sequencing is deterministic: the failing walk waits until the sibling is
// provably inside FilterLogs before returning its error, so the sibling can
// only finish via the cancellation this test asserts.
func TestFilterLogsInParallel_FirstErrorCancelsSiblings(t *testing.T) {
	t.Parallel()

	failingTopic := common.HexToHash("0x00000000000000000000000000000000000000000000000000000000000000f1")
	blockedTopic := common.HexToHash("0x00000000000000000000000000000000000000000000000000000000000000b1")

	siblingStarted := make(chan struct{})
	siblingCancelled := make(chan struct{})
	walkErr := errors.New("call budget exhausted (simulated)")

	fakeClient := &fanoutFakeEthClient{
		filterLogs: func(ctx context.Context, query ethereum.FilterQuery) ([]types.Log, error) {
			switch query.Topics[0][0] {
			case failingTopic:
				<-siblingStarted // guarantee the sibling is in-flight before failing
				return nil, walkErr
			case blockedTopic:
				close(siblingStarted)
				select {
				case <-ctx.Done():
					close(siblingCancelled)
					return nil, ctx.Err()
				case <-time.After(10 * time.Second):
					return nil, nil
				}
			default:
				t.Errorf("unexpected query topic %v", query.Topics[0][0])
				return nil, nil
			}
		},
	}

	pagination := helpers.NewPaginationHelper(fakeClient, ethadapter.NewClock(), nil)
	queries := []ethereum.FilterQuery{
		{
			FromBlock: common.Big0,
			ToBlock:   common.Big1,
			Topics:    [][]common.Hash{{failingTopic}},
		},
		{
			FromBlock: common.Big0,
			ToBlock:   common.Big1,
			Topics:    [][]common.Hash{{blockedTopic}},
		},
	}

	logs, err := filterLogsInParallel(context.Background(), pagination, queries)
	require.ErrorIs(t, err, walkErr)
	require.Nil(t, logs)

	select {
	case <-siblingCancelled:
	case <-time.After(5 * time.Second):
		t.Fatal("sibling walk was not canceled after the first walk failed")
	}
}
