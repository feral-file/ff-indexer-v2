package helpers_test

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	ethadapter "github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/mocks"
	"github.com/feral-file/ff-indexer-v2/internal/providers/ethereum/helpers"
)

// routedHelper is a pagination helper with a mocked vendor and a mocked
// warehouse, span-capped like production so vendor windows are predictable.
type routedHelper struct {
	vendor    *mocks.MockEthClient
	warehouse *mocks.MockLogWarehouse
	helper    *helpers.PaginationHelper
}

// newRoutedHelper builds a helper in vendor-fall-through mode (a warehouse
// failure degrades to the vendor). Strict-mode behavior — the production
// default — is exercised by newStrictRoutedHelper.
func newRoutedHelper(t *testing.T, spanCap uint64) routedHelper {
	return newRoutedHelperWithFallthrough(t, spanCap, true)
}

// newStrictRoutedHelper builds a helper in the default strict mode: a warehouse
// failure fails the query and never touches the vendor.
func newStrictRoutedHelper(t *testing.T, spanCap uint64) routedHelper {
	return newRoutedHelperWithFallthrough(t, spanCap, false)
}

func newRoutedHelperWithFallthrough(t *testing.T, spanCap uint64, fallthrough_ bool) routedHelper {
	t.Helper()
	ctrl := gomock.NewController(t)
	vendor := mocks.NewMockEthClient(ctrl)
	warehouse := mocks.NewMockLogWarehouse(ctrl)
	clock := mocks.NewMockClock(ctrl)
	clock.EXPECT().Sleep(gomock.Any()).AnyTimes()
	return routedHelper{
		vendor:    vendor,
		warehouse: warehouse,
		helper: helpers.NewGuardedPaginationHelper(vendor, clock, nil, helpers.PaginationGuards{
			SpanCap:                       spanCap,
			LogWarehouse:                  warehouse,
			LogWarehouseVendorFallthrough: fallthrough_,
		}),
	}
}

func rangeOf(q ethereum.FilterQuery) (uint64, uint64) {
	return q.FromBlock.Uint64(), q.ToBlock.Uint64()
}

func logAt(block uint64, index uint) types.Log {
	return types.Log{BlockNumber: block, Index: index, BlockTimestamp: 1_700_000_000 + block}
}

func transferQuery(from, to uint64) ethereum.FilterQuery {
	return ethereum.FilterQuery{
		FromBlock: new(big.Int).SetUint64(from),
		ToBlock:   new(big.Int).SetUint64(to),
		Topics:    [][]common.Hash{{helpers.TransferEventSignature}, nil, nil, {common.HexToHash("0x7")}},
	}
}

// TestFilterLogsWithPagination_WarehouseServesWholeRange pins the headline
// behavior: a range at or below the warehouse head is ONE warehouse call
// with the caller's filter unchanged, and the vendor is never asked — the
// strict vendor mock has no FilterLogs expectation.
func TestFilterLogsWithPagination_WarehouseServesWholeRange(t *testing.T) {
	t.Parallel()
	h := newRoutedHelper(t, 10_000)
	h.warehouse.EXPECT().Head(gomock.Any()).Return(uint64(25_000_000), nil)
	h.warehouse.EXPECT().FilterLogs(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, q ethereum.FilterQuery, _ *common.Hash) ([]types.Log, error) {
			from, to := rangeOf(q)
			require.Equal(t, uint64(0), from)
			require.Equal(t, uint64(24_000_000), to)
			require.Equal(t, [][]common.Hash{{helpers.TransferEventSignature}, nil, nil, {common.HexToHash("0x7")}}, q.Topics,
				"the filter must reach the warehouse unchanged")
			return []types.Log{logAt(5, 1), logAt(23_999_999, 0)}, nil
		})

	logs, err := h.helper.FilterLogsWithPagination(context.Background(), transferQuery(0, 24_000_000))
	require.NoError(t, err)
	require.Equal(t, []types.Log{logAt(5, 1), logAt(23_999_999, 0)}, logs)
}

// TestFilterLogsWithPagination_ERC1155IDReachesWarehouse pins that
// WithERC1155TokenID forwards the token id to the warehouse leg's FilterLogs
// and only there: the vendor is never asked (strict mock), so a node never
// sees the warehouse-only field.
func TestFilterLogsWithPagination_ERC1155IDReachesWarehouse(t *testing.T) {
	t.Parallel()
	h := newRoutedHelper(t, 10_000)
	want := common.BigToHash(big.NewInt(0x2a))
	h.warehouse.EXPECT().Head(gomock.Any()).Return(uint64(25_000_000), nil)
	h.warehouse.EXPECT().FilterLogs(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ ethereum.FilterQuery, id *common.Hash) ([]types.Log, error) {
			require.NotNil(t, id, "the token id must reach the warehouse leg")
			require.Equal(t, want, *id)
			return []types.Log{logAt(5, 1)}, nil
		})

	logs, err := h.helper.FilterLogsWithPagination(context.Background(), transferQuery(0, 24_000_000),
		helpers.WithERC1155TokenID(want))
	require.NoError(t, err)
	require.Equal(t, []types.Log{logAt(5, 1)}, logs)
}

// TestFilterLogsWithPagination_SplitsAtWarehouseHead pins the split: blocks up
// to the head come from the warehouse in one call, blocks above it from the
// vendor under the span cap, and the result is the concatenation in order.
func TestFilterLogsWithPagination_SplitsAtWarehouseHead(t *testing.T) {
	t.Parallel()
	h := newRoutedHelper(t, 10_000)
	const head = uint64(1_000_000)
	h.warehouse.EXPECT().Head(gomock.Any()).Return(head, nil)
	h.warehouse.EXPECT().FilterLogs(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, q ethereum.FilterQuery, _ *common.Hash) ([]types.Log, error) {
			from, to := rangeOf(q)
			require.Equal(t, uint64(0), from)
			require.Equal(t, head, to, "the warehouse leg ends exactly at the head")
			return []types.Log{logAt(10, 0), logAt(head, 3)}, nil
		})
	var vendorRanges []blockRange
	h.vendor.EXPECT().FilterLogs(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, q ethereum.FilterQuery) ([]types.Log, error) {
			from, to := rangeOf(q)
			vendorRanges = append(vendorRanges, blockRange{from, to})
			if from <= head+1 && head+1 <= to {
				return []types.Log{logAt(head+1, 0)}, nil
			}
			return nil, nil
		}).AnyTimes()

	logs, err := h.helper.FilterLogsWithPagination(context.Background(), transferQuery(0, head+25_000))
	require.NoError(t, err)
	require.Equal(t, []types.Log{logAt(10, 0), logAt(head, 3), logAt(head+1, 0)}, logs)
	requireContiguousCoverage(t, vendorRanges, head+1, head+25_000)
	for _, r := range vendorRanges {
		require.LessOrEqual(t, r.to-r.from, uint64(10_000), "vendor windows stay under the span cap")
	}
}

// TestFilterLogsWithPagination_AboveHeadSkipsWarehouseQuery pins the
// steady-state ingestion case: a range entirely above the head costs one
// head lookup and no warehouse eth_getLogs, then goes to the vendor.
func TestFilterLogsWithPagination_AboveHeadSkipsWarehouseQuery(t *testing.T) {
	t.Parallel()
	h := newRoutedHelper(t, 10_000)
	h.warehouse.EXPECT().Head(gomock.Any()).Return(uint64(100), nil)
	h.vendor.EXPECT().FilterLogs(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, q ethereum.FilterQuery) ([]types.Log, error) {
			from, to := rangeOf(q)
			require.Equal(t, blockRange{101, 110}, blockRange{from, to})
			return []types.Log{logAt(105, 0)}, nil
		})

	logs, err := h.helper.FilterLogsWithPagination(context.Background(), transferQuery(101, 110))
	require.NoError(t, err)
	require.Equal(t, []types.Log{logAt(105, 0)}, logs)
}

// TestFilterLogsWithPagination_FallsThroughOnWarehouseFailure pins the agreed
// failure policy: a head-lookup error, a scope refusal, and a transport
// error on eth_getLogs each hand the WHOLE range to the vendor, with no retry
// against the warehouse.
func TestFilterLogsWithPagination_FallsThroughOnWarehouseFailure(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name  string
		setup func(w *mocks.MockLogWarehouse)
	}{
		{"head lookup fails", func(w *mocks.MockLogWarehouse) {
			w.EXPECT().Head(gomock.Any()).Return(uint64(0), errors.New("dial tcp: connection refused"))
		}},
		{"scope refusal", func(w *mocks.MockLogWarehouse) {
			w.EXPECT().Head(gomock.Any()).Return(uint64(1_000), nil)
			w.EXPECT().FilterLogs(gomock.Any(), gomock.Any(), gomock.Any()).
				Return(nil, fmt.Errorf("%w: blocks 0-1000 extend below the warehouse coverage start 500", ethadapter.ErrOutOfScope))
		}},
		{"transport error on eth_getLogs", func(w *mocks.MockLogWarehouse) {
			w.EXPECT().Head(gomock.Any()).Return(uint64(1_000), nil)
			w.EXPECT().FilterLogs(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, context.DeadlineExceeded)
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newRoutedHelper(t, 10_000)
			tc.setup(h.warehouse)
			var vendorRanges []blockRange
			h.vendor.EXPECT().FilterLogs(gomock.Any(), gomock.Any()).
				DoAndReturn(func(_ context.Context, q ethereum.FilterQuery) ([]types.Log, error) {
					from, to := rangeOf(q)
					vendorRanges = append(vendorRanges, blockRange{from, to})
					return []types.Log{logAt(from, 0)}, nil
				}).AnyTimes()

			logs, err := h.helper.FilterLogsWithPagination(context.Background(), transferQuery(0, 1_000))
			require.NoError(t, err)
			requireContiguousCoverage(t, vendorRanges, 0, 1_000)
			require.NotEmpty(t, logs)
		})
	}
}

// TestFilterLogsWithPagination_StrictModeFailsInsteadOfFallingThrough pins the
// production default (LogWarehouseVendorFallthrough false): every warehouse
// failure — a head-lookup outage, a scope refusal, and a transport error on
// eth_getLogs — fails the whole query and the vendor is NEVER asked (strict
// mock, no FilterLogs expectation), so a warehouse outage cannot silently burn
// vendor credits on a genesis-to-head walk.
func TestFilterLogsWithPagination_StrictModeFailsInsteadOfFallingThrough(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name  string
		setup func(w *mocks.MockLogWarehouse)
	}{
		{"head lookup fails", func(w *mocks.MockLogWarehouse) {
			w.EXPECT().Head(gomock.Any()).Return(uint64(0), errors.New("dial tcp: connection refused"))
		}},
		{"scope refusal", func(w *mocks.MockLogWarehouse) {
			w.EXPECT().Head(gomock.Any()).Return(uint64(1_000), nil)
			w.EXPECT().FilterLogs(gomock.Any(), gomock.Any(), gomock.Any()).
				Return(nil, fmt.Errorf("%w: blocks 0-1000 extend below the warehouse coverage start 500", ethadapter.ErrOutOfScope))
		}},
		{"transport error on eth_getLogs", func(w *mocks.MockLogWarehouse) {
			w.EXPECT().Head(gomock.Any()).Return(uint64(1_000), nil)
			w.EXPECT().FilterLogs(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, errors.New("dial tcp: connection refused"))
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newStrictRoutedHelper(t, 10_000)
			tc.setup(h.warehouse)
			// The vendor mock has no FilterLogs expectation: any call fails the test.

			logs, err := h.helper.FilterLogsWithPagination(context.Background(), transferQuery(0, 1_000))
			require.Error(t, err)
			require.Nil(t, logs, "a failed warehouse leg in strict mode yields no logs, not a partial result")
			require.ErrorContains(t, err, "vendor fall-through disabled")
		})
	}
}

// TestFilterLogsWithPagination_StrictModeAboveHeadStillReachesVendor pins that
// strict mode does not block the NORMAL above-head split: blocks above the
// warehouse head are not a failure, so they still go to the vendor even with
// fall-through disabled.
func TestFilterLogsWithPagination_StrictModeAboveHeadStillReachesVendor(t *testing.T) {
	t.Parallel()
	h := newStrictRoutedHelper(t, 10_000)
	h.warehouse.EXPECT().Head(gomock.Any()).Return(uint64(100), nil)
	h.vendor.EXPECT().FilterLogs(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, q ethereum.FilterQuery) ([]types.Log, error) {
			from, to := rangeOf(q)
			require.Equal(t, blockRange{101, 110}, blockRange{from, to})
			return []types.Log{logAt(105, 0)}, nil
		})

	logs, err := h.helper.FilterLogsWithPagination(context.Background(), transferQuery(101, 110))
	require.NoError(t, err)
	require.Equal(t, []types.Log{logAt(105, 0)}, logs)
}

// TestFilterLogsWithPagination_StrictModeCancelledContextIsError pins that a
// caller cancellation is still returned as the context error in strict mode,
// not wrapped as a fall-through-disabled failure.
func TestFilterLogsWithPagination_StrictModeCancelledContextIsError(t *testing.T) {
	t.Parallel()
	h := newStrictRoutedHelper(t, 10_000)
	ctx, cancel := context.WithCancel(context.Background())
	h.warehouse.EXPECT().Head(gomock.Any()).DoAndReturn(func(context.Context) (uint64, error) {
		cancel()
		return 0, context.Canceled
	})

	_, err := h.helper.FilterLogsWithPagination(ctx, transferQuery(0, 1_000))
	require.ErrorIs(t, err, context.Canceled)
}

// TestFilterLogsWithPagination_WarehouseResultCapBisects pins that the
// warehouse's "query returned more than N results" is a split signal, not a
// fall-through: the range is bisected until each half fits, in order, with no
// vendor involvement and no sleeps.
func TestFilterLogsWithPagination_WarehouseResultCapBisects(t *testing.T) {
	t.Parallel()
	h := newRoutedHelper(t, 10_000)
	h.warehouse.EXPECT().Head(gomock.Any()).Return(uint64(1_000), nil)
	var served []blockRange
	h.warehouse.EXPECT().FilterLogs(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, q ethereum.FilterQuery, _ *common.Hash) ([]types.Log, error) {
			from, to := rangeOf(q)
			if to-from > 300 {
				return nil, errors.New("query returned more than 100000 results")
			}
			served = append(served, blockRange{from, to})
			return []types.Log{logAt(from, 0)}, nil
		}).AnyTimes()

	logs, err := h.helper.FilterLogsWithPagination(context.Background(), transferQuery(0, 1_000))
	require.NoError(t, err)
	requireContiguousCoverage(t, served, 0, 1_000)
	require.Len(t, logs, len(served))
	for i := 1; i < len(logs); i++ {
		require.Less(t, logs[i-1].BlockNumber, logs[i].BlockNumber, "halves are concatenated in block order")
	}
}

// TestFilterLogsWithPagination_WarehouseSingleBlockOverflowIsNotAFallThrough
// pins that a single warehouse block over the result cap surfaces as a
// SingleBlockOverflowError (the receipt-recovery signal) rather than being
// routed through the outage fall-through — in strict mode, where a generic
// wrapped error would otherwise hide the type and wedge ingestion on the dense
// block. The vendor is never asked (strict mock, no FilterLogs).
func TestFilterLogsWithPagination_WarehouseSingleBlockOverflowIsNotAFallThrough(t *testing.T) {
	t.Parallel()
	h := newStrictRoutedHelper(t, 10_000)
	h.warehouse.EXPECT().Head(gomock.Any()).Return(uint64(1_000), nil)
	// Every window is over the cap, so the bisection walks down to one block.
	h.warehouse.EXPECT().FilterLogs(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil, errors.New("query returned more than 100000 results")).AnyTimes()

	logs, err := h.helper.FilterLogsWithPagination(context.Background(), transferQuery(0, 1_000))
	require.Nil(t, logs)
	var overflow *helpers.SingleBlockOverflowError
	require.ErrorAs(t, err, &overflow, "a single warehouse block over the cap must stay a SingleBlockOverflowError")
	require.NotContains(t, err.Error(), "fall-through disabled",
		"a dense-block overflow is a receipts signal, not a warehouse outage")
}

// TestFilterLogsWithPagination_WarehouseCallsBypassCallBudget pins that the
// warehouse leg is not metered by the vendor call budget: with a budget of 1,
// a bisected warehouse fetch (several warehouse calls) still succeeds and the
// single residual vendor call fits the budget.
func TestFilterLogsWithPagination_WarehouseCallsBypassCallBudget(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	vendor := mocks.NewMockEthClient(ctrl)
	warehouse := mocks.NewMockLogWarehouse(ctrl)
	clock := mocks.NewMockClock(ctrl)
	helper := helpers.NewGuardedPaginationHelper(vendor, clock, nil, helpers.PaginationGuards{
		SpanCap:      10_000,
		CallBudget:   1,
		LogWarehouse: warehouse,
	})
	warehouse.EXPECT().Head(gomock.Any()).Return(uint64(999), nil)
	warehouse.EXPECT().FilterLogs(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, q ethereum.FilterQuery, _ *common.Hash) ([]types.Log, error) {
			from, to := rangeOf(q)
			if to-from == 999 {
				return nil, errors.New("query returned more than 100000 results")
			}
			return nil, nil
		}).Times(3)
	vendor.EXPECT().FilterLogs(gomock.Any(), gomock.Any()).Return(nil, nil).Times(1)

	_, err := helper.FilterLogsWithPagination(context.Background(), transferQuery(0, 1_000))
	require.NoError(t, err)
}

// TestFilterLogsWithPagination_CancelledContextIsNotAFallThrough pins that a
// caller cancellation surfacing from the warehouse leg is returned as such —
// it must not turn into a vendor walk that would fail the same way.
func TestFilterLogsWithPagination_CancelledContextIsNotAFallThrough(t *testing.T) {
	t.Parallel()
	h := newRoutedHelper(t, 10_000)
	ctx, cancel := context.WithCancel(context.Background())
	h.warehouse.EXPECT().Head(gomock.Any()).DoAndReturn(func(context.Context) (uint64, error) {
		cancel()
		return 0, context.Canceled
	})

	_, err := h.helper.FilterLogsWithPagination(ctx, transferQuery(0, 1_000))
	require.ErrorIs(t, err, context.Canceled)
}

// TestFilterLogsWithPagination_BlockHashQueryBypassesWarehouse pins that a
// block-hash query stays a single vendor call: the warehouse is never asked
// (strict mock, no expectations).
func TestFilterLogsWithPagination_BlockHashQueryBypassesWarehouse(t *testing.T) {
	t.Parallel()
	h := newRoutedHelper(t, 10_000)
	hash := common.HexToHash("0xabc")
	h.vendor.EXPECT().FilterLogs(gomock.Any(), gomock.Any()).Return([]types.Log{logAt(1, 0)}, nil)

	logs, err := h.helper.FilterLogsWithPagination(context.Background(), ethereum.FilterQuery{BlockHash: &hash})
	require.NoError(t, err)
	require.Len(t, logs, 1)
}

// TestFilterLogsWithPagination_VendorFailureDropsWarehousePrefix pins the
// completeness invariant: when the residual vendor leg fails for a reason
// other than the walk deadline, the caller gets no logs at all rather than
// the warehouse-served prefix masquerading as the full history.
func TestFilterLogsWithPagination_VendorFailureDropsWarehousePrefix(t *testing.T) {
	t.Parallel()
	h := newRoutedHelper(t, 10_000)
	h.warehouse.EXPECT().Head(gomock.Any()).Return(uint64(100), nil)
	h.warehouse.EXPECT().FilterLogs(gomock.Any(), gomock.Any(), gomock.Any()).Return([]types.Log{logAt(1, 0)}, nil)
	h.vendor.EXPECT().FilterLogs(gomock.Any(), gomock.Any()).Return(nil, errors.New("execution aborted"))

	logs, err := h.helper.FilterLogsWithPagination(context.Background(), transferQuery(0, 200))
	require.Error(t, err)
	require.Nil(t, logs)
}
