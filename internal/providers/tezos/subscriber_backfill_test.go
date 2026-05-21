package tezos

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/domain"
)

type fakeBackfillTzKTClient struct {
	head              uint64
	headErr           error
	transferPages     map[int][]TzKTTokenTransfer
	bigmapPages       map[int][]TzKTBigMapUpdate
	transferPageErr   error
	bigmapPageErr     error
	parseTransfer     func(*TzKTTokenTransfer) (*domain.BlockchainEvent, error)
	parseBigMapUpdate func(*TzKTBigMapUpdate) (*domain.BlockchainEvent, error)
}

func (f *fakeBackfillTzKTClient) ParseTransfer(_ context.Context, transfer *TzKTTokenTransfer) (*domain.BlockchainEvent, error) {
	if f.parseTransfer != nil {
		return f.parseTransfer(transfer)
	}
	return &domain.BlockchainEvent{BlockNumber: transfer.Level, EventType: domain.EventTypeTransfer}, nil
}

func (f *fakeBackfillTzKTClient) ParseBigMapUpdate(_ context.Context, update *TzKTBigMapUpdate) (*domain.BlockchainEvent, error) {
	if f.parseBigMapUpdate != nil {
		return f.parseBigMapUpdate(update)
	}
	return &domain.BlockchainEvent{BlockNumber: update.Level, EventType: domain.EventTypeMetadataUpdate}, nil
}

func (f *fakeBackfillTzKTClient) GetTransactionsByID(context.Context, uint64) ([]TzKTTransaction, error) {
	return nil, nil
}

func (f *fakeBackfillTzKTClient) GetTokenOwnerBalance(context.Context, string, string, string) (string, error) {
	return "", nil
}

func (f *fakeBackfillTzKTClient) GetTokenBalances(context.Context, string, string) ([]TzKTTokenBalance, error) {
	return nil, nil
}

func (f *fakeBackfillTzKTClient) GetTokenMetadata(context.Context, string, string) (map[string]interface{}, error) {
	return nil, nil
}

func (f *fakeBackfillTzKTClient) GetTokenTransfers(context.Context, string, string) ([]TzKTTokenTransfer, error) {
	return nil, nil
}

func (f *fakeBackfillTzKTClient) GetTokenMetadataUpdates(context.Context, string, string) ([]TzKTBigMapUpdate, error) {
	return nil, nil
}

func (f *fakeBackfillTzKTClient) GetTokenEvents(context.Context, string, string) ([]domain.BlockchainEvent, error) {
	return nil, nil
}

func (f *fakeBackfillTzKTClient) GetTokenBalancesByAccountWithinBlockRange(context.Context, string, uint64, uint64, int, int) ([]TzKTTokenBalance, error) {
	return nil, nil
}

func (f *fakeBackfillTzKTClient) GetLatestBlock(context.Context) (uint64, error) {
	if f.headErr != nil {
		return 0, f.headErr
	}
	return f.head, nil
}

func (f *fakeBackfillTzKTClient) GetTokenTransfersByLevelRange(_ context.Context, _, _ uint64, _, offset int) ([]TzKTTokenTransfer, error) {
	if f.transferPageErr != nil {
		return nil, f.transferPageErr
	}
	if f.transferPages == nil {
		return nil, nil
	}
	return f.transferPages[offset], nil
}

func (f *fakeBackfillTzKTClient) GetBigMapUpdatesByLevelRange(_ context.Context, _, _ uint64, _, offset int) ([]TzKTBigMapUpdate, error) {
	if f.bigmapPageErr != nil {
		return nil, f.bigmapPageErr
	}
	if f.bigmapPages == nil {
		return nil, nil
	}
	return f.bigmapPages[offset], nil
}

func (f *fakeBackfillTzKTClient) GetContractDeployer(context.Context, string) (string, error) {
	return "", nil
}

func (f *fakeBackfillTzKTClient) ChainID() domain.Chain {
	return domain.ChainTezosMainnet
}

func TestGroupBackfillByLevel_ordersMixedFeeds(t *testing.T) {
	transfers := []TzKTTokenTransfer{
		{Level: 102, ID: 1},
		{Level: 100, ID: 2},
	}
	updates := []TzKTBigMapUpdate{
		{Level: 101, Path: "token_metadata", ID: 3},
		{Level: 100, Path: "other", ID: 4},
	}

	buffers := groupBackfillByLevel(transfers, updates)
	levels := sortedLevels(buffers)

	require.Equal(t, []uint64{100, 101, 102}, levels)
	require.Len(t, buffers[100].transfers, 1)
	require.Len(t, buffers[100].bigmaps, 0)
	require.Len(t, buffers[101].bigmaps, 1)
	require.Len(t, buffers[102].transfers, 1)
}

func TestBackfillHistoricLevels_skipsWhenAlreadyCaughtUp(t *testing.T) {
	subscriber := &tzSubscriber{
		chainID:    domain.ChainTezosMainnet,
		tzktClient: &fakeBackfillTzKTClient{head: 50},
	}

	called := false
	highest, err := subscriber.backfillHistoricLevels(context.Background(), 60, 0, func(*domain.BlockchainEvent) error {
		called = true
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, uint64(50), highest)
	require.False(t, called)
}

func TestBackfillHistoricLevels_emitsLevelsInAscendingOrder(t *testing.T) {
	transfer100 := TzKTTokenTransfer{
		Level: 100,
		ID:    1,
		Token: TzKTTokenInfo{Standard: domain.StandardFA2, Contract: TzKTContract{Address: "KT1"}, TokenID: "0"},
	}
	transfer102 := TzKTTokenTransfer{
		Level: 102,
		ID:    2,
		Token: TzKTTokenInfo{Standard: domain.StandardFA2, Contract: TzKTContract{Address: "KT1"}, TokenID: "1"},
	}
	update101 := TzKTBigMapUpdate{
		Level: 101,
		Path:  "token_metadata",
		ID:    3,
		Contract: struct {
			Address string `json:"address"`
		}{Address: "KT1"},
		Content: struct {
			Hash  string      `json:"hash"`
			Key   interface{} `json:"key"`
			Value interface{} `json:"value"`
		}{Key: "0"},
	}

	subscriber := &tzSubscriber{
		chainID: domain.ChainTezosMainnet,
		tzktClient: &fakeBackfillTzKTClient{
			head: 102,
			transferPages: map[int][]TzKTTokenTransfer{
				0: {transfer100, transfer102},
			},
			bigmapPages: map[int][]TzKTBigMapUpdate{
				0: {update101},
			},
		},
		clock: adapter.NewClock(),
	}

	var seen []uint64
	highest, err := subscriber.backfillHistoricLevels(context.Background(), 100, 0, func(evt *domain.BlockchainEvent) error {
		seen = append(seen, evt.BlockNumber)
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, uint64(102), highest)
	require.Equal(t, []uint64{100, 101, 102}, seen)
}

func TestBackfillHistoricLevels_paginatesBothFeeds(t *testing.T) {
	page1 := make([]TzKTTokenTransfer, backfillPageSize)
	for i := uint64(0); i < backfillPageSize; i++ {
		page1[i] = TzKTTokenTransfer{
			Level: 100 + i,
			ID:    i + 1,
			Token: TzKTTokenInfo{Standard: domain.StandardFA2, Contract: TzKTContract{Address: "KT1"}, TokenID: "0"},
		}
	}
	page2 := []TzKTTokenTransfer{{
		Level: 1100,
		ID:    999,
		Token: TzKTTokenInfo{Standard: domain.StandardFA2, Contract: TzKTContract{Address: "KT1"}, TokenID: "0"},
	}}

	bmPage1 := make([]TzKTBigMapUpdate, backfillPageSize)
	for i := uint64(0); i < backfillPageSize; i++ {
		bmPage1[i] = TzKTBigMapUpdate{
			Level: 2000 + i,
			Path:  "token_metadata",
			ID:    i + 1,
			Contract: struct {
				Address string `json:"address"`
			}{Address: "KT1"},
			Content: struct {
				Hash  string      `json:"hash"`
				Key   interface{} `json:"key"`
				Value interface{} `json:"value"`
			}{Key: fmt.Sprintf("%d", i)},
		}
	}
	bmPage2 := []TzKTBigMapUpdate{{
		Level: 3000,
		Path:  "token_metadata",
		ID:    999,
		Contract: struct {
			Address string `json:"address"`
		}{Address: "KT1"},
		Content: struct {
			Hash  string      `json:"hash"`
			Key   interface{} `json:"key"`
			Value interface{} `json:"value"`
		}{Key: "9"},
	}}

	subscriber := &tzSubscriber{
		chainID: domain.ChainTezosMainnet,
		tzktClient: &fakeBackfillTzKTClient{
			head: 3000,
			transferPages: map[int][]TzKTTokenTransfer{
				0:                page1,
				backfillPageSize: page2,
			},
			bigmapPages: map[int][]TzKTBigMapUpdate{
				0:                bmPage1,
				backfillPageSize: bmPage2,
			},
		},
		clock: adapter.NewClock(),
	}

	count := 0
	highest, err := subscriber.backfillHistoricLevels(context.Background(), 100, 0, func(*domain.BlockchainEvent) error {
		count++
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, uint64(3000), highest)
	require.Equal(t, backfillPageSize+1+backfillPageSize+1, count)
}

func TestBackfillHistoricLevels_propagatesHandlerError(t *testing.T) {
	subscriber := &tzSubscriber{
		chainID: domain.ChainTezosMainnet,
		tzktClient: &fakeBackfillTzKTClient{
			head: 100,
			transferPages: map[int][]TzKTTokenTransfer{
				0: {{
					Level: 100,
					Token: TzKTTokenInfo{Standard: domain.StandardFA2, Contract: TzKTContract{Address: "KT1"}, TokenID: "0"},
				}},
			},
		},
		clock: adapter.NewClock(),
	}

	wantErr := errors.New("handler failed")
	_, err := subscriber.backfillHistoricLevels(context.Background(), 100, 0, func(*domain.BlockchainEvent) error {
		return wantErr
	})
	require.ErrorIs(t, err, wantErr)
}

func TestBackfillHistoricLevels_requiresHandler(t *testing.T) {
	subscriber := &tzSubscriber{
		chainID:    domain.ChainTezosMainnet,
		tzktClient: &fakeBackfillTzKTClient{head: 100},
	}

	_, err := subscriber.backfillHistoricLevels(context.Background(), 100, 0, nil)
	require.Error(t, err)
	require.ErrorContains(t, err, "backfill handler is required")
}
