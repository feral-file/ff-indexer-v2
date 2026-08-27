package adapters_test

import (
	"context"
	"errors"
	"math/big"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/mocks"
	"github.com/feral-file/ff-indexer-v2/internal/providers/ethereum/adapters"
	"github.com/feral-file/ff-indexer-v2/internal/providers/ethereum/helpers"
)

// newERC721TestAdapter builds an adapter with no client or block provider.
// EIP-4906 parsing needs neither: the logs below carry BlockTimestamp, so
// BaseEventFromLog resolves the timestamp without a provider lookup.
func newERC721TestAdapter() *adapters.ERC721Adapter {
	return adapters.NewERC721Adapter(nil, nil, nil, domain.ChainEthereumMainnet)
}

// eip4906Log builds a metadata-update log with the given topics and data.
func eip4906Log(topics []common.Hash, data []byte) types.Log {
	return types.Log{
		Address:        common.HexToAddress("0x0000000000000000000000000000000000000001"),
		BlockNumber:    25752049,
		BlockTimestamp: 1_700_000_000,
		Topics:         topics,
		Data:           data,
	}
}

// transferLog builds a Transfer log with the given number of address topics.
func transferLog(topics []common.Hash) types.Log {
	return types.Log{
		Address:        common.HexToAddress("0x0000000000000000000000000000000000000001"),
		BlockNumber:    25752049,
		BlockTimestamp: 1_700_000_000,
		Topics:         topics,
	}
}

func TestERC721ParseEvent_Transfer(t *testing.T) {
	t.Parallel()

	from := common.HexToAddress("0x1111111111111111111111111111111111111111")
	to := common.HexToAddress("0x2222222222222222222222222222222222222222")

	vLog := transferLog([]common.Hash{
		helpers.TransferEventSignature,
		common.BytesToHash(from.Bytes()),
		common.BytesToHash(to.Bytes()),
		common.BigToHash(big.NewInt(42)),
	})

	parsed, err := newERC721TestAdapter().ParseEvent(context.Background(), vLog)
	require.NoError(t, err)
	require.NotNil(t, parsed)
	require.Equal(t, domain.StandardERC721, parsed.Standard)
	require.Equal(t, "42", parsed.TokenNumber)
}

// TestERC721ParseEvent_ERC20TransferSkippedBeforeTimestampLookup pins the
// pre-existing ERC20 skip and, like the EIP-4906 cases, that it happens before
// the block-timestamp lookup.
func TestERC721ParseEvent_ERC20TransferSkippedBeforeTimestampLookup(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	blockProvider := mocks.NewMockBlockProvider(ctrl)
	blockProvider.EXPECT().
		GetBlockTimestamp(gomock.Any(), gomock.Any()).
		Return(time.Time{}, errors.New("block provider unavailable")).
		AnyTimes()

	from := common.HexToAddress("0x1111111111111111111111111111111111111111")
	to := common.HexToAddress("0x2222222222222222222222222222222222222222")

	vLog := transferLog([]common.Hash{
		helpers.TransferEventSignature,
		common.BytesToHash(from.Bytes()),
		common.BytesToHash(to.Bytes()),
	})
	vLog.BlockTimestamp = 0 // force the provider path

	adapter := adapters.NewERC721Adapter(nil, nil, blockProvider, domain.ChainEthereumMainnet)
	parsed, err := adapter.ParseEvent(context.Background(), vLog)
	require.NoError(t, err)
	require.Nil(t, parsed)
}

// TestERC721ParseEvent_NonStandardTransferSkippedBeforeTimestampLookup pins
// that any non-4-topic Transfer shape is dropped, not failed, and before the
// block-timestamp lookup. The 1-topic case is the CryptoKitties shape
// (pre-standard Transfer with no indexed parameters, all values in data) that
// crash-looped production ingestion on 2026-08-27 when it was a fatal error:
// the log replayed from the durable cursor on every restart.
func TestERC721ParseEvent_NonStandardTransferSkippedBeforeTimestampLookup(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		topics []common.Hash
		data   []byte
	}{
		{"one topic (CryptoKitties shape)", []common.Hash{helpers.TransferEventSignature},
			make([]byte, 96)}, // from, to, tokenId all in data
		{"two topics", []common.Hash{
			helpers.TransferEventSignature,
			common.BytesToHash(common.HexToAddress("0x1111111111111111111111111111111111111111").Bytes()),
		}, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctrl := gomock.NewController(t)
			blockProvider := mocks.NewMockBlockProvider(ctrl)
			blockProvider.EXPECT().
				GetBlockTimestamp(gomock.Any(), gomock.Any()).
				Return(time.Time{}, errors.New("block provider unavailable")).
				AnyTimes()

			vLog := transferLog(tc.topics)
			vLog.Data = tc.data
			vLog.BlockTimestamp = 0 // force the provider path if a lookup were attempted

			adapter := adapters.NewERC721Adapter(nil, nil, blockProvider, domain.ChainEthereumMainnet)
			parsed, err := adapter.ParseEvent(context.Background(), vLog)
			require.NoError(t, err, "a foreign Transfer shape must be dropped, never failed: a fatal error replays from the durable cursor and crash-loops ingestion")
			require.Nil(t, parsed)
		})
	}
}

func TestERC721ParseEvent_UnknownSignature(t *testing.T) {
	t.Parallel()

	vLog := transferLog([]common.Hash{common.HexToHash("0xdeadbeef")})

	parsed, err := newERC721TestAdapter().ParseEvent(context.Background(), vLog)
	require.ErrorIs(t, err, adapters.ErrUnknownEvent)
	require.Nil(t, parsed)
}

func TestERC721ParseEvent_NoTopics(t *testing.T) {
	t.Parallel()

	parsed, err := newERC721TestAdapter().ParseEvent(context.Background(), transferLog(nil))
	require.Error(t, err)
	require.Nil(t, parsed)
	require.Contains(t, err.Error(), "no topics")
}

func TestERC721ParseEvent_MetadataUpdateSpecShape(t *testing.T) {
	t.Parallel()

	vLog := eip4906Log(
		[]common.Hash{helpers.EIP4906MetadataUpdateEventSignature},
		common.BigToHash(big.NewInt(42)).Bytes(),
	)

	parsed, err := newERC721TestAdapter().ParseEvent(context.Background(), vLog)
	require.NoError(t, err)
	require.NotNil(t, parsed)
	require.Equal(t, domain.EventTypeMetadataUpdate, parsed.EventType)
	require.Equal(t, domain.StandardERC721, parsed.Standard)
	require.Equal(t, "42", parsed.TokenNumber)
	require.Equal(t, "1", parsed.Quantity)
}

// TestERC721ParseEvent_MetadataUpdateIndexedVariant covers the non-conforming
// `MetadataUpdate(uint256 indexed _tokenId)` shape: same topic0 as the EIP-4906
// spec event, but the token id lands in topics[1] with empty data. Regression
// test for a live poison log (mainnet block 25752049 index 300) that crashed
// ingestion with "expected 1 topic, got 2".
func TestERC721ParseEvent_MetadataUpdateIndexedVariant(t *testing.T) {
	t.Parallel()

	vLog := eip4906Log(
		[]common.Hash{
			helpers.EIP4906MetadataUpdateEventSignature,
			common.BigToHash(big.NewInt(42)),
		},
		nil,
	)

	parsed, err := newERC721TestAdapter().ParseEvent(context.Background(), vLog)
	require.NoError(t, err)
	require.NotNil(t, parsed)
	require.Equal(t, domain.EventTypeMetadataUpdate, parsed.EventType)
	require.Equal(t, "42", parsed.TokenNumber)
}

// TestERC721ParseEvent_MalformedMetadataUpdateSkipped verifies that
// MetadataUpdate logs whose shape carries no recoverable token id are skipped
// (nil, nil) instead of failing: a fatal parse error on a permanently malformed
// log would crash-loop live ingestion replaying from the durable cursor.
func TestERC721ParseEvent_MalformedMetadataUpdateSkipped(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		topics []common.Hash
		data   []byte
	}{
		{
			name:   "spec topic count with truncated data",
			topics: []common.Hash{helpers.EIP4906MetadataUpdateEventSignature},
			data:   []byte{0x01},
		},
		{
			name:   "spec topic count with empty data",
			topics: []common.Hash{helpers.EIP4906MetadataUpdateEventSignature},
			data:   nil,
		},
		{
			name: "more topics than any declaration can produce",
			topics: []common.Hash{
				helpers.EIP4906MetadataUpdateEventSignature,
				common.BigToHash(big.NewInt(1)),
				common.BigToHash(big.NewInt(2)),
			},
		},
		{
			// Neither encoding of MetadataUpdate(uint256) produces this: the
			// indexed form carries no data, the non-indexed form carries no
			// second topic. Provenance is unknown, so topics[1] cannot be
			// trusted as a token id.
			name: "indexed topic count with unexpected data",
			topics: []common.Hash{
				helpers.EIP4906MetadataUpdateEventSignature,
				common.BigToHash(big.NewInt(42)),
			},
			data: common.BigToHash(big.NewInt(99)).Bytes(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			parsed, err := newERC721TestAdapter().ParseEvent(context.Background(), eip4906Log(tt.topics, tt.data))
			require.NoError(t, err)
			require.Nil(t, parsed)
		})
	}
}

// TestERC721ParseEvent_MalformedSkippedBeforeTimestampLookup pins that shape
// validation runs before the block-timestamp lookup. Historical eth_getLogs
// results carry no BlockTimestamp, so a malformed log would otherwise reach
// BlockProvider.GetBlockTimestamp and surface that provider's error as fatal —
// re-opening the crash path this PR closes, just via a failing provider rather
// than the shape check itself.
func TestERC721ParseEvent_MalformedSkippedBeforeTimestampLookup(t *testing.T) {
	t.Parallel()

	malformed := []struct {
		name   string
		topics []common.Hash
		data   []byte
	}{
		{
			name: "MetadataUpdate with too many topics",
			topics: []common.Hash{
				helpers.EIP4906MetadataUpdateEventSignature,
				common.BigToHash(big.NewInt(1)),
				common.BigToHash(big.NewInt(2)),
			},
		},
		{
			name:   "BatchMetadataUpdate with truncated data",
			topics: []common.Hash{helpers.EIP4906BatchMetadataUpdateEventSignature},
			data:   common.BigToHash(big.NewInt(7)).Bytes(),
		},
		{
			name: "MetadataUpdate with indexed topic count and unexpected data",
			topics: []common.Hash{
				helpers.EIP4906MetadataUpdateEventSignature,
				common.BigToHash(big.NewInt(42)),
			},
			data: common.BigToHash(big.NewInt(99)).Bytes(),
		},
	}

	for _, tt := range malformed {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctrl := gomock.NewController(t)
			blockProvider := mocks.NewMockBlockProvider(ctrl)
			// Optional: a correct implementation never reaches this call. If it
			// does, the returned error propagates and the assertions below fail.
			blockProvider.EXPECT().
				GetBlockTimestamp(gomock.Any(), gomock.Any()).
				Return(time.Time{}, errors.New("block provider unavailable")).
				AnyTimes()

			adapter := adapters.NewERC721Adapter(nil, nil, blockProvider, domain.ChainEthereumMainnet)

			vLog := eip4906Log(tt.topics, tt.data)
			vLog.BlockTimestamp = 0 // force the provider path

			parsed, err := adapter.ParseEvent(context.Background(), vLog)
			require.NoError(t, err)
			require.Nil(t, parsed)
		})
	}
}

// TestERC721ParseEvent_WellFormedStillFailsOnTimestampError guards the other
// side of the boundary: skipping malformed shapes early must not swallow
// timestamp failures for logs we do intend to index.
func TestERC721ParseEvent_WellFormedStillFailsOnTimestampError(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	blockProvider := mocks.NewMockBlockProvider(ctrl)
	timestampErr := errors.New("block provider unavailable")
	blockProvider.EXPECT().
		GetBlockTimestamp(gomock.Any(), gomock.Any()).
		Return(time.Time{}, timestampErr)

	adapter := adapters.NewERC721Adapter(nil, nil, blockProvider, domain.ChainEthereumMainnet)

	vLog := eip4906Log(
		[]common.Hash{helpers.EIP4906MetadataUpdateEventSignature},
		common.BigToHash(big.NewInt(42)).Bytes(),
	)
	vLog.BlockTimestamp = 0

	parsed, err := adapter.ParseEvent(context.Background(), vLog)
	require.Error(t, err)
	require.Nil(t, parsed)
	require.ErrorIs(t, err, timestampErr)
}

func TestERC721ParseEvent_BatchMetadataUpdateSpecShape(t *testing.T) {
	t.Parallel()

	data := append(
		common.BigToHash(big.NewInt(7)).Bytes(),
		common.BigToHash(big.NewInt(9)).Bytes()...,
	)
	vLog := eip4906Log([]common.Hash{helpers.EIP4906BatchMetadataUpdateEventSignature}, data)

	parsed, err := newERC721TestAdapter().ParseEvent(context.Background(), vLog)
	require.NoError(t, err)
	require.NotNil(t, parsed)
	require.Equal(t, domain.EventTypeMetadataUpdateRange, parsed.EventType)
	require.Equal(t, "7", parsed.TokenNumber)
	require.Equal(t, "9", parsed.ToTokenNumber)
}

// TestERC721ParseEvent_MalformedBatchMetadataUpdateSkipped verifies that
// indexed or truncated BatchMetadataUpdate variants are skipped rather than
// fatal, for the same crash-loop reason as single MetadataUpdate. The indexed
// case is skipped rather than parsed because a lone id in topics cannot be
// attributed to _fromTokenId or _toTokenId from the log alone.
func TestERC721ParseEvent_MalformedBatchMetadataUpdateSkipped(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		topics []common.Hash
		data   []byte
	}{
		{
			name: "indexed variant leaves the range ambiguous",
			topics: []common.Hash{
				helpers.EIP4906BatchMetadataUpdateEventSignature,
				common.BigToHash(big.NewInt(7)),
			},
			data: common.BigToHash(big.NewInt(9)).Bytes(),
		},
		{
			name:   "spec topic count with only one id in data",
			topics: []common.Hash{helpers.EIP4906BatchMetadataUpdateEventSignature},
			data:   common.BigToHash(big.NewInt(7)).Bytes(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			parsed, err := newERC721TestAdapter().ParseEvent(context.Background(), eip4906Log(tt.topics, tt.data))
			require.NoError(t, err)
			require.Nil(t, parsed)
		})
	}
}
