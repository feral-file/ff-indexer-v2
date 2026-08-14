package adapters_test

import (
	"context"
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/stretchr/testify/require"

	"github.com/feral-file/ff-indexer-v2/internal/domain"
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
