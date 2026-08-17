package tezos_test

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/mocks"
	"github.com/feral-file/ff-indexer-v2/internal/providers/tezos"
)

func newTzKTClientWithMockHTTP(t *testing.T) (*mocks.MockHTTPClient, tezos.TzKTClient) {
	t.Helper()
	ctrl := gomock.NewController(t)
	httpClient := mocks.NewMockHTTPClient(ctrl)
	client := tezos.NewTzKTClient(domain.ChainTezosMainnet, "https://tzkt.example", httpClient, nil, nil, nil)
	return httpClient, client
}

func stubBigMapKeysResponse(httpClient *mocks.MockHTTPClient, wantURL string, body string) {
	httpClient.
		EXPECT().
		GetAndUnmarshal(gomock.Any(), wantURL, gomock.Any()).
		DoAndReturn(func(_ context.Context, _ string, result interface{}) error {
			return json.Unmarshal([]byte(body), result)
		})
}

func TestTzKTClient_GetTokenMetadataURI(t *testing.T) {
	httpClient, client := newTzKTClientWithMockHTTP(t)

	uri := "ipfs://QmRCRtivzx8SkmCsesW8GoFBVrofJN1WjYSHKvWa6BagTL"
	body := fmt.Sprintf(
		`[{"active":true,"key":"324719","value":{"token_id":"324719","token_info":{"":"%s"}}}]`,
		hex.EncodeToString([]byte(uri)),
	)
	stubBigMapKeysResponse(httpClient,
		"https://tzkt.example/v1/contracts/KT1KEa8z6vWXDJrVqtMrAeDVzsvxat3kHaCE/bigmaps/token_metadata/keys?key=324719&limit=1",
		body,
	)

	got, err := client.GetTokenMetadataURI(context.Background(), "KT1KEa8z6vWXDJrVqtMrAeDVzsvxat3kHaCE", "324719")
	require.NoError(t, err)
	require.Equal(t, uri, got)
}

func TestTzKTClient_GetTokenMetadataURI_NoKey(t *testing.T) {
	httpClient, client := newTzKTClientWithMockHTTP(t)

	// TzKT returns an empty array when the big map or key does not exist.
	stubBigMapKeysResponse(httpClient,
		"https://tzkt.example/v1/contracts/KT1ABC/bigmaps/token_metadata/keys?key=1&limit=1",
		`[]`,
	)

	got, err := client.GetTokenMetadataURI(context.Background(), "KT1ABC", "1")
	require.NoError(t, err)
	require.Empty(t, got)
}

func TestTzKTClient_GetTokenMetadataURI_NoEmptyKeyInTokenInfo(t *testing.T) {
	httpClient, client := newTzKTClientWithMockHTTP(t)

	stubBigMapKeysResponse(httpClient,
		"https://tzkt.example/v1/contracts/KT1ABC/bigmaps/token_metadata/keys?key=1&limit=1",
		`[{"active":true,"key":"1","value":{"token_id":"1","token_info":{"name":"6e6f74206120757269"}}}]`,
	)

	got, err := client.GetTokenMetadataURI(context.Background(), "KT1ABC", "1")
	require.NoError(t, err)
	require.Empty(t, got)
}

func TestTzKTClient_GetTokenMetadataURI_InvalidHex(t *testing.T) {
	httpClient, client := newTzKTClientWithMockHTTP(t)

	stubBigMapKeysResponse(httpClient,
		"https://tzkt.example/v1/contracts/KT1ABC/bigmaps/token_metadata/keys?key=1&limit=1",
		`[{"active":true,"key":"1","value":{"token_id":"1","token_info":{"":"zznothex"}}}]`,
	)

	_, err := client.GetTokenMetadataURI(context.Background(), "KT1ABC", "1")
	require.Error(t, err)
	require.Contains(t, err.Error(), "hex-decode")
}

func TestTzKTClient_GetTokenMetadataURI_HTTPError(t *testing.T) {
	httpClient, client := newTzKTClientWithMockHTTP(t)

	httpClient.
		EXPECT().
		GetAndUnmarshal(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(fmt.Errorf("boom"))

	_, err := client.GetTokenMetadataURI(context.Background(), "KT1ABC", "1")
	require.Error(t, err)
}
