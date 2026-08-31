package adapter_test

import (
	"context"
	"encoding/json"
	"errors"
	"math/big"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/stretchr/testify/require"

	"github.com/feral-file/ff-indexer-v2/internal/adapter"
)

// fakeWarehouse is a minimal JSON-RPC 2.0 server standing in for ff-eth-logs.
// handlers map method names to a function returning (result, jsonError).
type fakeWarehouse struct {
	t        *testing.T
	handlers map[string]func(params json.RawMessage) (any, *jsonError)
	requests []string
}

type jsonError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func (f *fakeWarehouse) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ID     json.RawMessage `json:"id"`
		Method string          `json:"method"`
		Params json.RawMessage `json:"params"`
	}
	require.NoError(f.t, json.NewDecoder(r.Body).Decode(&req))
	f.requests = append(f.requests, req.Method)

	resp := map[string]any{"jsonrpc": "2.0", "id": req.ID}
	handler, ok := f.handlers[req.Method]
	if !ok {
		resp["error"] = jsonError{Code: -32601, Message: "the method " + req.Method + " does not exist/is not available"}
	} else if result, jerr := handler(req.Params); jerr != nil {
		resp["error"] = jerr
	} else {
		resp["result"] = result
	}
	w.Header().Set("Content-Type", "application/json")
	require.NoError(f.t, json.NewEncoder(w).Encode(resp))
}

func newFakeWarehouse(t *testing.T) (*fakeWarehouse, adapter.LogWarehouse) {
	t.Helper()
	f := &fakeWarehouse{t: t, handlers: map[string]func(json.RawMessage) (any, *jsonError){}}
	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)
	wh, err := adapter.NewLogWarehouseDialer().Dial(context.Background(), srv.URL, time.Second)
	require.NoError(t, err)
	t.Cleanup(wh.Close)
	return f, wh
}

func scopeError(reason string) *jsonError {
	return &jsonError{Code: -32000, Message: "out of warehouse scope: " + reason}
}

// TestRealLogWarehouse_HeadAndChainID pins the two trivial calls: the head is
// the warehouse's eth_blockNumber (its head, not the chain tip) and the chain
// id decodes as a quantity.
func TestRealLogWarehouse_HeadAndChainID(t *testing.T) {
	t.Parallel()
	f, wh := newFakeWarehouse(t)
	f.handlers["eth_blockNumber"] = func(json.RawMessage) (any, *jsonError) { return "0x18a4f3d", nil }
	f.handlers["eth_chainId"] = func(json.RawMessage) (any, *jsonError) { return "0x1", nil }

	head, err := wh.Head(context.Background())
	require.NoError(t, err)
	require.Equal(t, uint64(0x18a4f3d), head)

	id, err := wh.ChainID(context.Background())
	require.NoError(t, err)
	require.Equal(t, big.NewInt(1), id)
}

// TestRealLogWarehouse_FilterLogsDecodesBlockTimestamp pins that the filter
// reaches the wire in geth's shape and that the warehouse's blockTimestamp
// field survives decoding — it is what lets event parsing skip the per-block
// eth_getBlockByNumber lookup for warehouse-served logs.
func TestRealLogWarehouse_FilterLogsDecodesBlockTimestamp(t *testing.T) {
	t.Parallel()
	f, wh := newFakeWarehouse(t)
	var gotParams json.RawMessage
	f.handlers["eth_getLogs"] = func(params json.RawMessage) (any, *jsonError) {
		gotParams = params
		return []map[string]any{{
			"address":          "0x00000000000000000000000000000000000000aa",
			"topics":           []string{"0x" + common.Bytes2Hex(make([]byte, 32))},
			"data":             "0x",
			"blockNumber":      "0x10",
			"transactionHash":  "0x" + common.Bytes2Hex(make([]byte, 32)),
			"transactionIndex": "0x2",
			"blockHash":        "0x" + common.Bytes2Hex(make([]byte, 32)),
			"blockTimestamp":   "0x68b0b3c0",
			"logIndex":         "0x7",
			"removed":          false,
		}}, nil
	}

	logs, err := wh.FilterLogs(context.Background(), ethereum.FilterQuery{
		FromBlock: big.NewInt(0),
		ToBlock:   big.NewInt(16),
		Topics:    [][]common.Hash{{common.Hash{}}},
	}, nil)
	require.NoError(t, err)
	require.Len(t, logs, 1)
	require.Equal(t, uint64(16), logs[0].BlockNumber)
	require.Equal(t, uint(7), logs[0].Index)
	require.Equal(t, uint64(0x68b0b3c0), logs[0].BlockTimestamp)

	var params []map[string]any
	require.NoError(t, json.Unmarshal(gotParams, &params))
	require.Len(t, params, 1)
	require.Equal(t, "0x0", params[0]["fromBlock"])
	require.Equal(t, "0x10", params[0]["toBlock"])
	require.NotContains(t, params[0], "erc1155Id", "a nil id sends a standard, node-compatible filter")
}

// TestRealLogWarehouse_FilterLogsSendsERC1155ID pins the warehouse-only wire
// contract: a non-nil erc1155ID is sent as the eth_getLogs "erc1155Id" field
// (32-byte hex) alongside the standard fields, and a nil id omits it entirely
// so the vendor-shaped request a node also accepts is preserved. The field
// name and encoding must match ff-eth-logs rpcapi.FilterCriteria.
func TestRealLogWarehouse_FilterLogsSendsERC1155ID(t *testing.T) {
	t.Parallel()
	f, wh := newFakeWarehouse(t)
	var gotParams json.RawMessage
	f.handlers["eth_getLogs"] = func(params json.RawMessage) (any, *jsonError) {
		gotParams = params
		return []map[string]any{}, nil
	}

	id := common.BigToHash(big.NewInt(42))
	_, err := wh.FilterLogs(context.Background(), ethereum.FilterQuery{
		FromBlock: big.NewInt(0),
		ToBlock:   big.NewInt(16),
		Addresses: []common.Address{common.HexToAddress("0xabc")},
		Topics:    [][]common.Hash{{common.Hash{}}},
	}, &id)
	require.NoError(t, err)

	var params []map[string]any
	require.NoError(t, json.Unmarshal(gotParams, &params))
	require.Len(t, params, 1)
	require.Equal(t, id.Hex(), params[0]["erc1155Id"], "the 32-byte token id is sent as erc1155Id")
	require.Equal(t, "0x0", params[0]["fromBlock"], "standard fields still present")
	require.Equal(t, "0x10", params[0]["toBlock"])
	require.Contains(t, params[0], "address")
	require.Contains(t, params[0], "topics")
}

// TestRealLogWarehouse_ScopeErrorIsOutOfScope pins the contract with
// ff-eth-logs: a -32000 whose message starts with "out of warehouse scope" is
// the fall-through signal, reachable both through the sentinel and the raw
// rpc.Error, while the result-cap error is NOT out-of-scope (it must reach the
// too-many-results classifier so the range gets split).
func TestRealLogWarehouse_ScopeErrorIsOutOfScope(t *testing.T) {
	t.Parallel()
	f, wh := newFakeWarehouse(t)
	f.handlers["eth_getLogs"] = func(json.RawMessage) (any, *jsonError) {
		return nil, scopeError("blocks 100-200 extend above the warehouse head 50")
	}
	f.handlers["eth_blockNumber"] = func(json.RawMessage) (any, *jsonError) {
		return nil, scopeError("warehouse is empty")
	}

	_, err := wh.FilterLogs(context.Background(), ethereum.FilterQuery{FromBlock: big.NewInt(100), ToBlock: big.NewInt(200)}, nil)
	require.True(t, adapter.IsOutOfScope(err), "scope refusal must classify: %v", err)
	require.ErrorIs(t, err, adapter.ErrOutOfScope)
	require.ErrorContains(t, err, "extend above the warehouse head 50", "the warehouse's reason must survive for the log line")

	_, err = wh.Head(context.Background())
	require.True(t, adapter.IsOutOfScope(err), "an empty warehouse is a scope condition: %v", err)

	// The raw classifier, independent of the sentinel wrapping.
	require.True(t, adapter.IsOutOfScope(&fakeRPCError{code: -32000, msg: "out of warehouse scope: warehouse is under maintenance"}))
	require.False(t, adapter.IsOutOfScope(&fakeRPCError{code: -32000, msg: "query returned more than 100000 results"}))
	require.False(t, adapter.IsOutOfScope(&fakeRPCError{code: -32602, msg: "out of warehouse scope: wrong code"}))
	require.False(t, adapter.IsOutOfScope(errors.New("out of warehouse scope: not an rpc error")))
	require.False(t, adapter.IsOutOfScope(nil))
}

// TestRealLogWarehouse_ResultCapErrorKeepsCodeAndMessage pins that a
// non-scope warehouse error passes through untouched, code included, so the
// pagination helper's existing classifiers see exactly what the warehouse said.
func TestRealLogWarehouse_ResultCapErrorKeepsCodeAndMessage(t *testing.T) {
	t.Parallel()
	f, wh := newFakeWarehouse(t)
	f.handlers["eth_getLogs"] = func(json.RawMessage) (any, *jsonError) {
		return nil, &jsonError{Code: -32000, Message: "query returned more than 100000 results"}
	}
	_, err := wh.FilterLogs(context.Background(), ethereum.FilterQuery{FromBlock: big.NewInt(0), ToBlock: big.NewInt(1)}, nil)
	require.Error(t, err)
	require.False(t, adapter.IsOutOfScope(err))
	var rpcErr rpc.Error
	require.ErrorAs(t, err, &rpcErr)
	require.Equal(t, -32000, rpcErr.ErrorCode())
	require.Equal(t, "query returned more than 100000 results", rpcErr.Error())
}

// TestRealLogWarehouse_TimeoutIsSingleAttempt pins the no-retry policy: a
// request that outlives the per-request timeout returns once, promptly, with a
// deadline error — the caller falls through to the vendor instead of waiting
// on a retry budget.
func TestRealLogWarehouse_TimeoutIsSingleAttempt(t *testing.T) {
	t.Parallel()
	release := make(chan struct{})
	defer close(release)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	t.Cleanup(srv.Close)
	wh, err := adapter.NewLogWarehouseDialer().Dial(context.Background(), srv.URL, 50*time.Millisecond)
	require.NoError(t, err)
	t.Cleanup(wh.Close)

	start := time.Now()
	_, err = wh.Head(context.Background())
	require.Error(t, err)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Less(t, time.Since(start), 2*time.Second, "no retry loop may run behind the timeout")
}

// TestRealLogWarehouse_UnreachableFailsFast pins that a warehouse that is down
// surfaces as a plain transport error on the first call (Dial itself does not
// connect over HTTP), not as a hang.
func TestRealLogWarehouse_UnreachableFailsFast(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.NotFoundHandler())
	url := srv.URL
	srv.Close()

	wh, err := adapter.NewLogWarehouseDialer().Dial(context.Background(), url, time.Second)
	require.NoError(t, err, "HTTP dial must not require a live server")
	t.Cleanup(wh.Close)

	_, err = wh.Head(context.Background())
	require.Error(t, err)
	require.False(t, adapter.IsOutOfScope(err))
}

// TestEndpointForLogs pins the redaction used in warehouse and vendor log lines.
func TestEndpointForLogs(t *testing.T) {
	t.Parallel()
	require.Equal(t, "http://10.124.0.4:8545", adapter.EndpointForLogs("http://10.124.0.4:8545"))
	require.Equal(t, "https://mainnet.example", adapter.EndpointForLogs("https://mainnet.example/v3/secret-key"))
	require.Equal(t, "<redacted>", adapter.EndpointForLogs("not a url"))
}

type fakeRPCError struct {
	code int
	msg  string
}

func (e *fakeRPCError) Error() string  { return e.msg }
func (e *fakeRPCError) ErrorCode() int { return e.code }
