# Event Emitters

This document describes the Ethereum and Tezos event emitters that monitor blockchain events and publish them to NATS JetStream.

## Architecture Overview

The event emitters follow a simple architecture:

1. **Subscribe** to blockchain events (Ethereum via Infura WebSocket, Tezos via TzKT SignalR)
2. **Normalize** events to a standard format
3. **Publish** to NATS JetStream
4. **Track** progress using database cursors

## Components

### 1. Ethereum Event Emitter (`cmd/ethereum-event-emitter`)

Monitors ERC721 and ERC1155 transfer events on Ethereum.

**Features:**
- Subscribes to Infura WebSocket for real-time events
- Monitors ERC721/ERC1155 Transfer events
- Monitors EIP-4906 MetadataUpdate and BatchMetadataUpdate events
- Monitors ERC1155 URI events
- Detects mint, transfer, burn, and metadata_update events
- Tracks last processed block in database
- Publishes to NATS subject: `events.ethereum.{event_type}`

**Monitored Events:**
- `Transfer(address,address,uint256)` - ERC721 transfers
- `TransferSingle(address,address,address,uint256,uint256)` - ERC1155 transfers
- `MetadataUpdate(uint256)` - EIP-4906 metadata updates
- `BatchMetadataUpdate(uint256,uint256)` - EIP-4906 batch metadata updates
- `URI(string,uint256)` - ERC1155 URI/metadata updates

**Configuration:** `cmd/ethereum-event-emitter/config.yaml.sample`

```yaml
ethereum:
  websocket_url: wss://mainnet.infura.io/ws/v3/YOUR_INFURA_PROJECT_ID
  chain_id: eip155:1
  start_block: 0  # 0 = start from latest or last cursor
```

**Running:**
```bash
go run cmd/ethereum-event-emitter/main.go -config cmd/ethereum-event-emitter/config.yaml
```

### 2. Tezos Event Emitter (`cmd/tezos-event-emitter`)

Monitors FA2 token transfer events on Tezos via TzKT.

**Features:**
- Subscribes to TzKT WebSocket using SignalR
- Monitors FA2 token transfers via `SubscribeToTokenTransfers`
- Monitors token metadata updates via `SubscribeToBigMaps`
- Detects mint, transfer, burn, and metadata_update events
- Tracks last processed level in database
- Publishes to NATS subject: `events.tezos.{event_type}`

**Monitored Channels:**
- `SubscribeToTokenTransfers` - FA2 token transfers
- `SubscribeToBigMaps` (path: "token_metadata") - Metadata updates in big maps

**Configuration:** `cmd/tezos-event-emitter/config.yaml.sample`

```yaml
tezos:
  api_url: https://api.tzkt.io
  websocket_url: https://api.tzkt.io/v1/ws
  chain_id: tezos:mainnet
  start_level: 0  # 0 = start from last cursor
```

**Running:**
```bash
go run cmd/tezos-event-emitter/main.go -config cmd/tezos-event-emitter/config.yaml
```

## Standardized Event Format

Both emitters normalize blockchain events to this format:

```json
{
  "chain": "eip155:1",
  "standard": "erc721",
  "contract_address": "0x...",
  "token_number": "1234",
  "event_type": "transfer",
  "from_address": "0x...",
  "to_address": "0x...",
  "quantity": "1",
  "tx_hash": "0x...",
  "block_number": 12345678,
  "block_hash": "0x...",
  "timestamp": "2025-10-15T12:00:00Z",
  "log_index": 123
}
```

### Metadata Update Range Events

For batch metadata updates (EIP-4906 `BatchMetadataUpdate`), a special range event is emitted:

```json
{
  "chain": "eip155:1",
  "standard": "erc721",
  "contract_address": "0x...",
  "token_number": "1000",
  "to_token_number": "1999",
  "event_type": "metadata_update_range",
  "from_address": "",
  "to_address": "",
  "quantity": "1",
  "tx_hash": "0x...",
  "block_number": 12345678,
  "block_hash": "0x...",
  "timestamp": "2025-10-15T12:00:00Z",
  "log_index": 123
}
```

**Key differences:**
- `event_type` is `metadata_update_range`
- `token_number` contains the **start** token ID
- `to_token_number` contains the **end** token ID (inclusive)
- The event-bridge will expand this into individual token updates

**Range size calculation**: `toTokenId - fromTokenId + 1`

**Event Types:**
- `mint`: Token created (from zero address)
- `burn`: Token destroyed (to zero address)
- `transfer`: Token ownership changed
- `metadata_update`: Single token metadata updated
- `metadata_update_range`: Range of tokens metadata updated (batch)

## Database Schema

### Key-Value Store

Used to track block/level cursors:

```sql
CREATE TABLE key_value_store (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL,
  updated_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ
);
```

**Keys:**
- `block_cursor:eip155:1` - Last processed Ethereum block
- `block_cursor:tezos:mainnet` - Last processed Tezos level

## Provider Interfaces

All blockchain providers implement interfaces for testability:

### Ethereum Subscriber (`internal/providers/ethereum/subscriber.go`)

```go
type Subscriber interface {
    SubscribeEvents(ctx context.Context, fromBlock uint64, handler EventHandler) error
    GetLatestBlockNumber(ctx context.Context) (uint64, error)
    Close()
}
```

**Event Handling:**
- Subscribes to multiple event signatures in a single filter
- ERC721: Transfer + MetadataUpdate + BatchMetadataUpdate
- ERC1155: TransferSingle + URI

### Tezos Subscriber (`internal/providers/tezos/subscriber.go`)

```go
type Subscriber interface {
    SubscribeEvents(ctx context.Context, handler EventHandler) error
    GetLatestLevel(ctx context.Context) (uint64, error)
    Close() error
}
```

**Event Handling:**
- Subscribes to two SignalR channels simultaneously:
  - `SubscribeToTokenTransfers` - Transfer events
  - `SubscribeToBigMaps` - Big map updates for metadata changes
- Filters big map updates by path: "token_metadata"

### NATS Publisher (`internal/providers/jetstream/publisher.go`)

```go
type Publisher interface {
    PublishEvent(ctx context.Context, event *domain.BlockchainEvent) error
    Close() error
}
```

## Error Handling

- **Connection Failures**: Emitters will retry connection based on NATS configuration
- **Parse Errors**: Logged but don't stop the subscription
- **Publish Errors**: Logged and event processing continues
- **Graceful Shutdown**: SIGINT/SIGTERM handled properly

## Cursor Management

Block/level cursors are saved periodically:
- Every 10 blocks/levels
- OR every 30 seconds

This balances performance with crash recovery.

## Testing

All providers use interfaces, making them mockable with `gomock`:

```go
// Example test setup
mockSubscriber := mocks.NewMockSubscriber(ctrl)
mockPublisher := mocks.NewMockPublisher(ctrl)

// Test event handling
mockSubscriber.EXPECT().
    SubscribeTransferEvents(gomock.Any(), gomock.Any(), gomock.Any()).
    Return(nil)
```

## Dependencies

- **Ethereum**: `github.com/ethereum/go-ethereum`
- **Tezos**: `github.com/philippseith/signalr`
- **NATS**: `github.com/nats-io/nats.go`
- **Config**: `github.com/spf13/viper`
- **Database**: `gorm.io/gorm`, `gorm.io/driver/postgres`
- **Logging**: `github.com/sirupsen/logrus` (via adapter)

## Future Enhancements

1. **Contract Filtering**: Add configuration to filter specific contracts
2. **Finality Handling**: Wait for N confirmations before publishing
3. **Backfill Support**: Historical event indexing from specific block
4. **Metrics**: Prometheus metrics for monitoring
5. **Health Checks**: HTTP endpoint for health status

