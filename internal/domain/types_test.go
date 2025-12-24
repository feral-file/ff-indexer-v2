package domain

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestIsValidChain(t *testing.T) {
	tests := []struct {
		name     string
		chain    Chain
		expected bool
	}{
		{
			name:     "valid ethereum mainnet",
			chain:    ChainEthereumMainnet,
			expected: true,
		},
		{
			name:     "valid ethereum sepolia",
			chain:    ChainEthereumSepolia,
			expected: true,
		},
		{
			name:     "valid tezos mainnet",
			chain:    ChainTezosMainnet,
			expected: true,
		},
		{
			name:     "valid tezos ghostnet",
			chain:    ChainTezosGhostnet,
			expected: true,
		},
		{
			name:     "invalid empty chain",
			chain:    Chain(""),
			expected: false,
		},
		{
			name:     "invalid random chain",
			chain:    Chain("invalid:chain"),
			expected: false,
		},
		{
			name:     "invalid polygon chain",
			chain:    Chain("eip155:137"),
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsValidChain(tt.chain)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestBlockchainEvent_Valid(t *testing.T) {
	// Use known valid Ethereum addresses (42 chars including 0x)
	validEthereumAddress := "0x396343362be2A4dA1cE0C1C210945346fb82Aa49"
	validEthereumAddress2 := "0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb"
	validTezosAddress := "KT1BvXTW1XqhE1GHTRKRvz8w3a7X5f5NqEZr"
	zeroAddress := ETHEREUM_ZERO_ADDRESS
	emptyAddress := ""

	tests := []struct {
		name     string
		event    BlockchainEvent
		expected bool
	}{
		// Valid transfer events
		{
			name: "valid ERC721 transfer",
			event: BlockchainEvent{
				Chain:           ChainEthereumMainnet,
				Standard:        StandardERC721,
				ContractAddress: validEthereumAddress,
				TokenNumber:     "123",
				EventType:       EventTypeTransfer,
				FromAddress:     stringPtr(validEthereumAddress),
				ToAddress:       stringPtr(validEthereumAddress2),
				Quantity:        "1",
				TxHash:          "0xabc123",
				BlockNumber:     1000,
				Timestamp:       time.Now(),
				TxIndex:         0,
			},
			expected: true,
		},
		{
			name: "valid ERC1155 transfer",
			event: BlockchainEvent{
				Chain:           ChainEthereumMainnet,
				Standard:        StandardERC1155,
				ContractAddress: validEthereumAddress,
				TokenNumber:     "456",
				EventType:       EventTypeTransfer,
				FromAddress:     stringPtr(validEthereumAddress),
				ToAddress:       stringPtr(validEthereumAddress2),
				Quantity:        "5",
				TxHash:          "0xdef456",
				BlockNumber:     2000,
				Timestamp:       time.Now(),
				TxIndex:         1,
			},
			expected: true,
		},
		{
			name: "valid FA2 transfer",
			event: BlockchainEvent{
				Chain:           ChainTezosMainnet,
				Standard:        StandardFA2,
				ContractAddress: validTezosAddress,
				TokenNumber:     "789",
				EventType:       EventTypeTransfer,
				FromAddress:     stringPtr("tz1VSUr8wwNhLAzempoch5d6hLRiTh8Cjcjb"),
				ToAddress:       stringPtr("tz2N4RqF6bgxXzZvSfbmLFFwY2bH7MvLvXeV"),
				Quantity:        "1",
				TxHash:          "op123",
				BlockNumber:     3000,
				Timestamp:       time.Now(),
				TxIndex:         0,
			},
			expected: true,
		},
		// Invalid transfer events
		{
			name: "invalid transfer - nil from address",
			event: BlockchainEvent{
				Chain:           ChainEthereumMainnet,
				Standard:        StandardERC721,
				ContractAddress: validEthereumAddress,
				TokenNumber:     "123",
				EventType:       EventTypeTransfer,
				FromAddress:     nil,
				ToAddress:       stringPtr(validEthereumAddress),
				Quantity:        "1",
				TxHash:          "0xabc123",
				BlockNumber:     1000,
				Timestamp:       time.Now(),
				TxIndex:         0,
			},
			expected: false,
		},
		{
			name: "invalid transfer - zero from address",
			event: BlockchainEvent{
				Chain:           ChainEthereumMainnet,
				Standard:        StandardERC721,
				ContractAddress: validEthereumAddress,
				TokenNumber:     "123",
				EventType:       EventTypeTransfer,
				FromAddress:     stringPtr(zeroAddress),
				ToAddress:       stringPtr(validEthereumAddress),
				Quantity:        "1",
				TxHash:          "0xabc123",
				BlockNumber:     1000,
				Timestamp:       time.Now(),
				TxIndex:         0,
			},
			expected: false,
		},
		{
			name: "invalid transfer - zero to address",
			event: BlockchainEvent{
				Chain:           ChainEthereumMainnet,
				Standard:        StandardERC721,
				ContractAddress: validEthereumAddress,
				TokenNumber:     "123",
				EventType:       EventTypeTransfer,
				FromAddress:     stringPtr(validEthereumAddress),
				ToAddress:       stringPtr(zeroAddress),
				Quantity:        "1",
				TxHash:          "0xabc123",
				BlockNumber:     1000,
				Timestamp:       time.Now(),
				TxIndex:         0,
			},
			expected: false,
		},
		// Valid mint events
		{
			name: "valid mint - nil from address",
			event: BlockchainEvent{
				Chain:           ChainEthereumMainnet,
				Standard:        StandardERC721,
				ContractAddress: validEthereumAddress,
				TokenNumber:     "123",
				EventType:       EventTypeMint,
				FromAddress:     nil,
				ToAddress:       stringPtr(validEthereumAddress2),
				Quantity:        "1",
				TxHash:          "0xabc123",
				BlockNumber:     1000,
				Timestamp:       time.Now(),
				TxIndex:         0,
			},
			expected: true,
		},
		{
			name: "valid mint - empty from address",
			event: BlockchainEvent{
				Chain:           ChainEthereumMainnet,
				Standard:        StandardERC721,
				ContractAddress: validEthereumAddress,
				TokenNumber:     "123",
				EventType:       EventTypeMint,
				FromAddress:     stringPtr(emptyAddress),
				ToAddress:       stringPtr(validEthereumAddress2),
				Quantity:        "1",
				TxHash:          "0xabc123",
				BlockNumber:     1000,
				Timestamp:       time.Now(),
				TxIndex:         0,
			},
			expected: true,
		},
		{
			name: "valid mint - zero from address",
			event: BlockchainEvent{
				Chain:           ChainEthereumMainnet,
				Standard:        StandardERC721,
				ContractAddress: validEthereumAddress,
				TokenNumber:     "123",
				EventType:       EventTypeMint,
				FromAddress:     stringPtr(zeroAddress),
				ToAddress:       stringPtr(validEthereumAddress2),
				Quantity:        "1",
				TxHash:          "0xabc123",
				BlockNumber:     1000,
				Timestamp:       time.Now(),
				TxIndex:         0,
			},
			expected: true,
		},
		// Invalid mint events
		{
			name: "invalid mint - non-zero from address",
			event: BlockchainEvent{
				Chain:           ChainEthereumMainnet,
				Standard:        StandardERC721,
				ContractAddress: validEthereumAddress,
				TokenNumber:     "123",
				EventType:       EventTypeMint,
				FromAddress:     stringPtr(validEthereumAddress),
				ToAddress:       stringPtr(validEthereumAddress),
				Quantity:        "1",
				TxHash:          "0xabc123",
				BlockNumber:     1000,
				Timestamp:       time.Now(),
				TxIndex:         0,
			},
			expected: false,
		},
		{
			name: "invalid mint - nil to address",
			event: BlockchainEvent{
				Chain:           ChainEthereumMainnet,
				Standard:        StandardERC721,
				ContractAddress: validEthereumAddress,
				TokenNumber:     "123",
				EventType:       EventTypeMint,
				FromAddress:     nil,
				ToAddress:       nil,
				Quantity:        "1",
				TxHash:          "0xabc123",
				BlockNumber:     1000,
				Timestamp:       time.Now(),
				TxIndex:         0,
			},
			expected: false,
		},
		{
			name: "invalid mint - zero to address",
			event: BlockchainEvent{
				Chain:           ChainEthereumMainnet,
				Standard:        StandardERC721,
				ContractAddress: validEthereumAddress,
				TokenNumber:     "123",
				EventType:       EventTypeMint,
				FromAddress:     nil,
				ToAddress:       stringPtr(zeroAddress),
				Quantity:        "1",
				TxHash:          "0xabc123",
				BlockNumber:     1000,
				Timestamp:       time.Now(),
				TxIndex:         0,
			},
			expected: false,
		},
		// Valid burn events
		{
			name: "valid burn - nil to address",
			event: BlockchainEvent{
				Chain:           ChainEthereumMainnet,
				Standard:        StandardERC721,
				ContractAddress: validEthereumAddress,
				TokenNumber:     "123",
				EventType:       EventTypeBurn,
				FromAddress:     stringPtr(validEthereumAddress2),
				ToAddress:       nil,
				Quantity:        "1",
				TxHash:          "0xabc123",
				BlockNumber:     1000,
				Timestamp:       time.Now(),
				TxIndex:         0,
			},
			expected: true,
		},
		{
			name: "valid burn - zero to address",
			event: BlockchainEvent{
				Chain:           ChainEthereumMainnet,
				Standard:        StandardERC721,
				ContractAddress: validEthereumAddress,
				TokenNumber:     "123",
				EventType:       EventTypeBurn,
				FromAddress:     stringPtr(validEthereumAddress2),
				ToAddress:       stringPtr(zeroAddress),
				Quantity:        "1",
				TxHash:          "0xabc123",
				BlockNumber:     1000,
				Timestamp:       time.Now(),
				TxIndex:         0,
			},
			expected: true,
		},
		// Invalid burn events
		{
			name: "invalid burn - nil from address",
			event: BlockchainEvent{
				Chain:           ChainEthereumMainnet,
				Standard:        StandardERC721,
				ContractAddress: validEthereumAddress,
				TokenNumber:     "123",
				EventType:       EventTypeBurn,
				FromAddress:     nil,
				ToAddress:       nil,
				Quantity:        "1",
				TxHash:          "0xabc123",
				BlockNumber:     1000,
				Timestamp:       time.Now(),
				TxIndex:         0,
			},
			expected: false,
		},
		{
			name: "invalid burn - zero from address",
			event: BlockchainEvent{
				Chain:           ChainEthereumMainnet,
				Standard:        StandardERC721,
				ContractAddress: validEthereumAddress,
				TokenNumber:     "123",
				EventType:       EventTypeBurn,
				FromAddress:     stringPtr(zeroAddress),
				ToAddress:       nil,
				Quantity:        "1",
				TxHash:          "0xabc123",
				BlockNumber:     1000,
				Timestamp:       time.Now(),
				TxIndex:         0,
			},
			expected: false,
		},
		{
			name: "invalid burn - non-zero to address",
			event: BlockchainEvent{
				Chain:           ChainEthereumMainnet,
				Standard:        StandardERC721,
				ContractAddress: validEthereumAddress,
				TokenNumber:     "123",
				EventType:       EventTypeBurn,
				FromAddress:     stringPtr(validEthereumAddress),
				ToAddress:       stringPtr(validEthereumAddress),
				Quantity:        "1",
				TxHash:          "0xabc123",
				BlockNumber:     1000,
				Timestamp:       time.Now(),
				TxIndex:         0,
			},
			expected: false,
		},
		// Valid metadata_update events
		{
			name: "valid metadata_update",
			event: BlockchainEvent{
				Chain:           ChainEthereumMainnet,
				Standard:        StandardERC721,
				ContractAddress: validEthereumAddress,
				TokenNumber:     "123",
				EventType:       EventTypeMetadataUpdate,
				FromAddress:     nil,
				ToAddress:       nil,
				Quantity:        "1",
				TxHash:          "0xabc123",
				BlockNumber:     1000,
				Timestamp:       time.Now(),
				TxIndex:         0,
			},
			expected: true,
		},
		// Invalid metadata_update events
		{
			name: "invalid metadata_update - has from address",
			event: BlockchainEvent{
				Chain:           ChainEthereumMainnet,
				Standard:        StandardERC721,
				ContractAddress: validEthereumAddress,
				TokenNumber:     "123",
				EventType:       EventTypeMetadataUpdate,
				FromAddress:     stringPtr(validEthereumAddress),
				ToAddress:       nil,
				Quantity:        "1",
				TxHash:          "0xabc123",
				BlockNumber:     1000,
				Timestamp:       time.Now(),
				TxIndex:         0,
			},
			expected: false,
		},
		// Valid metadata_update_range events
		{
			name: "valid metadata_update_range",
			event: BlockchainEvent{
				Chain:           ChainEthereumMainnet,
				Standard:        StandardERC721,
				ContractAddress: validEthereumAddress,
				TokenNumber:     "100",
				ToTokenNumber:   "200",
				EventType:       EventTypeMetadataUpdateRange,
				FromAddress:     nil,
				ToAddress:       nil,
				Quantity:        "1",
				TxHash:          "0xabc123",
				BlockNumber:     1000,
				Timestamp:       time.Now(),
				TxIndex:         0,
			},
			expected: true,
		},
		// Invalid metadata_update_range events
		{
			name: "invalid metadata_update_range - empty token number",
			event: BlockchainEvent{
				Chain:           ChainEthereumMainnet,
				Standard:        StandardERC721,
				ContractAddress: validEthereumAddress,
				TokenNumber:     "",
				ToTokenNumber:   "200",
				EventType:       EventTypeMetadataUpdateRange,
				FromAddress:     nil,
				ToAddress:       nil,
				Quantity:        "1",
				TxHash:          "0xabc123",
				BlockNumber:     1000,
				Timestamp:       time.Now(),
				TxIndex:         0,
			},
			expected: false,
		},
		{
			name: "invalid metadata_update_range - empty to token number",
			event: BlockchainEvent{
				Chain:           ChainEthereumMainnet,
				Standard:        StandardERC721,
				ContractAddress: validEthereumAddress,
				TokenNumber:     "100",
				ToTokenNumber:   "",
				EventType:       EventTypeMetadataUpdateRange,
				FromAddress:     nil,
				ToAddress:       nil,
				Quantity:        "1",
				TxHash:          "0xabc123",
				BlockNumber:     1000,
				Timestamp:       time.Now(),
				TxIndex:         0,
			},
			expected: false,
		},
		{
			name: "invalid metadata_update_range - invalid to token number",
			event: BlockchainEvent{
				Chain:           ChainEthereumMainnet,
				Standard:        StandardERC721,
				ContractAddress: validEthereumAddress,
				TokenNumber:     "100",
				ToTokenNumber:   "abc",
				EventType:       EventTypeMetadataUpdateRange,
				FromAddress:     nil,
				ToAddress:       nil,
				Quantity:        "1",
				TxHash:          "0xabc123",
				BlockNumber:     1000,
				Timestamp:       time.Now(),
				TxIndex:         0,
			},
			expected: false,
		},
		// Invalid quantity
		{
			name: "invalid - zero quantity",
			event: BlockchainEvent{
				Chain:           ChainEthereumMainnet,
				Standard:        StandardERC721,
				ContractAddress: validEthereumAddress,
				TokenNumber:     "123",
				EventType:       EventTypeTransfer,
				FromAddress:     stringPtr(validEthereumAddress),
				ToAddress:       stringPtr(validEthereumAddress),
				Quantity:        "0",
				TxHash:          "0xabc123",
				BlockNumber:     1000,
				Timestamp:       time.Now(),
				TxIndex:         0,
			},
			expected: false,
		},
		{
			name: "invalid - invalid quantity format",
			event: BlockchainEvent{
				Chain:           ChainEthereumMainnet,
				Standard:        StandardERC721,
				ContractAddress: validEthereumAddress,
				TokenNumber:     "123",
				EventType:       EventTypeTransfer,
				FromAddress:     stringPtr(validEthereumAddress),
				ToAddress:       stringPtr(validEthereumAddress),
				Quantity:        "abc",
				TxHash:          "0xabc123",
				BlockNumber:     1000,
				Timestamp:       time.Now(),
				TxIndex:         0,
			},
			expected: false,
		},
		// Invalid event type
		{
			name: "invalid - unknown event type",
			event: BlockchainEvent{
				Chain:           ChainEthereumMainnet,
				Standard:        StandardERC721,
				ContractAddress: validEthereumAddress,
				TokenNumber:     "123",
				EventType:       EventType("unknown"),
				FromAddress:     stringPtr(validEthereumAddress),
				ToAddress:       stringPtr(validEthereumAddress),
				Quantity:        "1",
				TxHash:          "0xabc123",
				BlockNumber:     1000,
				Timestamp:       time.Now(),
				TxIndex:         0,
			},
			expected: false,
		},
		// Invalid TokenCID
		{
			name: "invalid - invalid contract address",
			event: BlockchainEvent{
				Chain:           ChainEthereumMainnet,
				Standard:        StandardERC721,
				ContractAddress: "invalid",
				TokenNumber:     "123",
				EventType:       EventTypeTransfer,
				FromAddress:     stringPtr(validEthereumAddress),
				ToAddress:       stringPtr(validEthereumAddress),
				Quantity:        "1",
				TxHash:          "0xabc123",
				BlockNumber:     1000,
				Timestamp:       time.Now(),
				TxIndex:         0,
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.event.Valid()
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestBlockchainEvent_CurrentOwner(t *testing.T) {
	validAddress := "0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb"

	tests := []struct {
		name     string
		event    BlockchainEvent
		expected *string
	}{
		{
			name: "ERC721 transfer - returns to address",
			event: BlockchainEvent{
				Standard:  StandardERC721,
				ToAddress: stringPtr(validAddress),
				EventType: EventTypeTransfer,
			},
			expected: stringPtr(validAddress),
		},
		{
			name: "ERC721 mint - returns to address",
			event: BlockchainEvent{
				Standard:  StandardERC721,
				ToAddress: stringPtr(validAddress),
				EventType: EventTypeMint,
			},
			expected: stringPtr(validAddress),
		},
		{
			name: "ERC1155 transfer - returns nil",
			event: BlockchainEvent{
				Standard:  StandardERC1155,
				ToAddress: stringPtr(validAddress),
				EventType: EventTypeTransfer,
			},
			expected: nil,
		},
		{
			name: "FA2 transfer - returns nil",
			event: BlockchainEvent{
				Standard:  StandardFA2,
				ToAddress: stringPtr(validAddress),
				EventType: EventTypeTransfer,
			},
			expected: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.event.CurrentOwner()
			if tt.expected == nil {
				assert.Nil(t, result)
			} else {
				assert.NotNil(t, result)
				assert.Equal(t, *tt.expected, *result)
			}
		})
	}
}

func TestBlockchainEvent_TokenCID(t *testing.T) {
	validEthereumAddress := "0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb"
	validTezosAddress := "KT1BvXTW1XqhE1GHTRKRvz8w3a7X5f5NqEZr"

	tests := []struct {
		name     string
		event    BlockchainEvent
		expected TokenCID
	}{
		{
			name: "ERC721 ethereum mainnet",
			event: BlockchainEvent{
				Chain:           ChainEthereumMainnet,
				Standard:        StandardERC721,
				ContractAddress: validEthereumAddress,
				TokenNumber:     "123",
			},
			expected: TokenCID("eip155:1:erc721:" + validEthereumAddress + ":123"),
		},
		{
			name: "ERC1155 ethereum sepolia",
			event: BlockchainEvent{
				Chain:           ChainEthereumSepolia,
				Standard:        StandardERC1155,
				ContractAddress: validEthereumAddress,
				TokenNumber:     "456",
			},
			expected: TokenCID("eip155:11155111:erc1155:" + validEthereumAddress + ":456"),
		},
		{
			name: "FA2 tezos mainnet",
			event: BlockchainEvent{
				Chain:           ChainTezosMainnet,
				Standard:        StandardFA2,
				ContractAddress: validTezosAddress,
				TokenNumber:     "789",
			},
			expected: TokenCID("tezos:mainnet:fa2:" + validTezosAddress + ":789"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.event.TokenCID()
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestTokenCID_String(t *testing.T) {
	tests := []struct {
		name     string
		tokenCID TokenCID
		expected string
	}{
		{
			name:     "valid token CID",
			tokenCID: TokenCID("eip155:1:erc721:0xabc:123"),
			expected: "eip155:1:erc721:0xabc:123",
		},
		{
			name:     "empty token CID",
			tokenCID: TokenCID(""),
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.tokenCID.String()
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestTokenCID_Parse(t *testing.T) {
	tests := []struct {
		name          string
		tokenCID      TokenCID
		expectedChain Chain
		expectedStd   ChainStandard
		expectedAddr  string
		expectedToken string
	}{
		{
			name:          "valid ERC721 ethereum mainnet",
			tokenCID:      TokenCID("eip155:1:erc721:0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb:123"),
			expectedChain: ChainEthereumMainnet,
			expectedStd:   StandardERC721,
			expectedAddr:  "0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb",
			expectedToken: "123",
		},
		{
			name:          "valid ERC1155 ethereum sepolia",
			tokenCID:      TokenCID("eip155:11155111:erc1155:0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb:456"),
			expectedChain: ChainEthereumSepolia,
			expectedStd:   StandardERC1155,
			expectedAddr:  "0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb",
			expectedToken: "456",
		},
		{
			name:          "valid FA2 tezos mainnet",
			tokenCID:      TokenCID("tezos:mainnet:fa2:KT1BvXTW1XqhE1GHTRKRvz8w3a7X5f5NqEZr:789"),
			expectedChain: ChainTezosMainnet,
			expectedStd:   StandardFA2,
			expectedAddr:  "KT1BvXTW1XqhE1GHTRKRvz8w3a7X5f5NqEZr",
			expectedToken: "789",
		},
		{
			name:          "invalid - too few parts",
			tokenCID:      TokenCID("eip155:1:erc721"),
			expectedChain: Chain(""),
			expectedStd:   ChainStandard(""),
			expectedAddr:  "",
			expectedToken: "",
		},
		{
			name:          "invalid - empty",
			tokenCID:      TokenCID(""),
			expectedChain: Chain(""),
			expectedStd:   ChainStandard(""),
			expectedAddr:  "",
			expectedToken: "",
		},
		{
			name:          "invalid - missing token number",
			tokenCID:      TokenCID("eip155:1:erc721:0xabc"),
			expectedChain: Chain(""),
			expectedStd:   ChainStandard(""),
			expectedAddr:  "",
			expectedToken: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			chain, std, addr, token := tt.tokenCID.Parse()
			assert.Equal(t, tt.expectedChain, chain)
			assert.Equal(t, tt.expectedStd, std)
			assert.Equal(t, tt.expectedAddr, addr)
			assert.Equal(t, tt.expectedToken, token)
		})
	}
}

func TestTokenCID_Valid(t *testing.T) {
	// Use known valid Ethereum address (42 chars including 0x)
	validEthereumAddress := "0x396343362be2A4dA1cE0C1C210945346fb82Aa49"
	validTezosAddress := "KT1BvXTW1XqhE1GHTRKRvz8w3a7X5f5NqEZr"

	tests := []struct {
		name     string
		tokenCID TokenCID
		expected bool
	}{
		{
			name:     "valid ERC721 ethereum mainnet",
			tokenCID: TokenCID("eip155:1:erc721:" + validEthereumAddress + ":123"),
			expected: true,
		},
		{
			name:     "valid ERC1155 ethereum sepolia",
			tokenCID: TokenCID("eip155:11155111:erc1155:" + validEthereumAddress + ":456"),
			expected: true,
		},
		{
			name:     "valid FA2 tezos mainnet",
			tokenCID: TokenCID("tezos:mainnet:fa2:" + validTezosAddress + ":789"),
			expected: true,
		},
		{
			name:     "valid FA2 tezos ghostnet",
			tokenCID: TokenCID("tezos:ghostnet:fa2:" + validTezosAddress + ":999"),
			expected: true,
		},
		{
			name:     "invalid - wrong standard for ethereum",
			tokenCID: TokenCID("eip155:1:fa2:" + validEthereumAddress + ":123"),
			expected: false,
		},
		{
			name:     "invalid - wrong standard for tezos",
			tokenCID: TokenCID("tezos:mainnet:erc721:" + validTezosAddress + ":123"),
			expected: false,
		},
		{
			name:     "invalid - invalid ethereum address",
			tokenCID: TokenCID("eip155:1:erc721:invalid:123"),
			expected: false,
		},
		{
			name:     "invalid - invalid tezos address (not KT1)",
			tokenCID: TokenCID("tezos:mainnet:fa2:tz1VSUr8wwNhLAzempoch5d6hLRiTh8Cjcjb:123"),
			expected: false,
		},
		{
			name:     "invalid - invalid token number (non-numeric)",
			tokenCID: TokenCID("eip155:1:erc721:" + validEthereumAddress + ":abc"),
			expected: false,
		},
		{
			name:     "invalid - empty token number",
			tokenCID: TokenCID("eip155:1:erc721:" + validEthereumAddress + ":"),
			expected: false,
		},
		{
			name:     "invalid - invalid chain",
			tokenCID: TokenCID("invalid:chain:erc721:" + validEthereumAddress + ":123"),
			expected: false,
		},
		{
			name:     "invalid - empty",
			tokenCID: TokenCID(""),
			expected: false,
		},
		{
			name:     "invalid - too few parts",
			tokenCID: TokenCID("eip155:1:erc721"),
			expected: false,
		},
		{
			name:     "valid - zero token number",
			tokenCID: TokenCID("eip155:1:erc721:" + validEthereumAddress + ":0"),
			expected: true,
		},
		{
			name:     "valid - large token number",
			tokenCID: TokenCID("eip155:1:erc721:" + validEthereumAddress + ":99999999999999999999999999999999999999999999999999999999999999999999999999999"),
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.tokenCID.Valid()
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestNewTokenCID(t *testing.T) {
	validEthereumAddress := "0x99fc8ad516FBCC9bA3123D56e63A35d05AA9eFB8"
	validEthereumAddressNormalized := "0x99fc8AD516FBCC9bA3123D56e63A35d05AA9EFB8"
	lowercaseEthereumAddress := "0x99fc8ad516fbcc9ba3123d56e63a35d05aa9efb8"
	validTezosAddress := "KT1BvXTW1XqhE1GHTRKRvz8w3a7X5f5NqEZr"

	tests := []struct {
		name         string
		chain        Chain
		standard     ChainStandard
		contractAddr string
		tokenNumber  string
		expected     TokenCID
	}{
		{
			name:         "ERC721 ethereum mainnet - already normalized",
			chain:        ChainEthereumMainnet,
			standard:     StandardERC721,
			contractAddr: validEthereumAddressNormalized,
			tokenNumber:  "123",
			expected:     TokenCID("eip155:1:erc721:" + validEthereumAddressNormalized + ":123"),
		},
		{
			name:         "ERC721 ethereum mainnet - normalizes mixed case address",
			chain:        ChainEthereumMainnet,
			standard:     StandardERC721,
			contractAddr: validEthereumAddress,
			tokenNumber:  "123",
			expected:     TokenCID("eip155:1:erc721:" + validEthereumAddressNormalized + ":123"),
		},
		{
			name:         "ERC721 ethereum mainnet - normalizes lowercase address",
			chain:        ChainEthereumMainnet,
			standard:     StandardERC721,
			contractAddr: lowercaseEthereumAddress,
			tokenNumber:  "123",
			expected:     TokenCID("eip155:1:erc721:" + validEthereumAddressNormalized + ":123"),
		},
		{
			name:         "ERC1155 ethereum sepolia - normalizes address",
			chain:        ChainEthereumSepolia,
			standard:     StandardERC1155,
			contractAddr: validEthereumAddress,
			tokenNumber:  "456",
			expected:     TokenCID("eip155:11155111:erc1155:" + validEthereumAddressNormalized + ":456"),
		},
		{
			name:         "FA2 tezos mainnet - no normalization",
			chain:        ChainTezosMainnet,
			standard:     StandardFA2,
			contractAddr: validTezosAddress,
			tokenNumber:  "789",
			expected:     TokenCID("tezos:mainnet:fa2:" + validTezosAddress + ":789"),
		},
		{
			name:         "FA2 tezos ghostnet - no normalization",
			chain:        ChainTezosGhostnet,
			standard:     StandardFA2,
			contractAddr: validTezosAddress,
			tokenNumber:  "999",
			expected:     TokenCID("tezos:ghostnet:fa2:" + validTezosAddress + ":999"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := NewTokenCID(tt.chain, tt.standard, tt.contractAddr, tt.tokenNumber)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestTransferEventType(t *testing.T) {
	validAddress := "0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb"
	zeroAddress := ETHEREUM_ZERO_ADDRESS
	emptyAddress := ""

	tests := []struct {
		name     string
		from     *string
		to       *string
		expected EventType
	}{
		{
			name:     "mint - nil from",
			from:     nil,
			to:       stringPtr(validAddress),
			expected: EventTypeMint,
		},
		{
			name:     "mint - empty from",
			from:     stringPtr(emptyAddress),
			to:       stringPtr(validAddress),
			expected: EventTypeMint,
		},
		{
			name:     "mint - zero from",
			from:     stringPtr(zeroAddress),
			to:       stringPtr(validAddress),
			expected: EventTypeMint,
		},
		{
			name:     "burn - nil to",
			from:     stringPtr(validAddress),
			to:       nil,
			expected: EventTypeBurn,
		},
		{
			name:     "burn - empty to",
			from:     stringPtr(validAddress),
			to:       stringPtr(emptyAddress),
			expected: EventTypeBurn,
		},
		{
			name:     "burn - zero to",
			from:     stringPtr(validAddress),
			to:       stringPtr(zeroAddress),
			expected: EventTypeBurn,
		},
		{
			name:     "transfer - both addresses valid",
			from:     stringPtr(validAddress),
			to:       stringPtr("0x396343362be2A4dA1cE0C1C210945346fb82Aa49"),
			expected: EventTypeTransfer,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := TransferEventType(tt.from, tt.to)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestNormalizeAddress(t *testing.T) {
	tests := []struct {
		name     string
		address  string
		expected string
	}{
		{
			name:     "ethereum address lowercase",
			address:  "0x742d35cc6634c0532925a3b844bc9e7595f0beb",
			expected: "0x0742D35CC6634c0532925A3b844bc9E7595f0Beb", // Checksummed format
		},
		{
			name:     "ethereum address uppercase 0X prefix unchanged",
			address:  "0X742D35CC6634C0532925A3B844BC9E7595F0BEB",
			expected: "0X742D35CC6634C0532925A3B844BC9E7595F0BEB", // Not 0x prefix, so unchanged
		},
		{
			name:     "ethereum address mixed case",
			address:  "0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb",
			expected: "0x0742D35CC6634c0532925A3b844bc9E7595f0Beb", // Checksummed format
		},
		{
			name:     "tezos address unchanged",
			address:  "tz1VSUr8wwNhLAzempoch5d6hLRiTh8Cjcjb",
			expected: "tz1VSUr8wwNhLAzempoch5d6hLRiTh8Cjcjb",
		},
		{
			name:     "tezos contract address unchanged",
			address:  "KT1BvXTW1XqhE1GHTRKRvz8w3a7X5f5NqEZr",
			expected: "KT1BvXTW1XqhE1GHTRKRvz8w3a7X5f5NqEZr",
		},
		{
			name:     "empty address unchanged",
			address:  "",
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := NormalizeAddress(tt.address)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestNormalizeAddresses(t *testing.T) {
	tests := []struct {
		name      string
		addresses []string
		expected  []string
	}{
		{
			name:      "empty slice",
			addresses: []string{},
			expected:  []string{},
		},
		{
			name:      "single ethereum address",
			addresses: []string{"0x742d35cc6634c0532925a3b844bc9e7595f0beb"},
			expected:  []string{"0x0742D35CC6634c0532925A3b844bc9E7595f0Beb"},
		},
		{
			name: "multiple ethereum addresses",
			addresses: []string{
				"0x742d35cc6634c0532925a3b844bc9e7595f0beb",
				"0x396343362be2A4dA1cE0C1C210945346fb82Aa49",
			},
			expected: []string{
				"0x0742D35CC6634c0532925A3b844bc9E7595f0Beb",
				"0x396343362be2A4dA1cE0C1C210945346fb82Aa49", // Checksummed format
			},
		},
		{
			name: "mixed ethereum and tezos addresses",
			addresses: []string{
				"0x742d35cc6634c0532925a3b844bc9e7595f0beb",
				"tz1VSUr8wwNhLAzempoch5d6hLRiTh8Cjcjb",
			},
			expected: []string{
				"0x0742D35CC6634c0532925A3b844bc9E7595f0Beb",
				"tz1VSUr8wwNhLAzempoch5d6hLRiTh8Cjcjb",
			},
		},
		{
			name: "tezos addresses unchanged",
			addresses: []string{
				"tz1VSUr8wwNhLAzempoch5d6hLRiTh8Cjcjb",
				"KT1BvXTW1XqhE1GHTRKRvz8w3a7X5f5NqEZr",
			},
			expected: []string{
				"tz1VSUr8wwNhLAzempoch5d6hLRiTh8Cjcjb",
				"KT1BvXTW1XqhE1GHTRKRvz8w3a7X5f5NqEZr",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a copy to avoid modifying the original
			addresses := make([]string, len(tt.addresses))
			copy(addresses, tt.addresses)

			result := NormalizeAddresses(addresses)
			assert.Equal(t, tt.expected, result)
			// Verify that the original slice was modified in place
			assert.Equal(t, tt.expected, addresses)
		})
	}
}

// Helper function for tests
func stringPtr(s string) *string {
	return &s
}
