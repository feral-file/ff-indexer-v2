package domain

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewDID(t *testing.T) {
	tests := []struct {
		name     string
		address  string
		chain    Chain
		expected DID
	}{
		{
			name:     "ethereum mainnet address",
			address:  "0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb",
			chain:    ChainEthereumMainnet,
			expected: DID("did:pkh:eip155:1:0x742d35cc6634c0532925a3b844bc9e7595f0beb"),
		},
		{
			name:     "ethereum sepolia address uppercase",
			address:  "0x396343362be2A4dA1cE0C1C210945346fb82Aa49",
			chain:    ChainEthereumSepolia,
			expected: DID("did:pkh:eip155:11155111:0x396343362be2a4da1ce0c1c210945346fb82aa49"),
		},
		{
			name:     "tezos mainnet address",
			address:  "tz1VSUr8wwNhLAzempoch5d6hLRiTh8Cjcjb",
			chain:    ChainTezosMainnet,
			expected: DID("did:pkh:tezos:mainnet:tz1vsur8wwnhlazempoch5d6hlrith8cjcjb"),
		},
		{
			name:     "tezos ghostnet address",
			address:  "KT1BvXTW1XqhE1GHTRKRvz8w3a7X5f5NqEZr",
			chain:    ChainTezosGhostnet,
			expected: DID("did:pkh:tezos:ghostnet:kt1bvxtw1xqhe1ghtrkrvz8w3a7x5f5nqezr"),
		},
		{
			name:     "empty address",
			address:  "",
			chain:    ChainEthereumMainnet,
			expected: DID("did:pkh:eip155:1:"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := NewDID(tt.address, tt.chain)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestDID_String(t *testing.T) {
	tests := []struct {
		name     string
		did      DID
		expected string
	}{
		{
			name:     "valid DID",
			did:      DID("did:pkh:eip155:1:0x742d35cc6634c0532925a3b844bc9e7595f0beb"),
			expected: "did:pkh:eip155:1:0x742d35cc6634c0532925a3b844bc9e7595f0beb",
		},
		{
			name:     "empty DID",
			did:      DID(""),
			expected: "",
		},
		{
			name:     "tezos DID",
			did:      DID("did:pkh:tezos:mainnet:tz1vsur8wwnhlazempoc5d6hlrith8cjcjb"),
			expected: "did:pkh:tezos:mainnet:tz1vsur8wwnhlazempoc5d6hlrith8cjcjb",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.did.String()
			assert.Equal(t, tt.expected, result)
		})
	}
}
