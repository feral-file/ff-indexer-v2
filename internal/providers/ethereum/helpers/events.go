package helpers

import (
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
)

// Standard event topic signatures used across adapters, client, and subscriber.
var (
	// Transfer is shared by ERC20 and ERC721.
	TransferEventSignature = crypto.Keccak256Hash([]byte("Transfer(address,address,uint256)"))

	// ERC1155TransferSingle(address indexed operator, address indexed from, address indexed to, uint256 id, uint256 value).
	ERC1155TransferSingleEventSignature = crypto.Keccak256Hash([]byte("TransferSingle(address,address,address,uint256,uint256)"))

	// ERC1155TransferBatch(address indexed operator, address indexed from, address indexed to, uint256[] ids, uint256[] values).
	ERC1155TransferBatchEventSignature = crypto.Keccak256Hash([]byte("TransferBatch(address,address,address,uint256[],uint256[])"))

	// EIP4906MetadataUpdate(uint256 _tokenId).
	EIP4906MetadataUpdateEventSignature = crypto.Keccak256Hash([]byte("MetadataUpdate(uint256)"))

	// EIP4906BatchMetadataUpdate(uint256 _fromTokenId, uint256 _toTokenId).
	EIP4906BatchMetadataUpdateEventSignature = crypto.Keccak256Hash([]byte("BatchMetadataUpdate(uint256,uint256)"))

	// ERC1155URI(string _value, uint256 indexed _id).
	ERC1155URIEventSignature = crypto.Keccak256Hash([]byte("URI(string,uint256)"))
)

// StandardEventSignatures returns the default topic filter set for standard token events.
func StandardEventSignatures() []common.Hash {
	return []common.Hash{
		TransferEventSignature,
		ERC1155TransferSingleEventSignature,
		ERC1155TransferBatchEventSignature,
		EIP4906MetadataUpdateEventSignature,
		EIP4906BatchMetadataUpdateEventSignature,
		ERC1155URIEventSignature,
	}
}
