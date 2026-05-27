package helpers

import (
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"

	"github.com/feral-file/ff-indexer-v2/internal/domain"
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

// BaseEventFromLog builds log-derived metadata fields shared by all parsed events.
func BaseEventFromLog(chain domain.Chain, vLog types.Log) domain.BlockchainEvent {
	blockHash := vLog.BlockHash.Hex()
	return domain.BlockchainEvent{
		Chain:           chain,
		ContractAddress: vLog.Address.Hex(),
		TxHash:          vLog.TxHash.Hex(),
		BlockNumber:     vLog.BlockNumber,
		BlockHash:       &blockHash,
		TxIndex:         uint64(vLog.TxIndex), //nolint:gosec,G115
		LogIndex:        uint64(vLog.Index),   //nolint:gosec,G115
		Timestamp:       logBlockTime(vLog),
	}
}

// logBlockTime returns the block time encoded on the log via BlockTimestamp.
func logBlockTime(vLog types.Log) time.Time {
	if vLog.BlockTimestamp == 0 {
		return time.Time{}
	}
	return time.Unix(int64(vLog.BlockTimestamp), 0) //nolint:gosec,G115
}
