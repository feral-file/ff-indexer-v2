package domain

const (
	// Gateway constants
	// The unprobed fallback must be a fetching gateway: our own gateway serves
	// local pins only. Validated resolution also tries ipfs.feralfile.com.
	DEFAULT_IPFS_GATEWAY    = "https://ipfs.filebase.io"
	DEFAULT_ARWEAVE_GATEWAY = "https://arweave.net"

	// Blockchain constants
	ETHEREUM_ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"
)
