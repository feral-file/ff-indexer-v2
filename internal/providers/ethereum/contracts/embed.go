// Package contracts holds embedded contract adapter configuration and ABI files.
package contracts

import "embed"

// Files contains the default contracts.json and ABI definitions shipped with the indexer.
//
//go:embed contracts.json abis/*.json
var Files embed.FS
