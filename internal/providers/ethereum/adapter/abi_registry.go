package adapter

import (
	"fmt"
	"io/fs"
	"path"
	"strings"

	"github.com/ethereum/go-ethereum/accounts/abi"
)

const abiDirectory = "abis"

// ABIRegistry loads and caches contract ABIs referenced by adapter configuration.
type ABIRegistry struct {
	abis map[string]abi.ABI
}

// NewABIRegistry loads all JSON ABI files from the provided filesystem root.
func NewABIRegistry(fsys fs.FS) (*ABIRegistry, error) {
	entries, err := fs.ReadDir(fsys, abiDirectory)
	if err != nil {
		return nil, fmt.Errorf("read ABI directory: %w", err)
	}

	registry := &ABIRegistry{
		abis: make(map[string]abi.ABI, len(entries)),
	}

	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}

		name := strings.TrimSuffix(entry.Name(), ".json")
		content, err := fs.ReadFile(fsys, path.Join(abiDirectory, entry.Name()))
		if err != nil {
			return nil, fmt.Errorf("read ABI %s: %w", name, err)
		}

		parsed, err := abi.JSON(strings.NewReader(string(content)))
		if err != nil {
			return nil, fmt.Errorf("parse ABI %s: %w", name, err)
		}

		registry.abis[name] = parsed
	}

	return registry, nil
}

// Get returns a cached ABI by name.
func (r *ABIRegistry) Get(name string) (abi.ABI, error) {
	parsed, ok := r.abis[name]
	if !ok {
		return abi.ABI{}, fmt.Errorf("ABI not found: %s", name)
	}
	return parsed, nil
}

// MustGet returns a cached ABI or panics when the name is unknown.
func (r *ABIRegistry) MustGet(name string) abi.ABI {
	parsed, err := r.Get(name)
	if err != nil {
		panic(err)
	}
	return parsed
}
