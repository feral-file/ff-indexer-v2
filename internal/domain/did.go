package domain

import (
	"fmt"
	"strings"
)

// DID represents a Decentralized Identifier (W3C standard)
type DID string

// NewDID creates a new DID from a string
// Reference: https://github.com/w3c-ccg/did-pkh
func NewDID(address string, chain Chain) DID {
	return DID(fmt.Sprintf("did:pkh:%s:%s", strings.ToLower(string(chain)), strings.ToLower(address)))
}

// String returns the string representation of the DID
func (d DID) String() string {
	return string(d)
}
