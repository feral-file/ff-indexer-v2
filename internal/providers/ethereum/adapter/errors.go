package adapter

import "errors"

// ErrUnsupportedContractStandard is returned when no configured or standard adapter matches.
var ErrUnsupportedContractStandard = errors.New("unsupported contract standard")
