package adapters

import "errors"

// ErrUnsupportedContractStandard is returned when no configured or standard adapter matches.
var ErrUnsupportedContractStandard = errors.New("unsupported contract standard")

// ErrUnknownEvent is returned when an adapter cannot parse a log.
var ErrUnknownEvent = errors.New("unknown event")
