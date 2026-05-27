package adapters

import "errors"

// ErrUnsupportedContractStandard is returned when no configured or standard adapter matches.
var ErrUnsupportedContractStandard = errors.New("unsupported contract standard")

// ErrConfiguredStandardMismatch is returned when a lookup standard does not match a configured override.
var ErrConfiguredStandardMismatch = errors.New("configured contract standard mismatch")

// ErrUnknownEvent is returned when an adapter cannot parse a log.
var ErrUnknownEvent = errors.New("unknown event")
