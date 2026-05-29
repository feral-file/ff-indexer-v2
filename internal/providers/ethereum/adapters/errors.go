package adapters

import "errors"

// ErrUnsupportedContractStandard is returned when no configured or standard adapter matches.
var ErrUnsupportedContractStandard = errors.New("unsupported contract standard")

// ErrConfiguredStandardMismatch is returned when a lookup standard does not match a configured override.
var ErrConfiguredStandardMismatch = errors.New("configured contract standard mismatch")

// ErrUnknownEvent is returned when an adapter cannot parse a log.
var ErrUnknownEvent = errors.New("unknown event")

// ErrUnconfiguredContract is returned when a known custom signature is emitted by a contract we don't index.
// This is expected noise in the event stream and should be debug-logged and skipped.
var ErrUnconfiguredContract = errors.New("known signature from unconfigured contract")

// ErrUnexpectedEvent is returned when the event filter sent us a log that no adapter recognizes.
// This indicates a potential filter misconfiguration and should be error-logged but tolerated.
var ErrUnexpectedEvent = errors.New("unexpected event signature")

// ErrNeedsRepair is returned by parseEventInternal when a parsed event requires post-parse repair
// (e.g., corrupted CryptoPunks PunkBought events with zero toAddress/value that need receipt lookup).
var ErrNeedsRepair = errors.New("event needs repair")
