package jobs

import (
	"fmt"
	"time"
)

// RescheduleError is returned from a registered job handler to move the job back to pending
// with a new earliest run time (e.g. quota reset). The worker maps this to RescheduleJob.
//
// Reason: A typed error carries the run_after instant without ad hoc string parsing.
// Trade-offs: Callers use errors.As on *RescheduleError, not a package-level sentinel, so the
// time value is first-class. Constraints: Time is stored in UTC.
type RescheduleError struct {
	// At is the minimum time before the job may be claimed again.
	At time.Time
}

// Error implements the error interface.
func (e *RescheduleError) Error() string {
	if e == nil {
		return "reschedule: <nil>"
	}
	return fmt.Sprintf("reschedule job at %s", e.At.UTC().Format(time.RFC3339Nano))
}

// ErrReschedule wraps the given instant as a *RescheduleError. Handlers that need to delay work
// return this value; any other error fails the job.
func ErrReschedule(at time.Time) error {
	if at.IsZero() {
		at = time.Now().UTC()
	} else {
		at = at.UTC()
	}
	return &RescheduleError{At: at}
}
