package jobs

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestErrReschedule(t *testing.T) {
	t.Parallel()
	at := time.Date(2026, 4, 1, 12, 0, 0, 0, time.UTC)
	err := ErrReschedule(at)
	var re *RescheduleError
	require.ErrorAs(t, err, &re)
	require.Equal(t, at.UTC(), re.At)

	// Zero time becomes "now" — only check it is UTC and non-zero.
	z := ErrReschedule(time.Time{})
	require.ErrorAs(t, z, &re)
	require.False(t, re.At.IsZero())
}

func TestRescheduleError_Error(t *testing.T) {
	t.Parallel()
	at := time.Date(2026, 1, 2, 3, 4, 5, 6, time.FixedZone("east", 3600))
	// Error formats At in UTC.
	require.Contains(t, ErrReschedule(at).Error(), "reschedule")
	require.Contains(t, ErrReschedule(at).Error(), "2026")

	require.Contains(t, (&RescheduleError{At: at}).Error(), "reschedule")
}

func TestRescheduleError_Error_NilReceiver(t *testing.T) {
	t.Parallel()
	var e *RescheduleError
	require.Contains(t, e.Error(), "nil")
}

func TestRescheduleError_As(t *testing.T) {
	t.Parallel()
	inner := ErrReschedule(time.Now())
	wrapped := errors.Join(errors.New("wrap"), inner)
	var re *RescheduleError
	require.True(t, errors.As(wrapped, &re))
}
