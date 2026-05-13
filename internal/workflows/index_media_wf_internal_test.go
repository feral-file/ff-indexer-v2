//go:build cgo

package workflows

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIsExpectedMediaSourceTimeout(t *testing.T) {
	t.Parallel()

	err := fmt.Errorf("failed to process media file: failed to transform and upload image: transformation failed: failed to get response: %w", context.DeadlineExceeded)

	require.True(t, isExpectedMediaSourceTimeout(err))
}

func TestIsExpectedMediaSourceTimeout_IgnoresOtherDeadlineFailures(t *testing.T) {
	t.Parallel()

	err := fmt.Errorf("failed to check existing media asset: %w", context.DeadlineExceeded)

	require.False(t, isExpectedMediaSourceTimeout(err))
}
