package helpers_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/feral-file/ff-indexer-v2/internal/providers/ethereum/helpers"
)

func TestIsTooManyResultsError(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"alchemy result cap", errors.New("query returned more than 10000 results"), true},
		{"generic too many results", errors.New("too many results"), true},
		// Observed in production: provider caps the queried block span, and the
		// error arrives wrapped by the retry layer.
		{
			"range cap wrapped by retry layer",
			fmt.Errorf("ethereum operation FilterLogs failed after retries: permanent error: range 9999999 exceeds limit of 10000"),
			true,
		},
		{"block range too wide", errors.New("block range is too wide"), true},
		{"range too large", errors.New("range too large"), true},
		{"unrelated error", errors.New("execution reverted"), false},
		{"connection error", errors.New("connection refused"), false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, helpers.IsTooManyResultsError(tc.err))
		})
	}
}
