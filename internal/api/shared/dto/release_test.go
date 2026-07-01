package dto

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReleaseResponseJSONIncludesNameAndTotalMints(t *testing.T) {
	t.Parallel()

	name := "Fidenza by Tyler Hobbs"
	totalMints := int64(999)
	response := ReleaseResponse{
		ID:              42,
		Vendor:          "artblocks",
		VendorReleaseID: "1-0xa7d8d9ef8d8ce8992df33d8b8cf4aebabd5bd270-78",
		Name:            &name,
		TotalMints:      &totalMints,
	}

	data, err := json.Marshal(response)
	require.NoError(t, err)

	var decoded map[string]interface{}
	require.NoError(t, json.Unmarshal(data, &decoded))

	assert.Equal(t, "Fidenza by Tyler Hobbs", decoded["name"])
	assert.Equal(t, float64(999), decoded["total_mints"])
}
