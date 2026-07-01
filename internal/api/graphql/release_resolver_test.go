package graphql

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/feral-file/ff-indexer-v2/internal/api/shared/dto"
	"github.com/feral-file/ff-indexer-v2/internal/mocks"
)

func TestQueryResolverReleaseReturnsNameAndTotalMints(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockExec := mocks.NewMockAPIExecutor(ctrl)
	resolver := NewResolver(false, mockExec)

	name := "1DE94 by Raven Kwok"
	totalMints := int64(75)
	mockExec.EXPECT().
		GetRelease(gomock.Any(), uint64(7)).
		Return(&dto.ReleaseResponse{
			ID:              7,
			Vendor:          "feralfile",
			VendorReleaseID: "1f060e42-0000-0000-0000-000000000001",
			Name:            &name,
			TotalMints:      &totalMints,
		}, nil)

	result, err := resolver.Query().Release(context.Background(), Uint64(7))
	require.NoError(t, err)
	require.NotNil(t, result)
	require.NotNil(t, result.Name)
	assert.Equal(t, name, *result.Name)
	require.NotNil(t, result.TotalMints)
	assert.Equal(t, totalMints, *result.TotalMints)
}
