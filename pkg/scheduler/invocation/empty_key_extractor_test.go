package invocation_test

import (
	"context"
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-remote-execution/pkg/scheduler/invocation"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/stretchr/testify/require"

	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/emptypb"
)

func TestEmptyInvocationKeyExtractor(t *testing.T) {
	ctx := context.Background()

	key, err := invocation.EmptyKeyExtractor.ExtractKey(ctx, &remoteexecution.RequestMetadata{
		ToolDetails: &remoteexecution.ToolDetails{
			ToolName:    "bazel",
			ToolVersion: "4.2.1",
		},
		ToolInvocationId:        "9c9e7705-d757-4e57-b0df-58bc69c1cb51",
		CorrelatedInvocationsId: "92d71492-175e-40ba-9c8c-3b5d3b9a6808",
		ActionMnemonic:          "CppLink",
		TargetId:                "//:hello_world",
		ConfigurationId:         "cfdad5b3966911c7ca6cf551c4b64c1bbe3642f1d1f7ec373bc449671e1d5c02",
	})
	require.NoError(t, err)
	id, err := anypb.New(&emptypb.Empty{})
	require.NoError(t, err)
	testutil.RequireEqualProto(t, id, key.GetID())
}
