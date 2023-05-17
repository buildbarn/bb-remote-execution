package builder_test

import (
	"strings"
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-remote-execution/pkg/builder"
	"github.com/stretchr/testify/require"
)

func TestConvertCommandToShellScript(t *testing.T) {
	var b strings.Builder
	require.NoError(t, builder.ConvertCommandToShellScript(&remoteexecution.Command{
		Arguments: []string{"cc", "-o", "../obj/hello world.o", "hello world.c"},
		EnvironmentVariables: []*remoteexecution.Command_EnvironmentVariable{
			{Name: "LD_LIBRARY_PATH", Value: "/lib"},
			{Name: "PATH", Value: "/bin:/sbin:/usr/bin:/usr/sbin"},
			{Name: "FUNKY_CHARACTERS", Value: "~Hello$world*"},
		},
		OutputPaths:      []string{"../obj/hello world.o", "../obj/notcreated", "other/dir/bar"},
		WorkingDirectory: "src",
	}, &b))
	require.Equal(t, `#!/bin/sh
set -e
mkdir -p obj
mkdir -p src/other/dir
cd src
export LD_LIBRARY_PATH=/lib
export PATH=/bin:/sbin:/usr/bin:/usr/sbin
export FUNKY_CHARACTERS=\~Hello\$world\*
exec cc -o '../obj/hello world.o' 'hello world.c'
`, b.String())
}
