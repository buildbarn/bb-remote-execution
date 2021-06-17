// +build windows

package cleaner

import (
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func killProcess(id int) error {
	return status.Error(codes.Unimplemented, "Killing processes is not supported on this platform")
}
