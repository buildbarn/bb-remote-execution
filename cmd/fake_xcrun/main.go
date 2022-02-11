package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"syscall"

	"github.com/spf13/pflag"
)

// On a system that has none of the components of Xcode installed, tools
// like /usr/bin/cc and /usr/bin/python3 will not be functional. They
// can be made to work by either installing full Xcode or just the Xcode
// CLTools package.
//
// This tool can be used to make the stubs in /usr/bin work without
// installing any of the Xcode components. When installed at
// /Library/Developer/CommandLineTools/usr/bin/xcrun, all invocations of
// stubs /usr/bin will be forwarded to executables in the same directory
// as xcrun.
//
// This tool can, for example, be used to forward all invocations of
// development tools in the base system to binaries placed in action
// input roots.

func main() {
	find := pflag.Bool("find", false, "Only find and print the tool path")
	pflag.SetInterspersed(false)
	pflag.Parse()
	args := pflag.Args()
	if len(args) == 0 {
		log.Fatal("Expected xcrun to be invoked with a utility name")
	}
	utilityPath := filepath.Join(filepath.Dir(os.Args[0]), args[0])
	if *find {
		if len(args) != 1 {
			log.Fatal("--find can only be called with a single argument")
		}
		fmt.Println(utilityPath)
	} else {
		log.Fatalf(
			"Failed to execute %#v: %s",
			utilityPath,
			syscall.Exec(
				utilityPath,
				append([]string{utilityPath}, args[1:]...),
				os.Environ()))
	}
}
