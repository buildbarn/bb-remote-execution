package main

import (
	_ "embed"
	"log"
	"os"
	"path/filepath"
)

//go:embed bb_runner
var bbRunner []byte

func main() {
	installationPath := "/bb"
	switch len(os.Args) {
	case 2:
		installationPath = os.Args[1]
		fallthrough
	case 1:
	default:
		log.Fatal("usage: bb_runner_installer [installation_path]")
	}

	bbRunnerPath := filepath.Join(installationPath, "bb_runner")
	os.Remove(bbRunnerPath)
	installedPath := filepath.Join(installationPath, "installed")
	os.Remove(installedPath)

	if err := os.WriteFile(bbRunnerPath, bbRunner, 0o555); err != nil {
		log.Fatal("Failed to install bb_runner: ", err)
	}
	if err := os.WriteFile(installedPath, nil, 0o444); err != nil {
		log.Fatal("Failed to install \"installed\" file: ", err)
	}
}
