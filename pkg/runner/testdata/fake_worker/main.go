package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"time"

	"github.com/google/uuid"
)

// Fake worker binary to process test requests.
func main() {
	mode := flag.String("mode", "echo", "Fake worker behavior")
	flag.Parse()
	if err := run(*mode); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(mode string) error {
	workerID := uuid.NewString()
	switch mode {
	case "echo", "exit", "truncated", "invalid-length", "oversized":
	case "exit-idle":
		return nil
	default:
		return fmt.Errorf("unknown fake worker mode %q", mode)
	}
	stdin := bufio.NewReader(os.Stdin)
	for requestIndex := 1; ; requestIndex++ {
		size, err := binary.ReadUvarint(stdin)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		request := make([]byte, int(size))
		if _, err := io.ReadFull(stdin, request); err != nil {
			return err
		}
		response := append(fmt.Appendf(nil, "%s:%d:", workerID, requestIndex), request...)
		switch mode {
		case "exit":
			os.Exit(23)
		case "truncated":
			_, err := os.Stdout.Write([]byte{3, 0xff})
			return err
		case "invalid-length":
			if _, err := os.Stdout.Write(bytes.Repeat([]byte{0x80}, binary.MaxVarintLen64)); err != nil {
				return err
			}
			time.Sleep(time.Hour)
			return nil
		case "oversized":
			if _, err := os.Stdout.Write(binary.AppendUvarint(nil, math.MaxUint64)); err != nil {
				return err
			}
			time.Sleep(time.Hour)
			return nil
		}
		if _, err := os.Stdout.Write(binary.AppendUvarint(nil, uint64(len(response)))); err != nil {
			return err
		}
		if _, err := os.Stdout.Write(response); err != nil {
			return err
		}
	}
}
