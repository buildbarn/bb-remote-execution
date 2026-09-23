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
	"strings"
	"time"

	worker_pb "github.com/buildbarn/bb-remote-execution/pkg/proto/worker"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
)

// Fake worker binary to process test requests.
func main() {
	mode := flag.String("mode", "echo", "Fake worker behavior")
	persistent := flag.Bool("persistent_worker", false, "Use the persistent worker protocol")
	flag.Parse()
	if *mode == "proto" && !*persistent {
		fmt.Fprintln(os.Stderr, "Expected --persistent_worker")
		os.Exit(1)
	}
	if err := run(*mode); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(mode string) error {
	workerID := uuid.NewString()
	switch mode {
	case "echo", "proto", "wait", "exit", "exit-259", "truncated", "invalid-length", "oversized":
	case "exit-idle":
		return nil
	case "exit-startup":
		fmt.Fprintln(os.Stderr, "discarded prefix"+strings.Repeat("x", 8192)+mode)
		os.Exit(23)
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
		case "proto":
			workRequest := &worker_pb.WorkRequest{}
			if err := proto.Unmarshal(request, workRequest); err != nil {
				return err
			}
			workResponse := &worker_pb.WorkResponse{RequestId: workRequest.RequestId, Output: workerID}
			if err := compile(workRequest.Arguments); err != nil {
				workResponse.ExitCode = 1
				workResponse.Output += "\n" + err.Error()
			}
			response, err = proto.Marshal(workResponse)
			if err != nil {
				return err
			}
		case "wait":
			fmt.Fprintln(os.Stderr, "Request received")
			time.Sleep(time.Hour)
			return nil
		case "exit", "exit-259":
			fmt.Fprintln(os.Stderr, "discarded prefix"+strings.Repeat("x", 8192)+mode)
			if mode == "exit-259" {
				os.Exit(259)
			}
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

func compile(arguments []string) error {
	if len(arguments) != 2 {
		return fmt.Errorf("expected input and output paths")
	}
	contents, err := os.ReadFile(arguments[0])
	if err != nil {
		return err
	}
	return os.WriteFile(arguments[1], []byte(strings.ToUpper(string(contents))), 0o666)
}
