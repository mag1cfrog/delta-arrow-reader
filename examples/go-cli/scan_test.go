//go:build linux

package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func fixtureSchema() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: true}}, nil)
}

func TestMain(m *testing.M) {
	if mode := os.Getenv("DAR_GO_TEST_MODE"); mode != "" {
		os.Exit(runTestChild(mode))
	}
	os.Exit(m.Run())
}

func runTestChild(mode string) int {
	if mode == "main" {
		main()
		return 0
	}
	if err := os.WriteFile(os.Getenv("DAR_GO_TEST_PID"), []byte(strconv.Itoa(os.Getpid())), 0o600); err != nil {
		return 90
	}
	var expected []string
	if err := json.Unmarshal([]byte(os.Getenv("DAR_GO_TEST_ARGS")), &expected); err != nil || !reflect.DeepEqual(os.Args[1:], expected) {
		return 91
	}
	if mode == "no-output" {
		time.Sleep(30 * time.Second)
		return 92
	}
	if mode == "no-schema" {
		os.Stdout.Write([]byte{1, 2, 3})
		return 0
	}
	if mode == "noisy-partial" {
		io.WriteString(os.Stderr, strings.Repeat("private-stderr ", 100000))
	}
	schema := fixtureSchema()
	if mode == "wrong-schema" {
		schema = arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true}}, nil)
	}
	writer := ipc.NewWriter(os.Stdout, ipc.WithSchema(schema))
	if mode == "empty" || mode == "wrong-schema" {
		if writer.Close() != nil {
			return 93
		}
		return 0
	}
	builder := array.NewInt32Builder(memory.DefaultAllocator)
	defer builder.Release()
	builder.AppendValues([]int32{1, 2, 3}, nil)
	if mode == "null-id" {
		builder.AppendNull()
	}
	ids := builder.NewArray()
	defer ids.Release()
	batch := array.NewRecordBatch(schema, []arrow.Array{ids}, int64(ids.Len()))
	defer batch.Release()
	if writer.Write(batch) != nil {
		return 94
	}
	switch mode {
	case "partial", "noisy-partial":
		return 7
	case "invalid-body":
		os.Stdout.Write([]byte{1, 2, 3})
		return 0
	case "signal":
		syscall.Kill(os.Getpid(), syscall.SIGTERM)
		time.Sleep(time.Second)
		return 95
	case "endless":
		for {
			if writer.Write(batch) != nil {
				return 96
			}
		}
	}
	if writer.Close() != nil {
		return 97
	}
	if mode == "ended-error" {
		return 7
	}
	if mode == "delayed-exit" {
		time.Sleep(200 * time.Millisecond)
	}
	return 0
}

func childFixture(t *testing.T, mode string, args []string) string {
	t.Helper()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	pidFile := filepath.Join(t.TempDir(), "pid")
	encoded, err := json.Marshal(append([]string{"scan"}, args...))
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("DAR_GO_TEST_MODE", mode)
	t.Setenv("DAR_GO_TEST_PID", pidFile)
	t.Setenv("DAR_GO_TEST_ARGS", string(encoded))
	t.Cleanup(func() {
		contents, err := os.ReadFile(pidFile)
		if err != nil {
			t.Fatal("test child never started:", err)
		}
		pid, err := strconv.Atoi(string(contents))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := syscall.Wait4(pid, nil, syscall.WNOHANG, nil); !errors.Is(err, syscall.ECHILD) {
			syscall.Kill(pid, syscall.SIGKILL)
			syscall.Wait4(pid, nil, 0, nil)
			t.Errorf("child %d was not reaped: %v", pid, err)
		}
	})
	return executable
}

func TestScanSuccessAndEmptyResult(t *testing.T) {
	for _, mode := range []string{"success", "empty", "delayed-exit"} {
		t.Run(mode, func(t *testing.T) {
			args := []string{"--column", "id", "--", "-table with spaces?token=secret&x=$(false)"}
			executable := childFixture(t, mode, args)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			var ids []int32
			err := runScan(ctx, executable, args, fixtureSchema(), func(batch arrow.RecordBatch) error {
				ids = append(ids, batch.Column(0).(*array.Int32).Int32Values()...)
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			if mode == "empty" {
				if len(ids) != 0 {
					t.Fatal(ids)
				}
			} else if !reflect.DeepEqual(ids, []int32{1, 2, 3}) {
				t.Fatal(ids)
			}
		})
	}
}

func TestDecodeAndChildFailures(t *testing.T) {
	for _, mode := range []string{"partial", "ended-error", "noisy-partial", "signal", "no-schema", "invalid-body", "wrong-schema"} {
		t.Run(mode, func(t *testing.T) {
			executable := childFixture(t, mode, nil)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			var rows int64
			err := runScan(ctx, executable, nil, fixtureSchema(), func(batch arrow.RecordBatch) error {
				rows += batch.NumRows()
				return nil
			})
			var failure *scanError
			if !errors.As(err, &failure) || err.Error() != "dar scan failed" {
				t.Fatalf("unexpected error: %v", err)
			}
			if mode != "no-schema" && mode != "wrong-schema" && rows != 3 {
				t.Fatalf("partial rows: %d", rows)
			}
			if mode == "partial" || mode == "ended-error" || mode == "noisy-partial" || mode == "signal" {
				var exit *exec.ExitError
				if !errors.As(err, &exit) {
					t.Fatalf("missing child status: %v", failure.cause)
				}
				if mode == "signal" {
					if exit.ExitCode() != -1 {
						t.Fatal(exit)
					}
				} else if exit.ExitCode() != 7 {
					t.Fatal(exit)
				}
			}
			if mode == "noisy-partial" && len(failure.stderr) != maxStderrBytes {
				t.Fatalf("stderr length: %d", len(failure.stderr))
			}
		})
	}
}

func TestTimeoutAndConsumerStopReapChild(t *testing.T) {
	for _, mode := range []string{"no-output", "endless"} {
		t.Run(mode, func(t *testing.T) {
			executable := childFixture(t, mode, nil)
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			stop := errors.New("consumer stopped")
			err := runScan(ctx, executable, nil, fixtureSchema(), func(arrow.RecordBatch) error { return stop })
			if mode == "no-output" {
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("missing deadline error: %v", err)
				}
			} else if !errors.Is(err, stop) || ctx.Err() != nil {
				t.Fatalf("early stop did not complete before deadline: %v", err)
			}
		})
	}
}

func TestConsumerPanicStillReapsChild(t *testing.T) {
	executable := childFixture(t, "endless", nil)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	defer func() {
		if recovered := recover(); recovered != "consumer panic" {
			t.Fatalf("unexpected panic: %v", recovered)
		}
	}()
	runScan(ctx, executable, nil, fixtureSchema(), func(arrow.RecordBatch) error { panic("consumer panic") })
}

func TestSummaryDiscardsPartialValues(t *testing.T) {
	args := []string{"--column", "id", "--limit", "1000", "--", "table"}
	for _, mode := range []string{"success", "partial", "null-id"} {
		t.Run(mode, func(t *testing.T) {
			executable := childFixture(t, mode, args)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			summary, err := summarizeTable(ctx, executable, "table")
			if mode == "success" {
				if err != nil || summary != (scanSummary{Rows: 3, IDSum: 6}) {
					t.Fatalf("%+v: %v", summary, err)
				}
			} else if err == nil || summary != (scanSummary{}) {
				t.Fatalf("partial summary escaped: %+v, %v", summary, err)
			}
		})
	}
}

func TestStartFailuresDoNotExposePaths(t *testing.T) {
	for _, path := range []string{"dar", filepath.Join(t.TempDir(), "private-executable")} {
		err := runScan(context.Background(), path, []string{"secret"}, fixtureSchema(), nil)
		if err == nil || strings.Contains(err.Error(), "private") || strings.Contains(err.Error(), "secret") {
			t.Fatal(err)
		}
	}
}

func TestArgumentErrorsDoNotExposeValues(t *testing.T) {
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{{"-timeout", "private-signed-location"}, {"-private-signed-location"}} {
		cmd := exec.Command(executable, args...)
		cmd.Env = append(os.Environ(), "DAR_GO_TEST_MODE=main")
		output, err := cmd.CombinedOutput()
		var exit *exec.ExitError
		if !errors.As(err, &exit) || exit.ExitCode() != 2 || strings.Contains(string(output), "private-signed-location") {
			t.Fatalf("%q: %v", output, err)
		}
	}
}
