package main

import (
	"context"
	"errors"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
)

const maxStderrBytes = 32 * 1024

// Error text omits arguments, decoder details, and potentially sensitive stderr.
type scanError struct {
	cause  error
	stderr []byte
}

func (e *scanError) Error() string { return "dar scan failed" }
func (e *scanError) Unwrap() error { return e.cause }

type boundedStderr struct {
	data []byte
}

func (b *boundedStderr) Write(p []byte) (int, error) {
	n := min(len(p), maxStderrBytes-len(b.data))
	b.data = append(b.data, p[:n]...)
	return len(p), nil // Continue draining after the capture limit.
}

// consume borrows each batch until it returns; returning an error stops the scan.
func runScan(ctx context.Context, executable string, scanArgs []string, expectedSchema *arrow.Schema, consume func(arrow.RecordBatch) error) (err error) {
	if !filepath.IsAbs(executable) {
		return errors.New("dar executable must be an absolute path")
	}
	scanCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	cmd := exec.CommandContext(scanCtx, executable, append([]string{"scan"}, scanArgs...)...)
	var stderr boundedStderr
	cmd.Stderr = &stderr
	cmd.WaitDelay = time.Second
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return &scanError{cause: err}
	}
	if err := cmd.Start(); err != nil {
		stdout.Close()
		return &scanError{cause: err}
	}
	// Closing the pipe also interrupts a read if another process inherited stdout.
	stopClose := context.AfterFunc(scanCtx, func() { stdout.Close() })
	defer stopClose()
	decodeSucceeded := false
	defer func() {
		if !decodeSucceeded {
			cancel()
		}
		stdout.Close()
		waitErr := cmd.Wait()
		err = errors.Join(err, waitErr, ctx.Err())
		if err != nil {
			err = &scanError{cause: err, stderr: stderr.data}
		}
	}()

	reader, err := ipc.NewReader(stdout, ipc.WithSchema(expectedSchema))
	if err != nil {
		return err
	}
	defer reader.Release()
	for reader.Next() {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := consume(reader.RecordBatch()); err != nil {
			return err
		}
	}
	err = reader.Err()
	decodeSucceeded = err == nil
	return err
}
