package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

type scanSummary struct {
	Rows  int64 `json:"rows"`
	IDSum int64 `json:"id_sum"`
}

func summarizeTable(ctx context.Context, executable, table string) (scanSummary, error) {
	schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: true}}, nil)
	var summary scanSummary
	err := runScan(ctx, executable, []string{"--column", "id", "--limit", "1000", "--", table}, schema, func(batch arrow.RecordBatch) error {
		ids := batch.Column(0).(*array.Int32)
		for i := 0; i < ids.Len(); i++ {
			if ids.IsNull(i) {
				return errors.New("example requires non-null id values")
			}
			summary.IDSum += int64(ids.Value(i))
		}
		summary.Rows += batch.NumRows()
		return nil
	})
	if err != nil {
		return scanSummary{}, err
	}
	return summary, nil
}

func main() {
	flags := flag.NewFlagSet("go-cli", flag.ContinueOnError)
	flags.SetOutput(io.Discard) // Parse errors can contain sensitive argument values.
	executable := flags.String("dar", "", "Absolute path to the dar executable (required)")
	table := flags.String("table", "", "Delta table with an Int32 id column (required)")
	timeout := flags.Duration("timeout", 30*time.Second, "Maximum scan duration")
	parseErr := flags.Parse(os.Args[1:])
	if errors.Is(parseErr, flag.ErrHelp) {
		flags.SetOutput(os.Stdout)
		flags.PrintDefaults()
		return
	}
	if parseErr != nil || *executable == "" || *table == "" || *timeout <= 0 || flags.NArg() != 0 {
		fmt.Fprintln(os.Stderr, "Provide -dar, -table, and a positive -timeout; see -help.")
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, *timeout)
	defer cancel()
	summary, err := summarizeTable(ctx, *executable, *table)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := json.NewEncoder(os.Stdout).Encode(summary); err != nil {
		fmt.Fprintln(os.Stderr, "Could not write the scan summary.")
		os.Exit(1)
	}
}
