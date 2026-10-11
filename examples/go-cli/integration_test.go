package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
)

func TestRealCLI(t *testing.T) {
	executable := os.Getenv("DAR_BINARY")
	if executable == "" {
		t.Skip("set DAR_BINARY to an absolute path to run the real CLI check")
	}
	fixture := filepath.Join("..", "..", "crates", "delta-arrow-reader-core", "tests", "reader", "fixtures", "external_writer", "corpus", "partitioned")
	file, err := os.Open(filepath.Join(fixture, "expected.arrow"))
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	oracle, err := ipc.NewFileReader(file)
	if err != nil {
		t.Fatal(err)
	}
	defer oracle.Close()
	field := oracle.Schema().Field(0)
	if field.Name != "id" || field.Type.ID() != arrow.INT32 {
		t.Fatal("unexpected oracle id field")
	}
	want := make(map[int32]int)
	for i := 0; i < oracle.NumRecords(); i++ {
		batch, err := oracle.RecordBatch(i)
		if err != nil {
			t.Fatal(err)
		}
		for _, id := range batch.Column(0).(*array.Int32).Int32Values() {
			want[id]++
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	table := filepath.Join(fixture, "table")
	got := make(map[int32]int)
	err = runScan(ctx, executable, []string{"--column", "id", "--", table}, arrow.NewSchema([]arrow.Field{field}, nil), func(batch arrow.RecordBatch) error {
		ids := batch.Column(0).(*array.Int32)
		if ids.NullN() != 0 {
			return errors.New("unexpected null id")
		}
		for _, id := range ids.Int32Values() {
			got[id]++
		}
		return nil
	})
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("CLI differs from the Spark oracle: %v", err)
	}
	summary, err := summarizeTable(ctx, executable, table)
	if err != nil || summary != (scanSummary{Rows: 360, IDSum: 64980}) {
		t.Fatalf("unexpected summary: %+v, %v", summary, err)
	}
	err = runScan(ctx, executable, []string{"--limit", "0", "--column", "id", "--", table}, arrow.NewSchema([]arrow.Field{field}, nil), func(arrow.RecordBatch) error {
		t.Error("zero limit returned a batch")
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = summarizeTable(ctx, executable, filepath.Join(t.TempDir(), "private-signed-location"))
	var failure *scanError
	if !errors.As(err, &failure) || len(failure.stderr) == 0 || strings.Contains(string(failure.stderr), "private-signed-location") {
		t.Fatalf("unexpected reader failure: %v", err)
	}
}
