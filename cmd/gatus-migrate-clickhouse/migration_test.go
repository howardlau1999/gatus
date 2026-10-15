package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/TwiN/gatus/v5/storage/store/clickhouse"
)

type recordingTarget struct {
	sourceID string
	batches  []*clickhouse.ImportBatch
	fail     bool
	stats    clickhouse.ImportStats
}

func (r *recordingTarget) PrepareImport(sourceID string) error {
	r.sourceID = sourceID
	return nil
}
func (r *recordingTarget) ImportBatch(batch *clickhouse.ImportBatch) error {
	if r.fail {
		return errors.New("simulated target outage")
	}
	r.batches = append(r.batches, batch)
	r.stats.Endpoints += int64(len(batch.Endpoints))
	r.stats.Suites += int64(len(batch.Suites))
	r.stats.Results += int64(len(batch.Results))
	r.stats.SuiteResults += int64(len(batch.SuiteResults))
	r.stats.Events += int64(len(batch.Events))
	r.stats.Uptimes += int64(len(batch.Uptimes))
	r.stats.Alerts += int64(len(batch.Alerts))
	for _, row := range batch.Results {
		r.stats.Conditions += int64(len(row.Result.ConditionResults))
	}
	for _, row := range batch.Uptimes {
		r.stats.TotalExecutions += row.TotalExecutions
		r.stats.SuccessfulExecutions += row.SuccessfulExecutions
		r.stats.TotalResponseTime += row.TotalResponseTime
	}
	return nil
}
func (r *recordingTarget) ImportStats() (*clickhouse.ImportStats, error) {
	return &r.stats, nil
}

func TestMigrationPreservesConditionsSuitesAlertsAndAggregateHistory(t *testing.T) {
	source, err := openSource(context.Background(), fixture(t))
	if err != nil {
		t.Fatal(err)
	}
	defer source.close()
	m, err := inspect(context.Background(), source, 1)
	if err != nil {
		t.Fatal(err)
	}
	target := &recordingTarget{}
	if err = m.apply(context.Background(), source, target, 1, io.Discard); err != nil {
		t.Fatal(err)
	}
	var results, suites, conditions, alerts, events int
	var executions int64
	for _, batch := range target.batches {
		for _, row := range batch.Endpoints {
			if row.Standalone != (row.Key == "prod_probe") {
				t.Fatalf("incorrect suite visibility for %s", row.Key)
			}
		}
		for _, row := range batch.Results {
			results++
			conditions += len(row.Result.ConditionResults)
			if row.IncludeUptime {
				t.Fatal("retained result would double count uptime")
			}
			if row.Key == "prod_probe" && (row.Result.CertificateExpiration != 12345 || row.Result.DomainExpiration != 67890 || row.Result.IP != "127.0.0.1" || !row.Result.Connected) {
				t.Fatal("non-JSON endpoint result fields lost")
			}
		}
		for _, row := range batch.SuiteResults {
			suites++
			if len(row.Result.EndpointResults) != 1 || row.Result.EndpointResults[0].Name != "child" || len(row.Result.EndpointResults[0].ConditionResults) != 1 || len(row.Result.EndpointResults[0].Errors) != 2 {
				t.Fatalf("suite child metadata lost: %+v", row.Result)
			}
		}
		for _, row := range batch.Uptimes {
			executions += row.TotalExecutions
		}
		for _, row := range batch.Alerts {
			alerts++
			if row.ResolveKey != "resolve" || row.Successes != 4 {
				t.Fatal("alert resolution state lost")
			}
		}
		events += len(batch.Events)
	}
	if results != 2 || suites != 1 || conditions != 2 || alerts != 1 || events != 1 || executions != 722008 {
		t.Fatalf("unexpected migrated history: results=%d suites=%d conditions=%d alerts=%d events=%d executions=%d", results, suites, conditions, alerts, events, executions)
	}
	if target.sourceID != m.sourceID {
		t.Fatal("snapshot guard missing")
	}
	target.fail = true
	if err = m.apply(context.Background(), source, target, 1, io.Discard); err == nil {
		t.Fatal("target error was ignored")
	}
}

func TestSnapshotIdentityIndependentOfPathAndBatchSize(t *testing.T) {
	path := fixture(t)
	copyPath := filepath.Join(t.TempDir(), "renamed.db")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(copyPath, data, 0600); err != nil {
		t.Fatal(err)
	}
	var identity string
	for i, name := range []string{path, copyPath} {
		source, err := openSource(context.Background(), name)
		if err != nil {
			t.Fatal(err)
		}
		m, err := inspect(context.Background(), source, i+1)
		source.close()
		if err != nil {
			t.Fatal(err)
		}
		if identity != "" && identity != m.sourceID {
			t.Fatal("same immutable snapshot received a different identity")
		}
		identity = m.sourceID
	}
}

func TestDefaultDryRunNeverContactsTarget(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("dry run contacted ClickHouse")
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()
	var output bytes.Buffer
	if err := run(context.Background(), []string{"--sqlite", fixture(t), "--address", server.URL}, &output, io.Discard); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), "total=722008") || !strings.Contains(output.String(), "Dry run complete") {
		t.Fatalf("missing validation summary: %s", output.String())
	}
}
