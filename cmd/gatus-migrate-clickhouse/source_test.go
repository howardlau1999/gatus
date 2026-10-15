package main

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/TwiN/gatus/v5/alerting/alert"
	sqlstore "github.com/TwiN/gatus/v5/storage/store/sql"
)

func fixture(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "gatus #test.db")
	store, err := sqlstore.NewStore("sqlite", path, false, 100, 50)
	if err != nil {
		t.Fatal(err)
	}
	store.Close()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	queries := []string{
		`INSERT INTO endpoints VALUES (1, 'prod_probe', 'probe', 'prod'), (2, 'prod_child', 'child', 'prod')`,
		`INSERT INTO suites VALUES (1, 'prod_suite', 'suite', 'prod')`,
		`INSERT INTO suite_results VALUES (1, 1, 0, 'suite error', 987654321, '2026-10-15 05:27:29.123456789 +0000 UTC')`,
		`INSERT INTO endpoint_results VALUES (1, 1, 1, '', 1, 200, 'NOERROR', 12345, 67890, 'localhost', '127.0.0.1', 1234567, '2026-10-15 05:27:29.123456789 +0000 UTC', NULL)`,
		`INSERT INTO endpoint_results VALUES (2, 2, 0, 'first|~|second', 0, 503, '', 0, 0, '', '', 987654321, '2026-10-15 05:27:29.987654321 +0000 UTC', 1)`,
		`INSERT INTO endpoint_result_conditions VALUES (1, 1, '[STATUS] == 200', 1), (2, 2, '[STATUS] == 200', 0)`,
		`INSERT INTO endpoint_events VALUES (1, 1, 'START', '2026-08-17 03:24:32.892319515 +0000 UTC')`,
		`INSERT INTO endpoint_uptimes VALUES (1, 1, 1789488000, 722008, 718553, 123456789000)`,
	}
	for _, query := range queries {
		if _, err = db.Exec(query); err != nil {
			t.Fatal(err)
		}
	}
	if _, err = db.Exec(`INSERT INTO endpoint_alerts_triggered VALUES (1, 1, ?, 'resolve', 4)`, (&alert.Alert{}).Checksum()); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestSourcePreservesHistoryAndNanoseconds(t *testing.T) {
	path := fixture(t)
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	source, err := openSource(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	for _, table := range sourceTables {
		count, err := source.readTable(context.Background(), table, 1, func(rows []sourceRow) error {
			if len(rows) != 1 {
				t.Fatalf("batch size=%d", len(rows))
			}
			row := rows[0]
			if table.name == "endpoint_uptimes" && row["total_executions"] != int64(722008) {
				t.Fatalf("lost uptime history: %v", row)
			}
			if table.name == "endpoint_results" && row["endpoint_result_id"] == int64(1) {
				if row["timestamp"].(time.Time).Nanosecond() != 123456789 {
					t.Fatalf("lost timestamp precision: %v", row["timestamp"])
				}
				if row["suite_result_id"] != nil {
					t.Fatal("standalone result gained a suite association")
				}
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		if count == 0 {
			t.Fatalf("table %s was not migrated", table.name)
		}
	}
	if _, err = source.tx.Exec("DELETE FROM endpoints"); err == nil {
		t.Fatal("source connection must reject writes")
	}
	source.close()
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if sha256.Sum256(before) != sha256.Sum256(after) {
		t.Fatal("source database changed")
	}
}

func TestSourceDoesNotCreateMissingDatabase(t *testing.T) {
	path := filepath.Join(t.TempDir(), "missing.db")
	if _, err := openSource(context.Background(), path); err == nil {
		t.Fatal("missing source accepted")
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("source was created: %v", err)
	}
}

func TestSourceRejectsBrokenReferences(t *testing.T) {
	path := fixture(t)
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec("DELETE FROM suites")
	_ = db.Close()
	if err != nil {
		t.Fatal(err)
	}
	if source, err := openSource(context.Background(), path); err == nil {
		source.close()
		t.Fatal("broken suite association accepted")
	}
}

func TestParseTimestamp(t *testing.T) {
	for _, text := range []string{
		"2026-10-15 05:27:29.123456789 +0000 UTC",
		"2026-10-15T13:27:29.123456789+08:00",
		"2026-10-15 05:27:29.123456789+00:00",
		"2026-10-15 05:27:29.123456789",
	} {
		value, err := parseTimestamp(text)
		if err != nil || value.Format(time.RFC3339Nano) != "2026-10-15T05:27:29.123456789Z" {
			t.Errorf("parseTimestamp(%q)=%v,%v", text, value, err)
		}
	}
	if _, err := parseTimestamp("not a timestamp"); err == nil {
		t.Fatal("invalid timestamp accepted")
	}
}
