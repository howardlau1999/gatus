package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

// These are the persisted SQL tables, including the independent uptime history.
// Reading only retained endpoint_results cannot reconstruct that history.
var sourceTables = []tableSpec{
	{"endpoints", "endpoint_id", []string{"endpoint_id", "endpoint_key", "endpoint_name", "endpoint_group"}},
	{"suites", "suite_id", []string{"suite_id", "suite_key", "suite_name", "suite_group"}},
	{"suite_results", "suite_result_id", []string{"suite_result_id", "suite_id", "success", "errors", "duration", "timestamp"}},
	{"endpoint_results", "endpoint_result_id", []string{"endpoint_result_id", "endpoint_id", "success", "errors", "connected", "status", "dns_rcode", "certificate_expiration", "domain_expiration", "hostname", "ip", "duration", "timestamp", "suite_result_id"}},
	{"endpoint_result_conditions", "endpoint_result_condition_id", []string{"endpoint_result_condition_id", "endpoint_result_id", "condition", "success"}},
	{"endpoint_events", "endpoint_event_id", []string{"endpoint_event_id", "endpoint_id", "event_type", "event_timestamp"}},
	{"endpoint_uptimes", "endpoint_uptime_id", []string{"endpoint_uptime_id", "endpoint_id", "hour_unix_timestamp", "total_executions", "successful_executions", "total_response_time"}},
	{"endpoint_alerts_triggered", "endpoint_alert_trigger_id", []string{"endpoint_alert_trigger_id", "endpoint_id", "configuration_checksum", "resolve_key", "number_of_successes_in_a_row"}},
}

type tableSpec struct {
	name    string
	key     string
	columns []string
}

type sourceRow map[string]any

type sqliteSource struct {
	db *sql.DB
	tx *sql.Tx
}

func openSource(ctx context.Context, path string) (*sqliteSource, error) {
	absolute, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	info, err := os.Stat(absolute)
	if err != nil {
		return nil, fmt.Errorf("open SQLite source: %w", err)
	}
	if !info.Mode().IsRegular() {
		return nil, errors.New("SQLite source must be a regular file")
	}
	u := url.URL{Scheme: "file", Path: absolute}
	q := u.Query()
	q.Set("mode", "ro")
	q.Add("_pragma", "query_only(1)")
	q.Add("_pragma", "busy_timeout(5000)")
	u.RawQuery = q.Encode()
	db, err := sql.Open("sqlite", u.String())
	if err != nil {
		return nil, fmt.Errorf("open SQLite source: %w", err)
	}
	db.SetMaxOpenConns(1)
	tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("begin SQLite read transaction: %w", err)
	}
	s := &sqliteSource{db: db, tx: tx}
	if err = s.validate(ctx); err != nil {
		s.close()
		return nil, err
	}
	return s, nil
}

func (s *sqliteSource) close() {
	_ = s.tx.Rollback()
	_ = s.db.Close()
}

func (s *sqliteSource) validate(ctx context.Context) error {
	// This first query also establishes the consistent SQLite read snapshot.
	for _, spec := range sourceTables {
		rows, err := s.tx.QueryContext(ctx, "SELECT "+strings.Join(spec.columns, ",")+" FROM "+spec.name+" LIMIT 0")
		if err != nil {
			return fmt.Errorf("unsupported SQLite schema in %s: %w", spec.name, err)
		}
		if err = rows.Close(); err != nil {
			return err
		}
	}
	rows, err := s.tx.QueryContext(ctx, "PRAGMA foreign_key_check")
	if err != nil {
		return fmt.Errorf("check SQLite references: %w", err)
	}
	defer rows.Close()
	if rows.Next() {
		var table, parent string
		var id, constraint int64
		if err = rows.Scan(&table, &id, &parent, &constraint); err != nil {
			return fmt.Errorf("read SQLite reference violation: %w", err)
		}
		return fmt.Errorf("SQLite source has a broken reference: table=%s row=%d parent=%s", table, id, parent)
	}
	return rows.Err()
}

func (s *sqliteSource) readTable(ctx context.Context, spec tableSpec, batchSize int, consume func([]sourceRow) error) (int64, error) {
	rows, err := s.tx.QueryContext(ctx, "SELECT "+strings.Join(spec.columns, ",")+" FROM "+spec.name+" ORDER BY "+spec.key)
	if err != nil {
		return 0, fmt.Errorf("read %s: %w", spec.name, err)
	}
	defer rows.Close()
	var total int64
	batch := make([]sourceRow, 0, batchSize)
	for rows.Next() {
		values := make([]any, len(spec.columns))
		pointers := make([]any, len(values))
		for i := range values {
			pointers[i] = &values[i]
		}
		if err = rows.Scan(pointers...); err != nil {
			return total, fmt.Errorf("scan %s: %w", spec.name, err)
		}
		row := make(sourceRow, len(values))
		for i, column := range spec.columns {
			value, err := normalizeValue(column, values[i])
			if err != nil {
				return total, fmt.Errorf("%s row %v column %s: %w", spec.name, values[0], column, err)
			}
			row[column] = value
		}
		batch = append(batch, row)
		total++
		if len(batch) == batchSize {
			if err = consume(batch); err != nil {
				return total, err
			}
			batch = make([]sourceRow, 0, batchSize)
		}
	}
	if err = rows.Err(); err != nil {
		return total, fmt.Errorf("read %s: %w", spec.name, err)
	}
	if len(batch) > 0 {
		if err = consume(batch); err != nil {
			return total, err
		}
	}
	return total, nil
}

func normalizeValue(column string, value any) (any, error) {
	if value == nil {
		if column == "suite_result_id" {
			return nil, nil
		}
		return nil, errors.New("unexpected NULL")
	}
	if b, ok := value.([]byte); ok {
		value = string(b)
	}
	if column == "timestamp" || column == "event_timestamp" {
		if t, ok := value.(time.Time); ok {
			return t.UTC(), nil
		}
		if text, ok := value.(string); ok {
			return parseTimestamp(text)
		}
		return nil, fmt.Errorf("expected timestamp, received %T", value)
	}
	switch column {
	case "endpoint_key", "endpoint_name", "endpoint_group", "suite_key", "suite_name", "suite_group", "errors", "dns_rcode", "hostname", "ip", "condition", "event_type", "configuration_checksum", "resolve_key":
		if _, ok := value.(string); !ok {
			return nil, fmt.Errorf("expected text, received %T", value)
		}
	default:
		n, ok := value.(int64)
		if !ok {
			return nil, fmt.Errorf("expected integer, received %T", value)
		}
		if (column == "success" || column == "connected") && n != 0 && n != 1 {
			return nil, errors.New("boolean integer must be 0 or 1")
		}
	}
	return value, nil
}

func parseTimestamp(text string) (time.Time, error) {
	for _, layout := range []string{
		time.RFC3339Nano,
		"2006-01-02 15:04:05.999999999 -0700 MST",
		"2006-01-02 15:04:05.999999999 -0700",
		"2006-01-02 15:04:05.999999999Z07:00",
		"2006-01-02 15:04:05.999999999",
	} {
		if t, err := time.Parse(layout, text); err == nil {
			return t.UTC(), nil
		}
	}
	return time.Time{}, errors.New("invalid SQLite timestamp (expected Go timestamp or RFC3339)")
}
