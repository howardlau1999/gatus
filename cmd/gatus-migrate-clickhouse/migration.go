package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/TwiN/gatus/v5/config/endpoint"
	"github.com/TwiN/gatus/v5/config/suite"
	"github.com/TwiN/gatus/v5/storage/store/clickhouse"
)

type migration struct {
	sourceID          string
	counts            map[string]int64
	endpoints         map[int64]sourceRow
	suites            map[int64]sourceRow
	standalone        map[int64]bool
	conditions        map[int64][]*endpoint.ConditionResult
	suiteChildren     map[int64][]*endpoint.Result
	totalExecutions   int64
	successExecutions int64
	totalResponseTime int64
}

// inspect reads the entire source before opening a target connection. The
// canonical content digest identifies an immutable snapshot, independent of its
// filename or SQLite page layout. Both passes use the same read transaction.
func inspect(ctx context.Context, source *sqliteSource, batchSize int) (*migration, error) {
	m := &migration{
		counts:        make(map[string]int64),
		endpoints:     make(map[int64]sourceRow),
		suites:        make(map[int64]sourceRow),
		standalone:    make(map[int64]bool),
		conditions:    make(map[int64][]*endpoint.ConditionResult),
		suiteChildren: make(map[int64][]*endpoint.Result),
	}
	hash := sha256.New()
	encoder := json.NewEncoder(hash)
	var children []sourceRow
	for _, spec := range sourceTables {
		_, _ = fmt.Fprintln(hash, spec.name)
		count, err := source.readTable(ctx, spec, batchSize, func(rows []sourceRow) error {
			for _, row := range rows {
				if err := encoder.Encode(row); err != nil {
					return err
				}
				switch spec.name {
				case "endpoints":
					m.endpoints[number(row, "endpoint_id")] = row
				case "suites":
					m.suites[number(row, "suite_id")] = row
				case "endpoint_result_conditions":
					id := number(row, "endpoint_result_id")
					m.conditions[id] = append(m.conditions[id], &endpoint.ConditionResult{Condition: str(row, "condition"), Success: number(row, "success") == 1})
				case "endpoint_results":
					if row["suite_result_id"] == nil {
						m.standalone[number(row, "endpoint_id")] = true
					} else {
						children = append(children, row)
					}
				case "endpoint_uptimes":
					total, successful, duration := number(row, "total_executions"), number(row, "successful_executions"), number(row, "total_response_time")
					if total < 0 || successful < 0 || successful > total || duration < 0 {
						return fmt.Errorf("invalid uptime counters at row %d", number(row, "endpoint_uptime_id"))
					}
					m.totalExecutions += total
					m.successExecutions += successful
					m.totalResponseTime += duration
				}
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
		m.counts[spec.name] = count
	}
	for _, row := range children {
		id := number(row, "suite_result_id")
		result := m.endpointResult(row)
		result.Name = str(m.endpoints[number(row, "endpoint_id")], "endpoint_name")
		m.suiteChildren[id] = append(m.suiteChildren[id], result)
	}
	m.sourceID = "sqlite-sha256:" + hex.EncodeToString(hash.Sum(nil))
	return m, nil
}

func (m *migration) printSummary(out io.Writer) {
	fmt.Fprintf(out, "Source snapshot: %s\n", m.sourceID)
	for _, table := range sourceTables {
		fmt.Fprintf(out, "  %-30s %d\n", table.name, m.counts[table.name])
	}
	fmt.Fprintf(out, "Uptime counters: total=%d successful=%d total_response_time_ms=%d\n", m.totalExecutions, m.successExecutions, m.totalResponseTime)
}

type importTarget interface {
	PrepareImport(string) error
	ImportBatch(*clickhouse.ImportBatch) error
	ImportStats() (*clickhouse.ImportStats, error)
}

func (m *migration) apply(ctx context.Context, source *sqliteSource, target importTarget, batchSize int, out io.Writer) error {
	if err := target.PrepareImport(m.sourceID); err != nil {
		return fmt.Errorf("prepare target: %w", err)
	}
	for _, table := range sourceTables {
		if table.name == "endpoint_result_conditions" {
			continue // Conditions are embedded in result payloads, including suites.
		}
		count, err := source.readTable(ctx, table, batchSize, func(rows []sourceRow) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			batch := m.convert(table.name, rows)
			if err := target.ImportBatch(batch); err != nil {
				return fmt.Errorf("import %s batch ending at source ID %d: %w", table.name, number(rows[len(rows)-1], table.key), err)
			}
			return nil
		})
		if err != nil {
			return err
		}
		fmt.Fprintf(out, "Imported %-30s %d\n", table.name, count)
	}
	stats, err := target.ImportStats()
	if err != nil {
		return fmt.Errorf("verify imported history: %w", err)
	}
	expected := m.expectedStats()
	if stats == nil || *stats != expected {
		return fmt.Errorf("import verification failed: expected %+v; got %+v", expected, stats)
	}
	fmt.Fprintln(out, "Verified all source table counts, result conditions, and uptime totals against ClickHouse FINAL.")
	return nil
}

func (m *migration) expectedStats() clickhouse.ImportStats {
	return clickhouse.ImportStats{
		Endpoints: m.counts["endpoints"], Suites: m.counts["suites"],
		Results: m.counts["endpoint_results"], SuiteResults: m.counts["suite_results"],
		Conditions: m.counts["endpoint_result_conditions"], Events: m.counts["endpoint_events"],
		Uptimes: m.counts["endpoint_uptimes"], Alerts: m.counts["endpoint_alerts_triggered"],
		TotalExecutions: m.totalExecutions, SuccessfulExecutions: m.successExecutions,
		TotalResponseTime: m.totalResponseTime,
	}
}

func (m *migration) convert(table string, rows []sourceRow) *clickhouse.ImportBatch {
	batch := &clickhouse.ImportBatch{}
	for _, row := range rows {
		switch table {
		case "endpoints":
			batch.Endpoints = append(batch.Endpoints, clickhouse.EndpointRow{Key: str(row, "endpoint_key"), Name: str(row, "endpoint_name"), Group: str(row, "endpoint_group"), Standalone: m.standalone[number(row, "endpoint_id")]})
		case "suites":
			batch.Suites = append(batch.Suites, clickhouse.SuiteRow{Key: str(row, "suite_key"), Name: str(row, "suite_name"), Group: str(row, "suite_group")})
		case "suite_results":
			entity := m.suites[number(row, "suite_id")]
			children := m.suiteChildren[number(row, "suite_result_id")]
			if children == nil {
				children = []*endpoint.Result{}
			}
			batch.SuiteResults = append(batch.SuiteResults, clickhouse.SuiteResultRow{
				Key: str(entity, "suite_key"), ID: m.rowID(table, number(row, "suite_result_id")),
				Result: &suite.Result{
					Name: str(entity, "suite_name"), Group: str(entity, "suite_group"), Success: number(row, "success") == 1,
					Errors: splitErrors(str(row, "errors")), Duration: time.Duration(number(row, "duration")),
					Timestamp: row["timestamp"].(time.Time), EndpointResults: children,
				},
			})
		case "endpoint_results":
			suiteID := ""
			if row["suite_result_id"] != nil {
				suiteID = m.rowID("suite_results", number(row, "suite_result_id"))
			}
			batch.Results = append(batch.Results, clickhouse.EndpointResultRow{
				Key: m.endpointKey(row), ID: m.rowID(table, number(row, "endpoint_result_id")),
				Result: m.endpointResult(row), SuiteID: suiteID, IncludeUptime: false,
			})
		case "endpoint_events":
			batch.Events = append(batch.Events, clickhouse.EventRow{
				Key: m.endpointKey(row), ID: m.rowID(table, number(row, "endpoint_event_id")),
				Event: &endpoint.Event{Type: endpoint.EventType(str(row, "event_type")), Timestamp: row["event_timestamp"].(time.Time)},
			})
		case "endpoint_uptimes":
			batch.Uptimes = append(batch.Uptimes, clickhouse.UptimeRow{
				Key: m.endpointKey(row), ID: m.rowID(table, number(row, "endpoint_uptime_id")),
				Timestamp: number(row, "hour_unix_timestamp"), TotalExecutions: number(row, "total_executions"),
				SuccessfulExecutions: number(row, "successful_executions"), TotalResponseTime: number(row, "total_response_time"),
			})
		case "endpoint_alerts_triggered":
			batch.Alerts = append(batch.Alerts, clickhouse.AlertRow{
				Key: m.endpointKey(row), Checksum: str(row, "configuration_checksum"),
				ResolveKey: str(row, "resolve_key"), Successes: int(number(row, "number_of_successes_in_a_row")),
			})
		}
	}
	return batch
}

func (m *migration) rowID(table string, id int64) string {
	return fmt.Sprintf("%s:%s:%020d", m.sourceID, table, id)
}

func (m *migration) endpointKey(row sourceRow) string {
	return str(m.endpoints[number(row, "endpoint_id")], "endpoint_key")
}

func (m *migration) endpointResult(row sourceRow) *endpoint.Result {
	return &endpoint.Result{
		Success: number(row, "success") == 1, Connected: number(row, "connected") == 1,
		HTTPStatus: int(number(row, "status")), DNSRCode: str(row, "dns_rcode"),
		Hostname: str(row, "hostname"), IP: str(row, "ip"),
		CertificateExpiration: time.Duration(number(row, "certificate_expiration")),
		DomainExpiration:      time.Duration(number(row, "domain_expiration")),
		Duration:              time.Duration(number(row, "duration")), Timestamp: row["timestamp"].(time.Time),
		Errors: splitErrors(str(row, "errors")), ConditionResults: m.conditions[number(row, "endpoint_result_id")],
	}
}

func number(row sourceRow, name string) int64 { return row[name].(int64) }
func str(row sourceRow, name string) string   { return row[name].(string) }
func splitErrors(text string) []string {
	if text == "" {
		return nil
	}
	return strings.Split(text, "|~|")
}
