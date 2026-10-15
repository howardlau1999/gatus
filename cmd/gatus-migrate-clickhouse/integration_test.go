package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/TwiN/gatus/v5/alerting/alert"
	"github.com/TwiN/gatus/v5/config/endpoint"
	"github.com/TwiN/gatus/v5/config/suite"
	"github.com/TwiN/gatus/v5/storage"
	"github.com/TwiN/gatus/v5/storage/store/clickhouse"
	"github.com/TwiN/gatus/v5/storage/store/common/paging"
)

func integrationTarget(t *testing.T) *storage.ClickHouseConfig {
	t.Helper()
	address := os.Getenv("GATUS_TEST_CLICKHOUSE_URL")
	if address == "" {
		t.Skip("set GATUS_TEST_CLICKHOUSE_URL to run against an isolated ClickHouse server")
	}
	database := fmt.Sprintf("gatus_migration_test_%d", time.Now().UnixNano())
	exec := func(query string) error {
		u, err := url.Parse(address)
		if err != nil {
			return err
		}
		params := u.Query()
		params.Set("query", query)
		u.RawQuery = params.Encode()
		response, err := (&http.Client{Timeout: 10 * time.Second}).Post(u.String(), "text/plain", nil)
		if err != nil {
			return err
		}
		defer response.Body.Close()
		body, err := io.ReadAll(response.Body)
		if err != nil {
			return err
		}
		if response.StatusCode != http.StatusOK || len(strings.TrimSpace(string(body))) != 0 {
			return fmt.Errorf("test database command failed: status=%d body=%s", response.StatusCode, body)
		}
		return nil
	}
	if err := exec("CREATE DATABASE " + database); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := exec("DROP DATABASE " + database + " SYNC"); err != nil {
			t.Error(err)
		}
	})
	return &storage.ClickHouseConfig{Address: address, Database: database, Username: "default", Timeout: 30 * time.Second}
}

type interruptedTarget struct {
	*clickhouse.Store
	remaining int
}

func (s *interruptedTarget) ImportBatch(batch *clickhouse.ImportBatch) error {
	if s.remaining == 0 {
		return fmt.Errorf("simulated interrupted migration")
	}
	s.remaining--
	return s.Store.ImportBatch(batch)
}

func TestClickHouseMigrationResumeAndGuards(t *testing.T) {
	cfg := integrationTarget(t)
	source, err := openSource(context.Background(), fixture(t))
	if err != nil {
		t.Fatal(err)
	}
	defer source.close()
	m, err := inspect(context.Background(), source, 1)
	if err != nil {
		t.Fatal(err)
	}
	store, err := clickhouse.NewStore(cfg, 100000, 100000)
	if err != nil {
		t.Fatal(err)
	}
	partial := &interruptedTarget{Store: store, remaining: 5}
	if err = m.apply(context.Background(), source, partial, 1, io.Discard); err == nil {
		t.Fatal("interrupted migration succeeded")
	}
	store.Close()
	for range 2 {
		store, err = clickhouse.NewStore(cfg, 100000, 100000)
		if err != nil {
			t.Fatal(err)
		}
		if err = m.apply(context.Background(), source, store, 2, io.Discard); err != nil {
			t.Fatal(err)
		}
		verifyEndpointHistory(t, source, m, store)
		status, err := store.GetSuiteStatusByKey("prod_suite", paging.NewSuiteStatusParams())
		if err != nil || len(status.Results) != 1 || len(status.Results[0].EndpointResults) != 1 || status.Results[0].EndpointResults[0].Name != "child" || len(status.Results[0].EndpointResults[0].ConditionResults) != 1 {
			t.Fatalf("imported suite child/condition lost: status=%+v err=%v", status, err)
		}
		exists, resolveKey, successes, err := store.GetTriggeredEndpointAlert(&endpoint.Endpoint{Name: "probe", Group: "prod"}, &alert.Alert{})
		if err != nil || !exists || resolveKey != "resolve" || successes != 4 {
			t.Fatalf("imported alert resolution state lost: exists=%v resolve=%q successes=%d err=%v", exists, resolveKey, successes, err)
		}
		store.Close()
	}
	store, err = clickhouse.NewStore(cfg, 100000, 100000)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err = store.PrepareImport(m.sourceID + "different-snapshot"); err == nil {
		t.Fatal("different snapshot accepted")
	}
	if err = store.InsertSuiteResult(&suite.Suite{Name: "suite", Group: "prod"}, &suite.Result{Success: true, Timestamp: time.Now()}); err != nil {
		t.Fatal(err)
	}
	if err = store.PrepareImport(m.sourceID); err == nil {
		t.Fatal("migration resumed after live suite writes")
	}
}

// An external snapshot stays outside the repository; only read-only access is
// used, and all writes target a freshly created test database.
func TestClickHouseMigrationExternalSnapshot(t *testing.T) {
	path := os.Getenv("GATUS_MIGRATION_SQLITE")
	if path == "" {
		t.Skip("set GATUS_MIGRATION_SQLITE to validate an existing SQLite snapshot")
	}
	cfg := integrationTarget(t)
	source, err := openSource(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	defer source.close()
	m, err := inspect(context.Background(), source, 1000)
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		store, err := clickhouse.NewStore(cfg, 100000, 100000)
		if err != nil {
			t.Fatal(err)
		}
		if err = m.apply(context.Background(), source, store, 1000, io.Discard); err != nil {
			store.Close()
			t.Fatal(err)
		}
		verifyEndpointHistory(t, source, m, store)
		store.Close()
	}
	t.Logf("verified %d endpoints, %d results, %d events, %d aggregate executions; repeated import is identical", m.counts["endpoints"], m.counts["endpoint_results"], m.counts["endpoint_events"], m.totalExecutions)
}

func verifyEndpointHistory(t *testing.T, source *sqliteSource, m *migration, target *clickhouse.Store) {
	t.Helper()
	results := make(map[string][]*endpoint.Result)
	events := make(map[string][]*endpoint.Event)
	for _, spec := range sourceTables {
		if spec.name != "endpoint_results" && spec.name != "endpoint_events" {
			continue
		}
		_, err := source.readTable(context.Background(), spec, 1000, func(rows []sourceRow) error {
			for _, row := range rows {
				key := m.endpointKey(row)
				if spec.name == "endpoint_results" {
					results[key] = append(results[key], m.endpointResult(row))
				} else {
					events[key] = append(events[key], &endpoint.Event{Type: endpoint.EventType(str(row, "event_type")), Timestamp: row["event_timestamp"].(time.Time)})
				}
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	var latestHour int64
	if err := source.tx.QueryRow("SELECT coalesce(max(hour_unix_timestamp),0) FROM endpoint_uptimes").Scan(&latestHour); err != nil {
		t.Fatal(err)
	}
	to := time.Unix(latestHour+3600, 0)
	for id, entity := range m.endpoints {
		key := str(entity, "endpoint_key")
		status, err := target.GetEndpointStatusByKey(key, paging.NewEndpointStatusParams().WithResults(1, 100000).WithEvents(1, 100000))
		if err != nil {
			t.Fatalf("read endpoint %s: %v", key, err)
		}
		if len(status.Results) != len(results[key]) || len(status.Events) != len(events[key]) {
			t.Fatalf("endpoint %s history count mismatch: results=%d/%d events=%d/%d", key, len(status.Results), len(results[key]), len(status.Events), len(events[key]))
		}
		for i, result := range status.Results {
			if !reflect.DeepEqual(result, results[key][i]) {
				t.Fatalf("endpoint %s result %d changed during migration", key, i)
			}
		}
		for i, event := range status.Events {
			if !reflect.DeepEqual(event, events[key][i]) {
				t.Fatalf("endpoint %s event %d changed during migration", key, i)
			}
		}
		for _, duration := range []time.Duration{24 * time.Hour, 7 * 24 * time.Hour, 30 * 24 * time.Hour} {
			from := to.Add(-duration)
			var total, success, elapsed int64
			if err = source.tx.QueryRow(`SELECT coalesce(sum(total_executions),0), coalesce(sum(successful_executions),0), coalesce(sum(total_response_time),0) FROM endpoint_uptimes WHERE endpoint_id=? AND hour_unix_timestamp>=? AND hour_unix_timestamp<=?`, id, from.Unix(), to.Unix()).Scan(&total, &success, &elapsed); err != nil {
				t.Fatal(err)
			}
			var expectedUptime float64
			var expectedAverage int
			if total > 0 {
				expectedUptime = float64(success) / float64(total)
				expectedAverage = int(float64(elapsed) / float64(total))
			}
			uptime, err := target.GetUptimeByKey(key, from, to)
			if err != nil || uptime != expectedUptime {
				t.Fatalf("endpoint %s %s uptime=%f expected=%f err=%v", key, duration, uptime, expectedUptime, err)
			}
			average, err := target.GetAverageResponseTimeByKey(key, from, to)
			if err != nil || average != expectedAverage {
				t.Fatalf("endpoint %s %s response time=%d expected=%d err=%v", key, duration, average, expectedAverage, err)
			}
		}
	}
}
