package store

import (
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

// Each test gets its own database. The URL must point to a disposable test server
// on which the supplied user can create and drop databases.
func newClickHouseTestStore(t *testing.T) (*clickhouse.Store, func() *clickhouse.Store) {
	t.Helper()
	return newClickHouseTestStoreWithLimits(t, storage.DefaultMaximumNumberOfResults, storage.DefaultMaximumNumberOfEvents)
}

func newClickHouseTestStoreWithLimits(t *testing.T, maximumResults, maximumEvents int) (*clickhouse.Store, func() *clickhouse.Store) {
	t.Helper()
	address := os.Getenv("GATUS_TEST_CLICKHOUSE_URL")
	if address == "" {
		t.Skip("set GATUS_TEST_CLICKHOUSE_URL to run ClickHouse integration tests")
	}
	parsed, err := url.Parse(address)
	if err != nil {
		t.Fatal("invalid GATUS_TEST_CLICKHOUSE_URL")
	}
	username, password := "default", ""
	if parsed.User != nil {
		username = parsed.User.Username()
		password, _ = parsed.User.Password()
		parsed.User = nil
	}
	database := fmt.Sprintf("gatus_test_%d", time.Now().UnixNano())
	client := &http.Client{Timeout: 30 * time.Second}
	t.Cleanup(client.CloseIdleConnections)
	ddl := func(query string) error {
		u := *parsed
		params := u.Query()
		params.Set("database", "default")
		params.Set("wait_end_of_query", "1")
		u.RawQuery = params.Encode()
		request, err := http.NewRequest(http.MethodPost, u.String(), strings.NewReader(query))
		if err != nil {
			return err
		}
		request.SetBasicAuth(username, password)
		response, err := client.Do(request)
		if err != nil {
			return err
		}
		defer response.Body.Close()
		body, err := io.ReadAll(response.Body)
		if err != nil {
			return err
		}
		if response.StatusCode != http.StatusOK || len(strings.TrimSpace(string(body))) != 0 {
			return fmt.Errorf("ClickHouse DDL: status %d: %s", response.StatusCode, body)
		}
		return nil
	}
	if err := ddl("CREATE DATABASE " + database); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := ddl("DROP DATABASE " + database + " SYNC"); err != nil {
			t.Error(err)
		}
	})
	cfg := &storage.ClickHouseConfig{
		Address: parsed.String(), Database: database,
		Username: username, Password: password, Timeout: 30 * time.Second,
	}
	open := func() *clickhouse.Store {
		t.Helper()
		s, err := clickhouse.NewStore(cfg, maximumResults, maximumEvents)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(s.Close)
		return s
	}
	return open(), open
}

func TestClickHouseAlertPersistenceAndEndpointRecreation(t *testing.T) {
	s, reopen := newClickHouseTestStore(t)
	ep := &endpoint.Endpoint{Name: "quoted ' \\ 中文", Group: "group", NumberOfSuccessesInARow: 1}
	r := &endpoint.Result{Timestamp: time.Now().UTC(), Success: false, Duration: time.Second}
	if err := s.InsertEndpointResult(ep, r); err != nil {
		t.Fatal(err)
	}
	a := &alert.Alert{Type: alert.TypePagerDuty, ResolveKey: "incident ' \\ 中文", FailureThreshold: 2}
	if err := s.UpsertTriggeredEndpointAlert(ep, a); err != nil {
		t.Fatal(err)
	}
	ep.NumberOfSuccessesInARow = 2
	a.ResolveKey = "updated incident"
	if err := s.UpsertTriggeredEndpointAlert(ep, a); err != nil {
		t.Fatal(err)
	}
	s.Close()
	s = reopen()
	exists, resolveKey, successes, err := s.GetTriggeredEndpointAlert(ep, a)
	if err != nil || !exists || resolveKey != a.ResolveKey || successes != 2 {
		t.Fatalf("persisted alert: exists=%t key=%q successes=%d err=%v", exists, resolveKey, successes, err)
	}
	if err := s.DeleteTriggeredEndpointAlert(ep, a); err != nil {
		t.Fatal(err)
	}
	s.Close()
	s = reopen()
	if exists, _, _, err := s.GetTriggeredEndpointAlert(ep, a); err != nil || exists {
		t.Fatalf("deleted alert survived restart: exists=%t err=%v", exists, err)
	}
	other := &alert.Alert{Type: alert.TypeSlack, FailureThreshold: 3}
	for _, a := range []*alert.Alert{a, other} {
		if err := s.UpsertTriggeredEndpointAlert(ep, a); err != nil {
			t.Fatal(err)
		}
	}
	if deleted := s.DeleteAllTriggeredAlertsNotInChecksumsByEndpoint(ep, []string{a.Checksum()}); deleted != 1 {
		t.Fatalf("deleted %d alerts, want 1", deleted)
	}
	if exists, _, _, err := s.GetTriggeredEndpointAlert(ep, other); err != nil || exists {
		t.Fatalf("removed alert remains: exists=%t err=%v", exists, err)
	}
	if deleted := s.DeleteAllEndpointStatusesNotInKeys(nil); deleted != 1 {
		t.Fatalf("deleted %d endpoints, want 1", deleted)
	}
	s.Close()
	s = reopen()
	r = &endpoint.Result{Timestamp: r.Timestamp.Add(time.Minute), Success: true, Duration: 100 * time.Millisecond}
	if err := s.InsertEndpointResult(ep, r); err != nil {
		t.Fatal(err)
	}
	status, err := s.GetEndpointStatusByKey(ep.Key(), paging.NewEndpointStatusParams().WithResults(1, 100).WithEvents(1, 100))
	if err != nil || status == nil || len(status.Results) != 1 || len(status.Events) != 2 {
		t.Fatalf("recreated endpoint contains old history: status=%+v err=%v", status, err)
	}
	if exists, _, _, err := s.GetTriggeredEndpointAlert(ep, a); err != nil || exists {
		t.Fatalf("old alert revived with endpoint: exists=%t err=%v", exists, err)
	}
	uptime, err := s.GetUptimeByKey(ep.Key(), r.Timestamp.Add(-2*time.Hour), r.Timestamp.Add(time.Hour))
	if err != nil || uptime != 1 {
		t.Fatalf("old uptime revived with endpoint: uptime=%f err=%v", uptime, err)
	}
}

func TestClickHouseSuitePersistenceAndIsolation(t *testing.T) {
	s, reopen := newClickHouseTestStore(t)
	su := &suite.Suite{Name: "suite ' \\ 中文", Group: "group"}
	start := time.Now().UTC().Truncate(time.Second).Add(123456789 * time.Nanosecond)
	child := &endpoint.Result{
		Name: "child", Timestamp: start, Success: true, HTTPStatus: 201, DNSRCode: "NOERROR",
		IP: "192.0.2.1", Hostname: "example.org", Connected: true, Duration: 123456789,
		CertificateExpiration: 10 * time.Hour, DomainExpiration: 20 * time.Hour,
		Errors:           []string{"contains |~| and '\\ and 中文"},
		ConditionResults: []*endpoint.ConditionResult{{Condition: "[STATUS] == 201", Success: true}},
	}
	for i := 0; i < 3; i++ {
		r := &suite.Result{Timestamp: start, Success: true, Duration: time.Duration(i+1) * time.Second, EndpointResults: []*endpoint.Result{child}}
		if err := s.InsertSuiteResult(su, r); err != nil {
			t.Fatal(err)
		}
	}
	s.Close()
	s = reopen()
	status, err := s.GetSuiteStatusByKey(su.Key(), paging.NewSuiteStatusParams().WithPagination(1, 2))
	if err != nil || status == nil || len(status.Results) != 2 {
		t.Fatalf("suite page: status=%+v err=%v", status, err)
	}
	for i, r := range status.Results {
		if r.Name != su.Name || r.Group != su.Group || r.Duration != time.Duration(i+2)*time.Second || !r.Timestamp.Equal(start) {
			t.Errorf("suite result order/metadata changed: %+v", r)
		}
		if len(r.EndpointResults) != 1 || !reflect.DeepEqual(r.EndpointResults[0], child) {
			t.Errorf("suite child lost persisted fields: got %+v, want %+v", r.EndpointResults, child)
		}
	}
	page2, err := s.GetSuiteStatusByKey(su.Key(), paging.NewSuiteStatusParams().WithPagination(2, 2))
	if err != nil || page2 == nil || len(page2.Results) != 1 || page2.Results[0].Duration != time.Second {
		t.Fatalf("suite page 2: status=%+v err=%v", page2, err)
	}
	endpoints, err := s.GetAllEndpointStatuses(paging.NewEndpointStatusParams().WithResults(1, 10))
	if err != nil || len(endpoints) != 0 {
		t.Fatalf("suite children leaked into endpoint list: count=%d err=%v", len(endpoints), err)
	}
	if deleted := s.DeleteAllSuiteStatusesNotInKeys(nil); deleted != 1 {
		t.Fatalf("deleted %d suites, want 1", deleted)
	}
	if err := s.InsertSuiteResult(su, &suite.Result{Timestamp: start.Add(time.Minute), Success: false}); err != nil {
		t.Fatal(err)
	}
	status, err = s.GetSuiteStatusByKey(su.Key(), nil)
	if err != nil || status == nil || len(status.Results) != 1 || status.Results[0].Success {
		t.Fatalf("deleted suite history revived: status=%+v err=%v", status, err)
	}
	s.Clear()
	statuses, err := s.GetAllSuiteStatuses(nil)
	if err != nil || len(statuses) != 0 {
		t.Fatalf("Clear retained suites: count=%d err=%v", len(statuses), err)
	}
}

func TestClickHouseResultRetentionPreservesUptime(t *testing.T) {
	s, reopen := newClickHouseTestStoreWithLimits(t, 3, 2)
	ep := &endpoint.Endpoint{Name: "retention", Group: "group"}
	start := time.Now().UTC().Truncate(time.Hour)
	for i := 0; i < 5; i++ {
		if err := s.InsertEndpointResult(ep, &endpoint.Result{
			Timestamp: start.Add(time.Duration(i) * time.Minute), Success: i%2 == 0,
			Duration: time.Duration(i+1) * time.Millisecond,
		}); err != nil {
			t.Fatal(err)
		}
	}
	s.Close()
	s = reopen()
	status, err := s.GetEndpointStatusByKey(ep.Key(), paging.NewEndpointStatusParams().WithResults(1, 100).WithEvents(1, 100))
	if err != nil || status == nil || len(status.Results) != 3 || len(status.Events) != 2 {
		t.Fatalf("retention limits: status=%+v err=%v", status, err)
	}
	uptime, err := s.GetUptimeByKey(ep.Key(), start.Add(-time.Hour), start.Add(time.Hour))
	if err != nil || uptime != 0.6 {
		t.Fatalf("uptime must include results outside UI retention: uptime=%f err=%v", uptime, err)
	}
	average, err := s.GetAverageResponseTimeByKey(ep.Key(), start.Add(-time.Hour), start.Add(time.Hour))
	if err != nil || average != 3 {
		t.Fatalf("response time must include results outside UI retention: average=%d err=%v", average, err)
	}
}
