package clickhouse

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/TwiN/gatus/v5/config/endpoint"
	"github.com/TwiN/gatus/v5/config/suite"
	"github.com/TwiN/gatus/v5/storage"
	"github.com/TwiN/gatus/v5/storage/store/common/paging"
)

// The server really commits the write before this transport discards its
// response. This exercises the ambiguous outcome of a lost acknowledgement.
type lostResponseTransport struct {
	base                 http.RoundTripper
	dropMetadataResponse atomic.Bool
	failMetadataReload   atomic.Bool
}

func (t *lostResponseTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	query := req.URL.Query().Get("query")
	if strings.HasPrefix(query, "SELECT * FROM gatus_entities FINAL") && t.failMetadataReload.CompareAndSwap(true, false) {
		return nil, errors.New("injected temporary metadata read failure")
	}
	response, err := t.base.RoundTrip(req)
	if err == nil && response.StatusCode == http.StatusOK && strings.HasPrefix(query, "INSERT INTO gatus_entities ") && t.dropMetadataResponse.CompareAndSwap(true, false) {
		_, _ = io.Copy(io.Discard, response.Body)
		_ = response.Body.Close()
		return nil, io.ErrUnexpectedEOF
	}
	return response, err
}

func (t *lostResponseTransport) CloseIdleConnections() {
	if transport, ok := t.base.(interface{ CloseIdleConnections() }); ok {
		transport.CloseIdleConnections()
	}
}

func recoveryTestStore(t *testing.T) (*Store, func() *Store) {
	t.Helper()
	address := os.Getenv("GATUS_TEST_CLICKHOUSE_URL")
	if address == "" {
		t.Skip("set GATUS_TEST_CLICKHOUSE_URL for ClickHouse fault injection tests")
	}
	u, err := url.Parse(address)
	if err != nil {
		t.Fatal(err)
	}
	username, password := "default", ""
	if u.User != nil {
		username = u.User.Username()
		password, _ = u.User.Password()
		u.User = nil
	}
	database := fmt.Sprintf("gatus_recovery_%d", time.Now().UnixNano())
	admin := &http.Client{Timeout: 30 * time.Second}
	t.Cleanup(admin.CloseIdleConnections)
	ddl := func(query string) {
		t.Helper()
		req, err := http.NewRequest(http.MethodPost, u.String(), strings.NewReader(query))
		if err != nil {
			t.Fatal(err)
		}
		req.SetBasicAuth(username, password)
		response, err := admin.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		body, err := io.ReadAll(response.Body)
		if err != nil {
			t.Fatal(err)
		}
		if response.StatusCode != http.StatusOK || len(strings.TrimSpace(string(body))) != 0 {
			t.Fatalf("ClickHouse DDL: HTTP %d: %s", response.StatusCode, body)
		}
	}
	ddl("CREATE DATABASE " + database)
	t.Cleanup(func() { ddl("DROP DATABASE " + database + " SYNC") })
	open := func() *Store {
		t.Helper()
		s, err := NewStore(&storage.ClickHouseConfig{Address: u.String(), Database: database, Username: username, Password: password}, 100, 50)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(s.Close)
		return s
	}
	return open(), open
}

func TestMetadataLostAcknowledgementDoesNotHideNewResults(t *testing.T) {
	for _, operation := range []string{"delete", "clear"} {
		t.Run(operation, func(t *testing.T) {
			s, reopen := recoveryTestStore(t)
			ep := &endpoint.Endpoint{Name: "reused", Group: "recovery"}
			now := time.Now().UTC()
			if err := s.InsertEndpointResult(ep, &endpoint.Result{Timestamp: now, Duration: time.Millisecond, Success: false}); err != nil {
				t.Fatal(err)
			}
			fault := &lostResponseTransport{base: s.client.Transport}
			s.client.Transport = fault
			fault.dropMetadataResponse.Store(true)
			if operation == "clear" {
				s.Clear()
			} else if count := s.DeleteAllEndpointStatusesNotInKeys(nil); count != 0 {
				t.Fatalf("delete was acknowledged despite lost response: %d", count)
			}
			if fault.dropMetadataResponse.Load() || !s.entitiesDirty {
				t.Fatal("lost committed metadata write did not invalidate the cache")
			}
			// A transient reload failure must neither accept writes using the old
			// generation nor permanently poison the store.
			fault.failMetadataReload.Store(true)
			fresh := &endpoint.Result{Timestamp: now.Add(time.Second), Duration: 2 * time.Millisecond, Success: true}
			if err := s.InsertEndpointResult(ep, fresh); err == nil {
				t.Fatal("accepted a result while metadata refresh failed")
			}
			if !s.entitiesDirty {
				t.Fatal("failed refresh prematurely marked cache clean")
			}
			if err := s.InsertEndpointResult(ep, fresh); err != nil {
				t.Fatalf("store did not recover automatically: %v", err)
			}
			if s.entitiesDirty {
				t.Fatal("successful refresh left dirty metadata")
			}
			s.Close()
			restored := reopen()
			status, err := restored.GetEndpointStatusByKey(ep.Key(), paging.NewEndpointStatusParams().WithResults(1, 10).WithEvents(1, 10))
			if err != nil {
				t.Fatal(err)
			}
			if len(status.Results) != 1 || status.Results[0].Duration != fresh.Duration || !status.Results[0].Success {
				t.Fatalf("new generation result disappeared or old generation returned: %+v", status.Results)
			}
			if len(status.Events) != 2 || status.Events[0].Type != endpoint.EventStart || status.Events[1].Type != endpoint.EventHealthy {
				t.Fatalf("wrong generation events: %+v", status.Events)
			}
		})
	}
}

func TestSuiteOverviewReloadsAfterLostDeleteAcknowledgement(t *testing.T) {
	s, reopen := recoveryTestStore(t)
	su := &suite.Suite{Name: "suite", Group: "recovery"}
	if err := s.InsertSuiteResult(su, &suite.Result{Timestamp: time.Now().UTC(), Success: false}); err != nil {
		t.Fatal(err)
	}
	fault := &lostResponseTransport{base: s.client.Transport}
	s.client.Transport = fault
	fault.dropMetadataResponse.Store(true)
	if count := s.DeleteAllSuiteStatusesNotInKeys(nil); count != 0 {
		t.Fatalf("delete was acknowledged: %d", count)
	}
	if !s.entitiesDirty {
		t.Fatal("missing metadata invalidation")
	}
	statuses, err := s.GetAllSuiteStatuses(nil)
	if err != nil || len(statuses) != 0 {
		t.Fatalf("suite overview reused deleted cached metadata: %v %v", statuses, err)
	}
	if err := s.InsertSuiteResult(su, &suite.Result{Timestamp: time.Now().UTC(), Success: true}); err != nil {
		t.Fatal(err)
	}
	s.Close()
	s = reopen()
	status, err := s.GetSuiteStatusByKey(su.Key(), nil)
	if err != nil || status == nil || len(status.Results) != 1 || !status.Results[0].Success {
		t.Fatalf("recreated suite result was hidden after restart: %+v %v", status, err)
	}
}

func TestLostEntityCreationAcknowledgementReusesCommittedGeneration(t *testing.T) {
	s, _ := recoveryTestStore(t)
	if err := s.InsertEndpointResult(&endpoint.Endpoint{Name: "existing"}, &endpoint.Result{Timestamp: time.Now()}); err != nil {
		t.Fatal(err)
	}
	fault := &lostResponseTransport{base: s.client.Transport}
	s.client.Transport = fault
	fault.dropMetadataResponse.Store(true)
	ep := &endpoint.Endpoint{Name: "new"}
	result := &endpoint.Result{Timestamp: time.Now(), Success: true}
	if err := s.InsertEndpointResult(ep, result); err == nil {
		t.Fatal("lost entity creation response was acknowledged")
	}
	var committed []entityRow
	if err := s.query("SELECT * FROM gatus_entities FINAL WHERE kind='endpoint' AND entity_key="+quote(ep.Key()), &committed); err != nil || len(committed) != 1 {
		t.Fatalf("entity creation was not committed: %v %v", committed, err)
	}
	if err := s.InsertEndpointResult(ep, result); err != nil {
		t.Fatal(err)
	}
	active, err := s.entity("endpoint", ep.Key())
	if err != nil {
		t.Fatal(err)
	}
	if active.Generation != committed[0].Generation {
		t.Fatal("retry replaced the already committed generation")
	}
}
