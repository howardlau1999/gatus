package clickhouse

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/TwiN/gatus/v5/storage"
)

func TestHTTPFailuresAreNotAcknowledged(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name            string
		status          int
		body, exception string
	}{
		{"server error", 500, "DB::Exception: insert failed", ""},
		{"late streamed error", 200, "Code: 241. DB::Exception: memory limit exceeded", ""},
		{"exception header", 200, "", "241"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				user, password, ok := r.BasicAuth()
				if !ok || user != "gatus" || password != "secret" {
					t.Error("missing Basic authentication")
				}
				if strings.Contains(r.URL.RawQuery, "secret") {
					t.Error("password leaked into URL")
				}
				if r.URL.Query().Get("wait_end_of_query") != "1" || r.URL.Query().Get("async_insert") != "0" {
					t.Error("writes must be acknowledged synchronously")
				}
				w.Header().Set("X-ClickHouse-Exception-Code", tc.exception)
				w.WriteHeader(tc.status)
				_, _ = io.WriteString(w, tc.body)
			}))
			defer server.Close()
			s := &Store{config: storage.ClickHouseConfig{Address: server.URL, Database: "gatus", Username: "gatus", Password: "secret"}, client: server.Client()}
			if err := s.insert("gatus_events", []eventRow{{ID: "one"}}); err == nil {
				t.Fatal("failed insert was acknowledged")
			}
		})
	}
}

func TestQueryRejectsTrailingException(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "{\"count\":1}\nCode: 241. DB::Exception: memory limit exceeded\n")
	}))
	defer server.Close()
	s := &Store{config: storage.ClickHouseConfig{Address: server.URL}, client: server.Client()}
	var rows []struct {
		Count int `json:"count"`
	}
	if err := s.query("SELECT count() AS count FROM gatus_results", &rows); err == nil {
		t.Fatal("accepted partial query response")
	}
}

func TestNewStoreRejectsRedirect(t *testing.T) {
	var received bool
	destination := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { received = true }))
	defer destination.Close()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, destination.URL, http.StatusTemporaryRedirect)
	}))
	defer server.Close()
	_, err := NewStore(&storage.ClickHouseConfig{Address: server.URL, Database: "gatus", Password: "secret", Timeout: time.Second}, 10, 10)
	if err == nil || received {
		t.Fatal("followed database redirect or acknowledged it")
	}
}
