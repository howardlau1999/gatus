// Package clickhouse implements Gatus storage using ClickHouse's HTTP interface.
package clickhouse

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/TwiN/gatus/v5/storage"
)

type Store struct {
	mu                                            sync.Mutex
	config                                        storage.ClickHouseConfig
	client                                        *http.Client
	maximumNumberOfResults, maximumNumberOfEvents int
	version                                       uint64
	importSource                                  string
	closed                                        bool
	entities                                      map[string]entityRow
	entitiesDirty                                 bool
}

func NewStore(cfg *storage.ClickHouseConfig, maximumNumberOfResults, maximumNumberOfEvents int) (*Store, error) {
	if cfg == nil {
		return nil, errors.New("clickhouse configuration is required")
	}
	c := *cfg
	if err := c.ValidateAndSetDefaults(); err != nil {
		return nil, err
	}
	if maximumNumberOfResults <= 0 {
		maximumNumberOfResults = storage.DefaultMaximumNumberOfResults
	}
	if maximumNumberOfEvents <= 0 {
		maximumNumberOfEvents = storage.DefaultMaximumNumberOfEvents
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	// Do not send database credentials to a redirected server.
	s := &Store{
		config: c,
		client: &http.Client{
			Transport:     transport,
			Timeout:       c.Timeout,
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		},
		maximumNumberOfResults: maximumNumberOfResults,
		maximumNumberOfEvents:  maximumNumberOfEvents,
		entities:               make(map[string]entityRow),
	}
	for _, schema := range schemas {
		if _, err := s.request(schema, nil); err != nil {
			s.client.CloseIdleConnections()
			return nil, fmt.Errorf("initialize clickhouse schema: %w", err)
		}
	}
	var entities []entityRow
	if err := s.query("SELECT * FROM gatus_entities FINAL", &entities); err != nil {
		s.client.CloseIdleConnections()
		return nil, err
	}
	for _, e := range entities {
		s.entities[e.Kind+"\x00"+e.Key] = e
		if e.Version > s.version {
			s.version = e.Version
		}
	}
	// Preserve ordering and replacement semantics after a restart even if the
	// wall clock has moved backwards since the last result or alert update.
	var versions []struct {
		Version uint64 `json:"version"`
	}
	if err := s.query(`SELECT max(version) AS version FROM (
		SELECT max(version) AS version FROM gatus_entities UNION ALL
		SELECT max(version) AS version FROM gatus_results UNION ALL
		SELECT max(version) AS version FROM gatus_events UNION ALL
		SELECT max(version) AS version FROM gatus_uptimes UNION ALL
		SELECT max(version) AS version FROM gatus_alerts)`, &versions); err != nil {
		s.client.CloseIdleConnections()
		return nil, err
	}
	if len(versions) > 0 && versions[0].Version > s.version {
		s.version = versions[0].Version
	}
	return s, nil
}

func (s *Store) request(query string, body []byte) ([]byte, error) {
	if s.closed {
		return nil, errors.New("clickhouse store is closed")
	}
	u, _ := url.Parse(s.config.Address)
	params := u.Query()
	params.Set("database", s.config.Database)
	params.Set("query", query)
	params.Set("wait_end_of_query", "1")
	params.Set("output_format_json_quote_64bit_integers", "0")
	// Force acknowledgement after insertion even if the server enables async insert globally.
	params.Set("async_insert", "0")
	u.RawQuery = params.Encode()
	req, err := http.NewRequest(http.MethodPost, u.String(), bytes.NewReader(body))
	if err != nil {
		return nil, errors.New("construct clickhouse request")
	}
	req.SetBasicAuth(s.config.Username, s.config.Password)
	req.Header.Set("Content-Type", "application/octet-stream")
	response, err := s.client.Do(req)
	if err != nil { // net/http errors include the URL/query; never echo connection credentials.
		var ue *url.Error
		if errors.As(err, &ue) {
			err = ue.Err
		}
		return nil, fmt.Errorf("clickhouse HTTP request failed: %w", err)
	}
	defer response.Body.Close()
	data, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, fmt.Errorf("read clickhouse response: %w", err)
	}
	if response.StatusCode != http.StatusOK || response.Header.Get("X-ClickHouse-Exception-Code") != "" {
		if len(data) > 4096 {
			data = data[:4096]
		}
		return nil, fmt.Errorf("clickhouse HTTP %d: %s", response.StatusCode, strings.TrimSpace(string(data)))
	}
	// A streamed exception can follow an HTTP 200 response. Query decoding also
	// checks trailing data; INSERT/DDL must have an entirely empty response.
	if !strings.Contains(query, "FORMAT JSONEachRow") && len(bytes.TrimSpace(data)) > 0 {
		return nil, fmt.Errorf("unexpected clickhouse response: %.4096s", data)
	}
	return data, nil
}

func (s *Store) query(query string, out any) error {
	data, err := s.request(query+" FORMAT JSONEachRow", nil)
	if err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	v := reflect.ValueOf(out).Elem()
	for {
		row := reflect.New(v.Type().Elem())
		err = decoder.Decode(row.Interface())
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return fmt.Errorf("decode clickhouse rows: %w", err)
		}
		v.Set(reflect.Append(v, row.Elem()))
	}
}

func (s *Store) insert(table string, rows any) (err error) {
	defer func() {
		// A failed response does not prove that the server rejected the write.
		// Invalidate generations before any caller can continue using old metadata.
		if err != nil && table == "gatus_entities" {
			s.entitiesDirty = true
		}
	}()
	value := reflect.ValueOf(rows)
	if value.Len() == 0 {
		return nil
	}
	var buf bytes.Buffer
	encoder := json.NewEncoder(&buf)
	for i := 0; i < value.Len(); i++ {
		if err := encoder.Encode(value.Index(i).Interface()); err != nil {
			return err
		}
	}
	data, err := s.request("INSERT INTO "+table+" FORMAT JSONEachRow", buf.Bytes())
	if err != nil {
		return err
	}
	if len(bytes.TrimSpace(data)) != 0 {
		return fmt.Errorf("unexpected clickhouse insert response: %.4096s", data)
	}
	return nil
}

// refreshEntities is called under mu before consulting cached entity metadata.
// A failed refresh leaves the cache dirty; later requests retry automatically.
func (s *Store) refreshEntities() error {
	if !s.entitiesDirty {
		return nil
	}
	var rows []entityRow
	if err := s.query("SELECT * FROM gatus_entities FINAL", &rows); err != nil {
		return fmt.Errorf("refresh clickhouse entity metadata: %w", err)
	}
	entities := make(map[string]entityRow, len(rows))
	for _, e := range rows {
		entities[e.Kind+"\x00"+e.Key] = e
		if e.Version > s.version {
			s.version = e.Version
		}
	}
	s.entities = entities
	s.entitiesDirty = false
	return nil
}

func quote(value string) string {
	return "'" + strings.NewReplacer("\\", "\\\\", "'", "\\'").Replace(value) + "'"
}

// nextVersion is called under mu. Clock rollback never reorders writes within
// one process; generation isolation also makes a deleted entity's history inert.
func (s *Store) nextVersion() uint64 {
	n := uint64(time.Now().UnixNano())
	if n <= s.version {
		n = s.version + 1
	}
	s.version = n
	return n
}

var schemas = []string{
	`CREATE TABLE IF NOT EXISTS gatus_entities (
		kind String, entity_key String, entity_group String, name String, generation String,
		standalone UInt8, deleted UInt8, version UInt64
	) ENGINE=ReplacingMergeTree(version) ORDER BY (kind,entity_key)`,
	`CREATE TABLE IF NOT EXISTS gatus_results (
		kind String, entity_key String, generation String, id String, timestamp_ns Int64,
		payload String, success UInt8, duration_ns Int64, suite_id String, include_uptime UInt8,
		event_types Array(String), event_timestamps Array(Int64), origin String, version UInt64
	) ENGINE=ReplacingMergeTree(version) ORDER BY (kind,entity_key,generation,timestamp_ns,id)`,
	`CREATE TABLE IF NOT EXISTS gatus_events (
		entity_key String, generation String, id String, timestamp_ns Int64, event_type String, version UInt64
	) ENGINE=ReplacingMergeTree(version) ORDER BY (entity_key,generation,id)`,
	`CREATE TABLE IF NOT EXISTS gatus_uptimes (
		entity_key String, generation String, id String, hour_unix_timestamp Int64,
		total_executions Int64, successful_executions Int64, total_response_time Int64, version UInt64
	) ENGINE=ReplacingMergeTree(version) ORDER BY (entity_key,generation,id)`,
	`CREATE TABLE IF NOT EXISTS gatus_alerts (
		entity_key String, generation String, checksum String, resolve_key String,
		successes Int64, deleted UInt8, version UInt64
	) ENGINE=ReplacingMergeTree(version) ORDER BY (entity_key,generation,checksum)`,
}
