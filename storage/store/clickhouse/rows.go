package clickhouse

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/TwiN/gatus/v5/config/endpoint"
	"github.com/TwiN/gatus/v5/config/suite"
)

type entityRow struct {
	Kind       string `json:"kind"`
	Key        string `json:"entity_key"`
	Group      string `json:"entity_group"`
	Name       string `json:"name"`
	Generation string `json:"generation"`
	Standalone uint8  `json:"standalone"`
	Deleted    uint8  `json:"deleted"`
	Version    uint64 `json:"version"`
}
type resultRow struct {
	Kind            string   `json:"kind"`
	Key             string   `json:"entity_key"`
	Generation      string   `json:"generation"`
	ID              string   `json:"id"`
	Timestamp       int64    `json:"timestamp_ns"`
	Payload         string   `json:"payload"`
	Success         uint8    `json:"success"`
	Duration        int64    `json:"duration_ns"`
	SuiteID         string   `json:"suite_id"`
	IncludeUptime   uint8    `json:"include_uptime"`
	EventTypes      []string `json:"event_types"`
	EventTimestamps []int64  `json:"event_timestamps"`
	Origin          string   `json:"origin"`
	Version         uint64   `json:"version"`
}
type eventRow struct {
	Key        string `json:"entity_key"`
	Generation string `json:"generation"`
	ID         string `json:"id"`
	Timestamp  int64  `json:"timestamp_ns"`
	Type       string `json:"event_type"`
	Version    uint64 `json:"version"`
}
type uptimeRow struct {
	Key          string `json:"entity_key"`
	Generation   string `json:"generation"`
	ID           string `json:"id"`
	Timestamp    int64  `json:"hour_unix_timestamp"`
	Total        int64  `json:"total_executions"`
	Successful   int64  `json:"successful_executions"`
	ResponseTime int64  `json:"total_response_time"`
	Version      uint64 `json:"version"`
}
type alertRow struct {
	Key        string `json:"entity_key"`
	Generation string `json:"generation"`
	Checksum   string `json:"checksum"`
	ResolveKey string `json:"resolve_key"`
	Successes  int    `json:"successes"`
	Deleted    uint8  `json:"deleted"`
	Version    uint64 `json:"version"`
}

// Endpoint's public JSON omits fields that SQL storage retains. Persist them
// explicitly, while still excluding response bodies, port and suite contexts.
type persistedResult struct {
	*endpoint.Result
	DNSRCode              string        `json:"dns_rcode"`
	IP                    string        `json:"ip"`
	Connected             bool          `json:"connected"`
	CertificateExpiration time.Duration `json:"certificate_expiration"`
	DomainExpiration      time.Duration `json:"domain_expiration"`
}

func persistResult(r *endpoint.Result) persistedResult {
	return persistedResult{r, r.DNSRCode, r.IP, r.Connected, r.CertificateExpiration, r.DomainExpiration}
}
func (p persistedResult) result() *endpoint.Result {
	r := p.Result
	if r == nil {
		r = &endpoint.Result{}
	}
	r.DNSRCode = p.DNSRCode
	r.IP = p.IP
	r.Connected = p.Connected
	r.CertificateExpiration = p.CertificateExpiration
	r.DomainExpiration = p.DomainExpiration
	return r
}

type persistedSuite struct {
	*suite.Result
	Endpoints []persistedResult `json:"persisted_endpoints"`
}

func boolByte(v bool) uint8 {
	if v {
		return 1
	}
	return 0
}
func newEndpointResult(e entityRow, id string, r *endpoint.Result, suiteID string, includeUptime bool, version uint64) (resultRow, error) {
	data, err := json.Marshal(persistResult(r))
	return resultRow{Kind: "endpoint", Key: e.Key, Generation: e.Generation, ID: id, Timestamp: r.Timestamp.UnixNano(), Payload: string(data), Success: boolByte(r.Success), Duration: int64(r.Duration), SuiteID: suiteID, IncludeUptime: boolByte(includeUptime), EventTypes: []string{}, EventTimestamps: []int64{}, Origin: "live", Version: version}, err
}
func newSuiteResult(e entityRow, id string, r *suite.Result, version uint64) (resultRow, error) {
	copy := *r
	copy.EndpointResults = nil
	p := persistedSuite{Result: &copy, Endpoints: make([]persistedResult, 0, len(r.EndpointResults))}
	for _, ep := range r.EndpointResults {
		if ep == nil {
			return resultRow{}, errors.New("suite endpoint result must not be nil")
		}
		p.Endpoints = append(p.Endpoints, persistResult(ep))
	}
	data, err := json.Marshal(p)
	return resultRow{Kind: "suite", Key: e.Key, Generation: e.Generation, ID: id, Timestamp: r.Timestamp.UnixNano(), Payload: string(data), Success: boolByte(r.Success), Duration: int64(r.Duration), EventTypes: []string{}, EventTimestamps: []int64{}, Origin: "live", Version: version}, err
}
