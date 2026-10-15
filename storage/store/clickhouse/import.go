package clickhouse

import (
	"errors"
	"fmt"

	"github.com/TwiN/gatus/v5/config/endpoint"
	"github.com/TwiN/gatus/v5/config/suite"
)

// ImportBatch preserves SQL history without deriving uptime or events from the
// (usually much shorter) retained result history. IDs must be deterministic and
// unique within each entity and row type, so an interrupted import can be retried.
type ImportBatch struct {
	Endpoints    []EndpointRow
	Results      []EndpointResultRow
	Events       []EventRow
	Uptimes      []UptimeRow
	Alerts       []AlertRow
	Suites       []SuiteRow
	SuiteResults []SuiteResultRow
}
type EndpointRow struct {
	Key, Name, Group string
	Standalone       bool
}
type SuiteRow struct{ Key, Name, Group string }
type EndpointResultRow struct {
	Key, ID       string
	Result        *endpoint.Result
	SuiteID       string
	IncludeUptime bool
}
type SuiteResultRow struct {
	Key, ID string
	Result  *suite.Result
}
type EventRow struct {
	Key, ID string
	Event   *endpoint.Event
}
type UptimeRow struct {
	Key, ID                                                             string
	Timestamp, TotalExecutions, SuccessfulExecutions, TotalResponseTime int64
}
type AlertRow struct {
	Key, Checksum, ResolveKey string
	Successes                 int
}

// ImportStats describes logical rows (after ReplacingMergeTree deduplication),
// not physical parts. Uptime totals include every historical aggregate bucket.
type ImportStats struct {
	Endpoints            int64 `json:"endpoints"`
	Suites               int64 `json:"suites"`
	Results              int64 `json:"results"`
	SuiteResults         int64 `json:"suite_results"`
	Conditions           int64 `json:"conditions"`
	Events               int64 `json:"events"`
	Uptimes              int64 `json:"uptimes"`
	Alerts               int64 `json:"alerts"`
	TotalExecutions      int64 `json:"total_executions"`
	SuccessfulExecutions int64 `json:"successful_executions"`
	TotalResponseTime    int64 `json:"total_response_time"`
}

// ImportStats verifies the currently reserved snapshot before starting Gatus.
// Like PrepareImport, it must only be used while monitoring is stopped.
func (s *Store) ImportStats() (*ImportStats, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.importSource == "" {
		return nil, errors.New("PrepareImport must succeed before ImportStats")
	}
	var rows []ImportStats
	query := `SELECT
		(SELECT count() FROM gatus_entities FINAL WHERE kind='endpoint' AND deleted=0) AS endpoints,
		(SELECT count() FROM gatus_entities FINAL WHERE kind='suite' AND deleted=0) AS suites,
		countIf(kind='endpoint') AS results,countIf(kind='suite') AS suite_results,
		sumIf(length(JSONExtractArrayRaw(payload,'conditionResults')),kind='endpoint') AS conditions,
		(SELECT count() FROM gatus_events FINAL) AS events,
		(SELECT count() FROM gatus_uptimes FINAL) AS uptimes,
		(SELECT count() FROM gatus_alerts FINAL WHERE deleted=0) AS alerts,
		(SELECT sum(total_executions) FROM gatus_uptimes FINAL) AS total_executions,
		(SELECT sum(successful_executions) FROM gatus_uptimes FINAL) AS successful_executions,
		(SELECT sum(total_response_time) FROM gatus_uptimes FINAL) AS total_response_time
		FROM gatus_results FINAL WHERE origin=` + quote(s.importSource)
	if err := s.query(query, &rows); err != nil {
		return nil, err
	}
	return &rows[0], nil
}

// PrepareImport reserves an empty target for one immutable source snapshot.
// Resume uses exactly the same sourceID. Once monitoring has started, importing
// a baseline again is refused to prevent double-counting historical uptime.
func (s *Store) PrepareImport(sourceID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.refreshEntities(); err != nil {
		return err
	}
	if sourceID == "" {
		return errors.New("migration source ID must not be empty")
	}
	var live []entityRow
	if err := s.query("SELECT * FROM gatus_entities FINAL WHERE kind='lifecycle' AND entity_key='live'", &live); err != nil {
		return err
	}
	if len(live) > 0 {
		return errors.New("target has already been used by live Gatus; migration must precede monitoring")
	}
	var markers []entityRow
	if err := s.query("SELECT * FROM gatus_entities FINAL WHERE kind='migration' AND entity_key='source'", &markers); err != nil {
		return err
	}
	if len(markers) > 0 && markers[0].Name != sourceID {
		return errors.New("target belongs to a different migration source snapshot")
	}
	var counts []struct {
		Count uint64 `json:"count"`
	}
	where := ""
	if len(markers) > 0 {
		where = " WHERE origin='live'"
	}
	if err := s.query("SELECT count() AS count FROM gatus_results FINAL"+where, &counts); err != nil {
		return err
	}
	if counts[0].Count != 0 {
		return errors.New("migration requires an empty target, or a matching incomplete import with no live writes")
	}
	if len(markers) == 0 {
		var existing []struct {
			Count uint64 `json:"count"`
		}
		if err := s.query("SELECT count() AS count FROM gatus_entities FINAL", &existing); err != nil {
			return err
		}
		if existing[0].Count != 0 {
			return errors.New("target already contains entities; refusing an untracked import")
		}
		if err := s.insert("gatus_entities", []entityRow{{Kind: "migration", Key: "source", Name: sourceID, Generation: sourceID, Version: s.nextVersion()}}); err != nil {
			return err
		}
	}
	s.importSource = sourceID
	return nil
}

func (s *Store) ImportBatch(batch *ImportBatch) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.importSource == "" {
		return errors.New("PrepareImport must succeed before ImportBatch")
	}
	if batch == nil {
		return errors.New("nil import batch")
	}
	var entities []entityRow
	for _, row := range batch.Endpoints {
		if e, changed, err := s.prepareEntity("endpoint", row.Key, row.Group, row.Name, row.Standalone); err != nil {
			return err
		} else if changed {
			entities = append(entities, e)
		}
	}
	for _, row := range batch.Suites {
		if e, changed, err := s.prepareEntity("suite", row.Key, row.Group, row.Name, false); err != nil {
			return err
		} else if changed {
			entities = append(entities, e)
		}
	}
	if err := s.insert("gatus_entities", entities); err != nil {
		return err
	}
	for _, e := range entities {
		s.entities[e.Kind+"\x00"+e.Key] = e
	}
	var results []resultRow
	for _, row := range batch.Results {
		if row.ID == "" || row.Result == nil || row.IncludeUptime {
			return errors.New("imported results require an ID and result, and must not include uptime")
		}
		e, err := s.entity("endpoint", row.Key)
		if err != nil {
			return err
		}
		r, err := newEndpointResult(e, row.ID, row.Result, row.SuiteID, false, s.nextVersion())
		if err != nil {
			return err
		}
		r.Origin = s.importSource
		results = append(results, r)
	}
	for _, row := range batch.SuiteResults {
		if row.ID == "" || row.Result == nil {
			return errors.New("imported suite result requires an ID and result")
		}
		e, err := s.entity("suite", row.Key)
		if err != nil {
			return err
		}
		r, err := newSuiteResult(e, row.ID, row.Result, s.nextVersion())
		if err != nil {
			return err
		}
		r.Origin = s.importSource
		results = append(results, r)
	}
	if err := s.insert("gatus_results", results); err != nil {
		return err
	}
	var events []eventRow
	for _, row := range batch.Events {
		if row.ID == "" || row.Event == nil {
			return errors.New("imported event requires an ID and event")
		}
		e, err := s.entity("endpoint", row.Key)
		if err != nil {
			return err
		}
		events = append(events, eventRow{Key: row.Key, Generation: e.Generation, ID: row.ID, Timestamp: row.Event.Timestamp.UnixNano(), Type: string(row.Event.Type), Version: s.nextVersion()})
	}
	if err := s.insert("gatus_events", events); err != nil {
		return err
	}
	var uptimes []uptimeRow
	for _, row := range batch.Uptimes {
		if row.ID == "" || row.TotalExecutions < 0 || row.SuccessfulExecutions < 0 || row.SuccessfulExecutions > row.TotalExecutions || row.TotalResponseTime < 0 {
			return fmt.Errorf("invalid imported uptime row for %q", row.Key)
		}
		e, err := s.entity("endpoint", row.Key)
		if err != nil {
			return err
		}
		uptimes = append(uptimes, uptimeRow{Key: row.Key, Generation: e.Generation, ID: row.ID, Timestamp: row.Timestamp, Total: row.TotalExecutions, Successful: row.SuccessfulExecutions, ResponseTime: row.TotalResponseTime, Version: s.nextVersion()})
	}
	if err := s.insert("gatus_uptimes", uptimes); err != nil {
		return err
	}
	var alerts []alertRow
	for _, row := range batch.Alerts {
		e, err := s.entity("endpoint", row.Key)
		if err != nil {
			return err
		}
		alerts = append(alerts, alertRow{Key: row.Key, Generation: e.Generation, Checksum: row.Checksum, ResolveKey: row.ResolveKey, Successes: row.Successes, Version: s.nextVersion()})
	}
	return s.insert("gatus_alerts", alerts)
}
