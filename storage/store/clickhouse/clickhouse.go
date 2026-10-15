package clickhouse

import (
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/TwiN/gatus/v5/alerting/alert"
	"github.com/TwiN/gatus/v5/config/endpoint"
	"github.com/TwiN/gatus/v5/config/key"
	"github.com/TwiN/gatus/v5/config/suite"
	"github.com/TwiN/gatus/v5/storage/store/common"
	"github.com/TwiN/gatus/v5/storage/store/common/paging"
	"github.com/TwiN/logr"
	"github.com/google/uuid"
)

func (s *Store) entity(kind, key string) (entityRow, error) {
	if err := s.refreshEntities(); err != nil {
		return entityRow{}, err
	}
	e, ok := s.entities[kind+"\x00"+key]
	if !ok || e.Deleted != 0 {
		if kind == "suite" {
			return entityRow{}, common.ErrSuiteNotFound
		}
		return entityRow{}, common.ErrEndpointNotFound
	}
	return e, nil
}
func (s *Store) prepareEntity(kind, key, group, name string, standalone bool) (entityRow, bool, error) {
	e, err := s.entity(kind, key)
	if err != nil && !errors.Is(err, common.ErrEndpointNotFound) && !errors.Is(err, common.ErrSuiteNotFound) {
		return entityRow{}, false, err
	}
	if err == nil && e.Name == name && e.Group == group && (!standalone || e.Standalone != 0) {
		return e, false, nil
	}
	if err != nil {
		e = entityRow{Kind: kind, Key: key, Generation: uuid.NewString()}
	}
	e.Name = name
	e.Group = group
	e.Standalone |= boolByte(standalone)
	e.Version = s.nextVersion()
	return e, true, nil
}
func (s *Store) ensureEntity(kind, key, group, name string, standalone bool) (entityRow, error) {
	e, changed, err := s.prepareEntity(kind, key, group, name, standalone)
	if err != nil {
		return entityRow{}, err
	}
	if !changed {
		return e, nil
	}
	if err := s.insert("gatus_entities", []entityRow{e}); err != nil {
		return entityRow{}, err
	}
	s.entities[kind+"\x00"+key] = e
	return e, nil
}
func entityWhere(e entityRow) string {
	return "entity_key=" + quote(e.Key) + " AND generation=" + quote(e.Generation)
}
func resultWhere(e entityRow) string { return "kind=" + quote(e.Kind) + " AND " + entityWhere(e) }

// Suite deletion hides its child results even if the child is also monitored independently.
func endpointResultWhere(e entityRow) string {
	return resultWhere(e) + ` AND (suite_id='' OR suite_id IN (
		SELECT id FROM gatus_results FINAL WHERE kind='suite' AND (entity_key,generation) IN (
			SELECT entity_key,generation FROM gatus_entities FINAL WHERE kind='suite' AND deleted=0)))`
}

func (s *Store) markLive() error {
	s.importSource = ""
	if err := s.refreshEntities(); err != nil {
		return err
	}
	if _, ok := s.entities["lifecycle\x00live"]; ok {
		return nil
	}
	e := entityRow{Kind: "lifecycle", Key: "live", Name: "live", Generation: uuid.NewString(), Version: s.nextVersion()}
	if err := s.insert("gatus_entities", []entityRow{e}); err != nil {
		return err
	}
	s.entities["lifecycle\x00live"] = e
	return nil
}

// The Store contract has no operation ID. Each call is a distinct observation,
// including equal timestamps; migration supplies stable IDs explicitly.
func (s *Store) InsertEndpointResult(ep *endpoint.Endpoint, result *endpoint.Result) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if ep == nil || result == nil {
		return errors.New("endpoint and result must not be nil")
	}
	if err := s.markLive(); err != nil {
		return err
	}
	e, err := s.ensureEntity("endpoint", ep.Key(), ep.Group, ep.Name, true)
	if err != nil {
		return err
	}
	var previous []resultRow
	if err = s.query("SELECT * FROM gatus_results FINAL WHERE "+resultWhere(e)+" AND suite_id='' ORDER BY (origin='live') DESC,id DESC LIMIT 1", &previous); err != nil {
		return err
	}
	version := s.nextVersion()
	r, err := newEndpointResult(e, fmt.Sprintf("live:%020d:%s", version, uuid.NewString()), result, "", true, version)
	if err != nil {
		return err
	}
	if len(previous) == 0 {
		// Historical events can outlive retained result rows in a migrated snapshot.
		var oldEvents []eventRow
		if err = s.query("SELECT * FROM gatus_events FINAL WHERE "+entityWhere(e)+" ORDER BY id DESC LIMIT 1", &oldEvents); err != nil {
			return err
		}
		if len(oldEvents) == 0 {
			r.EventTypes = append(r.EventTypes, string(endpoint.EventStart))
			r.EventTimestamps = append(r.EventTimestamps, result.Timestamp.Add(-50*time.Millisecond).UnixNano())
		}
		if len(oldEvents) == 0 || oldEvents[0].Type != string(endpoint.NewEventFromResult(result).Type) {
			r.EventTypes = append(r.EventTypes, string(endpoint.NewEventFromResult(result).Type))
			r.EventTimestamps = append(r.EventTimestamps, result.Timestamp.UnixNano())
		}
	} else if previous[0].Success != r.Success {
		r.EventTypes = append(r.EventTypes, string(endpoint.NewEventFromResult(result).Type))
		r.EventTimestamps = append(r.EventTimestamps, result.Timestamp.UnixNano())
	}
	// One acknowledged INSERT persists the result, its event transitions and its
	// uptime contribution together. No read-modify-write aggregate can lose data.
	return s.insert("gatus_results", []resultRow{r})
}

func (s *Store) GetEndpointStatus(group, name string, p *paging.EndpointStatusParams) (*endpoint.Status, error) {
	return s.GetEndpointStatusByKey(key.ConvertGroupAndNameToKey(group, name), p)
}
func (s *Store) GetEndpointStatusByKey(key string, p *paging.EndpointStatusParams) (*endpoint.Status, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.endpointStatus(key, p)
}
func pageWindow(page, size, maximum int) (int, int) {
	if size <= 0 {
		return 0, 0
	}
	if page < 1 {
		page = 1
	}
	// Check before multiplying so a user-provided page cannot overflow int.
	if page-1 > maximum/size {
		return 0, 0
	}
	offset := (page - 1) * size
	if offset >= maximum {
		return 0, 0
	}
	if size > maximum-offset {
		size = maximum - offset
	}
	return offset, size
}
func (s *Store) endpointStatus(key string, p *paging.EndpointStatusParams) (*endpoint.Status, error) {
	e, err := s.entity("endpoint", key)
	if err != nil {
		return nil, err
	}
	status := endpoint.NewStatus(e.Group, e.Name)
	status.Key = e.Key
	if p == nil {
		p = paging.NewEndpointStatusParams()
	}
	offset, size := pageWindow(p.ResultsPage, p.ResultsPageSize, s.maximumNumberOfResults)
	if size > 0 {
		var rows []resultRow
		if err = s.query(fmt.Sprintf("SELECT * FROM gatus_results FINAL WHERE %s ORDER BY (origin='live') DESC,id DESC LIMIT %d OFFSET %d", endpointResultWhere(e), size, offset), &rows); err != nil {
			return nil, err
		}
		for _, row := range rows {
			var r persistedResult
			if err = json.Unmarshal([]byte(row.Payload), &r); err != nil {
				return nil, fmt.Errorf("decode endpoint result: %w", err)
			}
			status.Results = append(status.Results, r.result())
		}
		slices.Reverse(status.Results)
	}
	offset, size = pageWindow(p.EventsPage, p.EventsPageSize, s.maximumNumberOfEvents)
	if size > 0 {
		query := fmt.Sprintf(`SELECT timestamp_ns,event_type FROM (
			SELECT timestamp_ns,event_type,id,0 AS live,0 AS event_index FROM gatus_events FINAL WHERE %s
			UNION ALL SELECT tupleElement(event,2) AS timestamp_ns,tupleElement(event,1) AS event_type,id,1 AS live,tupleElement(event,3) AS event_index
			FROM gatus_results FINAL ARRAY JOIN arrayZip(event_types,event_timestamps,arrayEnumerate(event_types)) AS event WHERE %s
		) ORDER BY live DESC,id DESC,event_index DESC LIMIT %d OFFSET %d`, entityWhere(e), resultWhere(e), size, offset)
		var rows []eventRow
		if err = s.query(query, &rows); err != nil {
			return nil, err
		}
		for _, row := range rows {
			status.Events = append(status.Events, &endpoint.Event{Type: endpoint.EventType(row.Type), Timestamp: time.Unix(0, row.Timestamp).UTC()})
		}
		slices.Reverse(status.Events)
	}
	return status, nil
}
func (s *Store) GetAllEndpointStatuses(p *paging.EndpointStatusParams) ([]*endpoint.Status, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.refreshEntities(); err != nil {
		return nil, err
	}
	// Metadata may have been written before a failed first result. Only completed
	// standalone observations belong on the endpoint overview, matching SQL.
	var keys []struct {
		Key string `json:"entity_key"`
	}
	if err := s.query(`SELECT DISTINCT entity_key FROM gatus_results FINAL
		WHERE kind='endpoint' AND suite_id='' AND (entity_key,generation) IN (
			SELECT entity_key,generation FROM gatus_entities FINAL WHERE kind='endpoint' AND deleted=0)
		ORDER BY entity_key`, &keys); err != nil {
		return nil, err
	}
	out := make([]*endpoint.Status, 0, len(keys))
	for _, row := range keys {
		status, err := s.endpointStatus(row.Key, p)
		if err != nil {
			return nil, err
		}
		out = append(out, status)
	}
	return out, nil
}

func (s *Store) InsertSuiteResult(su *suite.Suite, result *suite.Result) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if su == nil || result == nil {
		return errors.New("suite and result must not be nil")
	}
	for _, r := range result.EndpointResults {
		if r == nil {
			return errors.New("suite endpoint result must not be nil")
		}
	}
	if err := s.markLive(); err != nil {
		return err
	}
	e, err := s.ensureEntity("suite", su.Key(), su.Group, su.Name, false)
	if err != nil {
		return err
	}
	version := s.nextVersion()
	id := fmt.Sprintf("live:%020d:%s", version, uuid.NewString())
	r, err := newSuiteResult(e, id, result, version)
	if err != nil {
		return err
	}
	rows := []resultRow{r}
	for index, epResult := range result.EndpointResults {
		if epResult == nil {
			return errors.New("suite endpoint result must not be nil")
		}
		ep, err := s.ensureEntity("endpoint", key.ConvertGroupAndNameToKey(su.Group, epResult.Name), su.Group, epResult.Name, false)
		if err != nil {
			return err
		}
		child, err := newEndpointResult(ep, fmt.Sprintf("%s:%06d", id, index), epResult, id, false, version)
		if err != nil {
			return err
		}
		rows = append(rows, child)
	}
	return s.insert("gatus_results", rows)
}
func (s *Store) suiteStatus(key string, p *paging.SuiteStatusParams) (*suite.Status, error) {
	e, err := s.entity("suite", key)
	if errors.Is(err, common.ErrSuiteNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	status := &suite.Status{Key: e.Key, Name: e.Name, Group: e.Group, Results: []*suite.Result{}}
	if p == nil {
		p = paging.NewSuiteStatusParams()
	}
	page, size := p.Page, p.PageSize
	if size <= 0 {
		size = 20
	}
	offset, size := pageWindow(page, size, s.maximumNumberOfResults)
	if size == 0 {
		return status, nil
	}
	var rows []resultRow
	if err = s.query(fmt.Sprintf("SELECT * FROM gatus_results FINAL WHERE %s ORDER BY (origin='live') DESC,id DESC LIMIT %d OFFSET %d", resultWhere(e), size, offset), &rows); err != nil {
		return nil, err
	}
	for _, row := range rows {
		var p persistedSuite
		if err = json.Unmarshal([]byte(row.Payload), &p); err != nil {
			return nil, err
		}
		if p.Result == nil {
			return nil, errors.New("empty stored suite result")
		}
		r := p.Result
		r.Name = e.Name
		r.Group = e.Group
		r.EndpointResults = make([]*endpoint.Result, 0, len(p.Endpoints))
		for _, ep := range p.Endpoints {
			r.EndpointResults = append(r.EndpointResults, ep.result())
		}
		status.Results = append(status.Results, r)
	}
	slices.Reverse(status.Results)
	return status, nil
}
func (s *Store) GetSuiteStatusByKey(key string, p *paging.SuiteStatusParams) (*suite.Status, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.suiteStatus(key, p)
}
func (s *Store) GetAllSuiteStatuses(p *paging.SuiteStatusParams) ([]*suite.Status, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.refreshEntities(); err != nil {
		return nil, err
	}
	keys := []string{}
	for _, e := range s.entities {
		if e.Kind == "suite" && e.Deleted == 0 {
			keys = append(keys, e.Key)
		}
	}
	slices.Sort(keys)
	statuses := make([]*suite.Status, 0, len(keys))
	for _, key := range keys {
		status, err := s.suiteStatus(key, p)
		if err != nil {
			return nil, err
		}
		statuses = append(statuses, status)
	}
	return statuses, nil
}

func (s *Store) uptimes(key string, from, to time.Time) ([]uptimeRow, error) {
	if from.After(to) {
		return nil, common.ErrInvalidTimeRange
	}
	e, err := s.entity("endpoint", key)
	if err != nil {
		return nil, err
	}
	query := fmt.Sprintf(`SELECT hour_unix_timestamp,sum(total_executions) AS total_executions,
		sum(successful_executions) AS successful_executions,sum(total_response_time) AS total_response_time
		FROM (SELECT hour_unix_timestamp,total_executions,successful_executions,total_response_time
		FROM gatus_uptimes FINAL WHERE %s
		UNION ALL SELECT intDiv(timestamp_ns,3600000000000)*3600 AS hour_unix_timestamp,
		toInt64(1) AS total_executions,toInt64(success) AS successful_executions,intDiv(duration_ns,1000000) AS total_response_time
		FROM gatus_results FINAL WHERE %s AND include_uptime=1)
		WHERE hour_unix_timestamp >= %d AND hour_unix_timestamp <= %d
		GROUP BY hour_unix_timestamp`, entityWhere(e), resultWhere(e), from.Unix(), to.Unix())
	var rows []uptimeRow
	err = s.query(query, &rows)
	return rows, err
}
func (s *Store) GetUptimeByKey(key string, from, to time.Time) (float64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rows, err := s.uptimes(key, from, to)
	if err != nil {
		return 0, err
	}
	var total, success int64
	for _, row := range rows {
		total += row.Total
		success += row.Successful
	}
	if total == 0 {
		return 0, nil
	}
	return float64(success) / float64(total), nil
}
func (s *Store) GetAverageResponseTimeByKey(key string, from, to time.Time) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rows, err := s.uptimes(key, from, to)
	if err != nil {
		return 0, err
	}
	var total, response int64
	for _, row := range rows {
		total += row.Total
		response += row.ResponseTime
	}
	if total == 0 {
		return 0, nil
	}
	return int(response / total), nil
}
func (s *Store) GetHourlyAverageResponseTimeByKey(key string, from, to time.Time) (map[int64]int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rows, err := s.uptimes(key, from, to)
	if err != nil {
		return nil, err
	}
	out := map[int64]int{}
	for _, row := range rows {
		if row.Total > 0 {
			out[row.Timestamp] = int(row.ResponseTime / row.Total)
		}
	}
	return out, nil
}
func (s *Store) HasEndpointStatusNewerThan(key string, timestamp time.Time) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if timestamp.IsZero() {
		return false, errors.New("timestamp is zero")
	}
	e, err := s.entity("endpoint", key)
	if errors.Is(err, common.ErrEndpointNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	var rows []struct {
		Count uint64 `json:"count"`
	}
	err = s.query(fmt.Sprintf("SELECT count() AS count FROM gatus_results FINAL WHERE %s AND timestamp_ns>%d", endpointResultWhere(e), timestamp.UnixNano()), &rows)
	if err != nil {
		return false, err
	}
	return rows[0].Count > 0, nil
}

func (s *Store) deleteEntities(kind string, keys []string) int {
	if err := s.refreshEntities(); err != nil {
		logr.Errorf("[clickhouse] refresh metadata for cleanup: %s", err)
		return 0
	}
	keep := map[string]bool{}
	for _, key := range keys {
		keep[key] = true
	}
	var rows []entityRow
	for _, e := range s.entities {
		if e.Kind == kind && e.Deleted == 0 && !keep[e.Key] {
			e.Deleted = 1
			e.Version = s.nextVersion()
			rows = append(rows, e)
		}
	}
	if len(rows) > 0 {
		if err := s.markLive(); err != nil {
			logr.Errorf("[clickhouse] mark cleanup as live: %s", err)
			return 0
		}
	}
	if err := s.insert("gatus_entities", rows); err != nil {
		logr.Errorf("[clickhouse] delete %s statuses: %s", kind, err)
		return 0
	}
	for _, e := range rows {
		s.entities[e.Kind+"\x00"+e.Key] = e
	}
	return len(rows)
}
func (s *Store) DeleteAllEndpointStatusesNotInKeys(keys []string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.deleteEntities("endpoint", keys)
}
func (s *Store) DeleteAllSuiteStatusesNotInKeys(keys []string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.deleteEntities("suite", keys)
}

func (s *Store) GetTriggeredEndpointAlert(ep *endpoint.Endpoint, a *alert.Alert) (bool, string, int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, err := s.entity("endpoint", ep.Key())
	if errors.Is(err, common.ErrEndpointNotFound) {
		return false, "", 0, nil
	}
	if err != nil {
		return false, "", 0, err
	}
	var rows []alertRow
	err = s.query("SELECT * FROM gatus_alerts FINAL WHERE "+entityWhere(e)+" AND checksum="+quote(a.Checksum())+" AND deleted=0", &rows)
	if err != nil || len(rows) == 0 {
		return false, "", 0, err
	}
	return true, rows[0].ResolveKey, rows[0].Successes, nil
}
func (s *Store) UpsertTriggeredEndpointAlert(ep *endpoint.Endpoint, a *alert.Alert) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.markLive(); err != nil {
		return err
	}
	e, err := s.ensureEntity("endpoint", ep.Key(), ep.Group, ep.Name, false)
	if err != nil {
		return err
	}
	return s.insert("gatus_alerts", []alertRow{{Key: e.Key, Generation: e.Generation, Checksum: a.Checksum(), ResolveKey: a.ResolveKey, Successes: ep.NumberOfSuccessesInARow, Version: s.nextVersion()}})
}
func (s *Store) DeleteTriggeredEndpointAlert(ep *endpoint.Endpoint, a *alert.Alert) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.markLive(); err != nil {
		return err
	}
	e, err := s.entity("endpoint", ep.Key())
	if errors.Is(err, common.ErrEndpointNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	return s.insert("gatus_alerts", []alertRow{{Key: e.Key, Generation: e.Generation, Checksum: a.Checksum(), Deleted: 1, Version: s.nextVersion()}})
}
func (s *Store) DeleteAllTriggeredAlertsNotInChecksumsByEndpoint(ep *endpoint.Endpoint, checksums []string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, err := s.entity("endpoint", ep.Key())
	if err != nil {
		return 0
	}
	var rows []alertRow
	if err = s.query("SELECT * FROM gatus_alerts FINAL WHERE "+entityWhere(e)+" AND deleted=0", &rows); err != nil {
		logr.Errorf("[clickhouse] read alerts for cleanup: %s", err)
		return 0
	}
	var removed []alertRow
	for _, row := range rows {
		if !slices.Contains(checksums, row.Checksum) {
			row.Deleted = 1
			row.Version = s.nextVersion()
			removed = append(removed, row)
		}
	}
	if len(removed) > 0 {
		if err := s.markLive(); err != nil {
			logr.Errorf("[clickhouse] mark alert cleanup as live: %s", err)
			return 0
		}
	}
	if err = s.insert("gatus_alerts", removed); err != nil {
		logr.Errorf("[clickhouse] delete alerts: %s", err)
		return 0
	}
	return len(removed)
}

// Clear hides all entity generations in one atomic insert. Old immutable rows
// remain available for auditing and cannot reappear when a key is reused.
func (s *Store) Clear() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.markLive(); err != nil {
		logr.Errorf("[clickhouse] mark clear as live: %s", err)
		return
	}
	var rows []entityRow
	for _, e := range s.entities {
		if e.Deleted == 0 && (e.Kind == "endpoint" || e.Kind == "suite") {
			e.Deleted = 1
			e.Version = s.nextVersion()
			rows = append(rows, e)
		}
	}
	if err := s.insert("gatus_entities", rows); err != nil {
		logr.Errorf("[clickhouse] clear: %s", err)
		return
	}
	for _, e := range rows {
		s.entities[e.Kind+"\x00"+e.Key] = e
	}
}
func (s *Store) Save() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errors.New("clickhouse store is closed")
	}
	return nil
}
func (s *Store) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	s.client.CloseIdleConnections()
}
