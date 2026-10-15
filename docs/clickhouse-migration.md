# Migrate SQLite history to ClickHouse

The migration command copies a **single immutable SQLite snapshot** into an
existing, dedicated ClickHouse database. It defaults to a read-only dry run;
`--apply` is required to write. It does not stop Gatus, change its configuration,
create the ClickHouse database, or delete SQLite data.

All eight SQL tables are covered: endpoints, suites, endpoint and suite results,
result conditions, events, uptime aggregates, and triggered alerts. Conditions
and suite children become part of the corresponding ClickHouse result payload.
Error lists, nanosecond timestamps/durations, endpoint keys, alert resolve keys,
and consecutive success counts are preserved.

Uptime aggregates are copied directly. They are **not** reconstructed from
retained results: a deployment can retain only about 38,000 detailed results
while its hourly/daily aggregate table represents over 720,000 executions.
Already purged SQLite detail cannot be recovered by migration.

## Build and dry run

Build with the Go version specified in `go.mod`:

```sh
mkdir -p bin
go build -o bin/gatus-migrate-clickhouse ./cmd/gatus-migrate-clickhouse
./bin/gatus-migrate-clickhouse --sqlite /secure/path/gatus-snapshot.db --dry-run
```

The script offers the same options, using `bin/gatus-migrate-clickhouse` when
present and otherwise running the Go command from the repository:

```sh
./scripts/migrate-sqlite-to-clickhouse.sh \
  --sqlite /secure/path/gatus-snapshot.db --dry-run
```

Set `GATUS_MIGRATE_BIN` to use a binary elsewhere. Omitting both `--apply` and
`--dry-run` also performs a dry run. No ClickHouse address or credentials are
needed for a dry run, and the target is never contacted.

The command checks required source columns and foreign keys, scans every row,
prints per-table counts and uptime totals, and computes a SHA-256 snapshot ID.
An invalid source, rejected target, interrupted request, or failed insert exits
nonzero. `--batch-size` defaults to 1,000 source rows; it changes neither the
snapshot identity nor the imported row IDs.

## Take a consistent SQLite snapshot

SQLite WAL databases may have recent writes only in `gatus.db-wal`. Copying only
`gatus.db` while the service is running can omit data. Use SQLite's backup API:

```sh
export GATUS_SQLITE_SOURCE=/path/to/live/gatus.db
export GATUS_SQLITE_SNAPSHOT=/secure/path/gatus-snapshot.db
umask 077
python3 - <<'PY'
import os
import pathlib
import sqlite3

source = pathlib.Path(os.environ["GATUS_SQLITE_SOURCE"]).resolve()
destination = pathlib.Path(os.environ["GATUS_SQLITE_SNAPSHOT"])
# Refuse to overwrite an earlier snapshot.
with destination.open("xb"):
    pass
reader = sqlite3.connect(source.as_uri() + "?mode=ro", uri=True)
writer = sqlite3.connect(str(destination))
try:
    reader.backup(writer)
    result = writer.execute("PRAGMA integrity_check").fetchone()[0]
    if result != "ok":
        raise RuntimeError("snapshot integrity check failed")
finally:
    writer.close()
    reader.close()
print(destination)
PY
```

A backup taken while Gatus runs is suitable for rehearsal. For the **final
cutover**, pause probe submissions and stop the old Gatus process before taking
the final snapshot; keep them paused until the import and verification finish.
This closes the gap between the snapshot and the new backend. The command opens
SQLite with `mode=ro` and `query_only`, and holds one consistent read transaction
across validation and import. Prefer a backup file over a long read transaction
on a live WAL database.

## Import

Choose the database name explicitly; there is no default. Have the ClickHouse
administrator create that dedicated database and grant the runtime/migration
account the required table creation, SELECT, and INSERT privileges. The backend
creates its `gatus_*` tables inside that database.

```sh
export GATUS_CLICKHOUSE_ADDRESS=http://clickhouse.example:8123
export GATUS_CLICKHOUSE_DATABASE=your_selected_database
export GATUS_CLICKHOUSE_USERNAME=gatus
read -r -s -p 'ClickHouse password: ' GATUS_CLICKHOUSE_PASSWORD
echo
export GATUS_CLICKHOUSE_PASSWORD

./scripts/migrate-sqlite-to-clickhouse.sh \
  --sqlite /secure/path/gatus-snapshot.db \
  --batch-size 1000 --apply
```

The address must use the ClickHouse **HTTP** interface, usually port 8123
(HTTP) or 8443 (HTTPS). Port 9000 is the native TCP protocol and cannot be used
by this backend. `--address`, `--database`, and `--username` override their
corresponding environment variables. `--password-env` selects another password
environment variable; the password is never a command-line argument.

Each source snapshot has a deterministic content identity. The target records
that identity before the first batch, and all historical row IDs derive from it
and their original SQLite IDs. Therefore:

- If an import fails halfway, rerun the same command using the **same snapshot**.
  Logical rows and uptime totals are not duplicated, even before background
  ClickHouse merges occur.
- The snapshot can be moved or renamed. Changing its data creates a different
  identity and is rejected by the existing target.
- A new rehearsal/final snapshot needs a **new empty target database**, or an
  administrator must explicitly discard the earlier unused rehearsal target.
  The tool never clears it automatically.
- Once Gatus has written live data to the target, migration into that database
  is refused. This prevents imported baseline uptime from overlapping live
  results. Do not use the migration command as ongoing replication.
- Do not run migration and Gatus concurrently against the same database, or run
  two different imports concurrently. The source marker is an operational guard,
  not a distributed lock.

## Verify and switch

The command automatically checks all logical table counts, embedded condition
counts, and uptime sums against the source, and prints `Verified` only when they
match. Any mismatch is a nonzero exit. You can also compare the source summary
manually using `FINAL` (plain physical row counts can include older replacement
versions after a retry):

```sql
SELECT kind, count() FROM gatus_entities FINAL
WHERE kind IN ('endpoint', 'suite') AND deleted = 0 GROUP BY kind;
SELECT kind, count() FROM gatus_results FINAL GROUP BY kind;
SELECT count() FROM gatus_events FINAL;
SELECT count(), sum(total_executions), sum(successful_executions),
       sum(total_response_time) FROM gatus_uptimes FINAL;
SELECT count() FROM gatus_alerts FINAL WHERE deleted = 0;
```

The result counts should equal `endpoint_results` and `suite_results` separately;
the uptime sums must match the printed source totals. The endpoint entity count
includes suite-only children, whereas the main dashboard lists standalone
endpoints. Verify several endpoint histories, event transitions, 24-hour/7-day/
30-day uptime and response-time views, suites, and active alerts after starting
Gatus.

Use the same chosen database in Gatus:

```yaml
storage:
  type: clickhouse
  maximum-number-of-results: 2000
  maximum-number-of-events: 50
  clickhouse:
    address: "${GATUS_CLICKHOUSE_ADDRESS}"
    database: "${GATUS_CLICKHOUSE_DATABASE}"
    username: "${GATUS_CLICKHOUSE_USERNAME}"
    password: "${GATUS_CLICKHOUSE_PASSWORD}"
    timeout: 30s
```

Remove the old `storage.path` and disable `storage.caching` for ClickHouse. Keep
all unrelated web, endpoint, and alerting settings. Supply the environment
variables to the actual Gatus supervisor process, not just the migration shell.
Install the new binary, start Gatus, check `/health` and the status API/dashboard,
then resume probe submissions. Retain the old binary/configuration, source
database, and final snapshot until the cutover has been accepted.

## Rollback

Stop the new Gatus process, restore the previous binary and SQLite configuration,
and restart the old deployment using the untouched SQLite database. Resume
probe submissions and confirm health. Keep the ClickHouse database for diagnosis.

Rollback restores history as of the SQLite cutover. Checks received only by
ClickHouse after the switch are not automatically copied back to SQLite; this
tool provides a one-way migration and does not claim a lossless reverse sync.

## Run integration tests

Point the test suite at a disposable ClickHouse HTTP server. Tests create and
drop their own uniquely named databases, so the test account needs database
creation/deletion privileges. Do not use the production server for these tests.

```sh
GATUS_TEST_CLICKHOUSE_URL=http://127.0.0.1:8123 \
  go test -race ./storage/... ./cmd/gatus-migrate-clickhouse
```

Optionally set `GATUS_MIGRATION_SQLITE` to an immutable snapshot to also run a
full migration rehearsal and compare endpoint histories and statistics against
SQLite. Without the ClickHouse test URL, these integration tests are skipped;
local configuration, HTTP error handling, and SQLite source tests still run.
