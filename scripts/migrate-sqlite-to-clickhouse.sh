#!/usr/bin/env bash
set -euo pipefail

# No process control or configuration edits: the CLI defaults to a read-only
# dry run. Pass --apply explicitly when the offline snapshot is ready.
repo_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
if [[ -n "${GATUS_MIGRATE_BIN:-}" ]]; then
  exec "$GATUS_MIGRATE_BIN" "$@"
fi
if [[ -x "$repo_dir/bin/gatus-migrate-clickhouse" ]]; then
  exec "$repo_dir/bin/gatus-migrate-clickhouse" "$@"
fi
cd -- "$repo_dir"
exec go run ./cmd/gatus-migrate-clickhouse "$@"
