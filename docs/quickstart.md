# GreptimeDB Flink Connector QuickStart

## Prerequisites

- Java `11`
- Maven
- Flink `1.20.x`
- `FLINK_HOME` points to the Flink installation
- `curl`
- Docker, only when starting GreptimeDB locally

## Build

```bash
mvn package -DskipTests
export GREPTIME_FLINK_CONNECTOR_JAR="$(find target -maxdepth 1 -name '*-shaded.jar' | sort | tail -n 1)"
test -f "$GREPTIME_FLINK_CONNECTOR_JAR"
: "${FLINK_HOME:?FLINK_HOME must point to Flink 1.20.x}"
test -x "$FLINK_HOME/bin/sql-client.sh"
```

Use the shaded jar with Flink SQL Client. The thin jar does not include GreptimeDB Java Ingester runtime dependencies.

## Start Flink

```bash
"$FLINK_HOME/bin/start-cluster.sh"
curl -sf http://127.0.0.1:8081/overview >/dev/null
```

## Start GreptimeDB

```bash
export GREPTIME_HTTP_URL="${GREPTIME_HTTP_URL:-http://127.0.0.1:4000}"
export GREPTIME_GRPC_ENDPOINT="${GREPTIME_GRPC_ENDPOINT:-127.0.0.1:4001}"
export GREPTIME_QUICKSTART_CONTAINER="${GREPTIME_QUICKSTART_CONTAINER:-greptimedb-flink-quickstart}"
export GREPTIME_QUICKSTART_IMAGE="${GREPTIME_QUICKSTART_IMAGE:-greptime/greptimedb:v1.0.1}"
export GREPTIME_QUICKSTART_DATA_DIR="${GREPTIME_QUICKSTART_DATA_DIR:-$(pwd)/greptimedb_data}"
```

Skip the Docker block if `GREPTIME_HTTP_URL` and `GREPTIME_GRPC_ENDPOINT` already point to a running GreptimeDB instance, or if local ports `4000-4003` are already in use.

```bash
docker rm -f "$GREPTIME_QUICKSTART_CONTAINER" 2>/dev/null || true
mkdir -p "$GREPTIME_QUICKSTART_DATA_DIR"
docker run -d --name "$GREPTIME_QUICKSTART_CONTAINER" \
  --rm \
  -p 127.0.0.1:4000-4003:4000-4003 \
  -v "$GREPTIME_QUICKSTART_DATA_DIR:/greptimedb_data" \
  "$GREPTIME_QUICKSTART_IMAGE" \
  standalone start \
  --http-addr 0.0.0.0:4000 \
  --rpc-bind-addr 0.0.0.0:4001 \
  --mysql-addr 0.0.0.0:4002 \
  --postgres-addr 0.0.0.0:4003

for i in $(seq 1 60); do
  if curl -sf "$GREPTIME_HTTP_URL/health" >/dev/null; then
    break
  fi
  sleep 1
done
curl -sf "$GREPTIME_HTTP_URL/health" >/dev/null
```

The Docker command follows the [GreptimeDB standalone Docker deployment](https://docs.greptime.cn/getting-started/installation/greptimedb-standalone/#docker): expose ports `4000-4003`, bind services to `0.0.0.0`, and persist data under `greptimedb_data/`. For Docker versions earlier than `23.0`, add `--security-opt seccomp=unconfined` if startup fails with a permission error.

## Helpers

`sql-client.sh -f` submits the bounded insert job and exits. `wait_for_rows` polls GreptimeDB before reading the result.

```bash
greptime_sql() {
  local format="${2:-table}"

  curl -sS -X POST "$GREPTIME_HTTP_URL/v1/sql" \
    -H 'Content-Type: application/x-www-form-urlencoded' \
    --data-urlencode "format=$format" \
    --data-urlencode "sql=$1"
}

run_flink_sql() {
  "$FLINK_HOME/bin/sql-client.sh" embedded \
    -D execution.runtime-mode=batch \
    -D parallelism.default=1 \
    -j "$GREPTIME_FLINK_CONNECTOR_JAR" \
    -f "$1"
}

wait_for_rows() {
  local table="$1"
  local expected_rows="$2"
  local result

  for i in $(seq 1 60); do
    result="$(greptime_sql "SELECT count(*) AS rows FROM $table" csv | tr -d '\r')"
    if [ "$result" = "$expected_rows" ]; then
      return 0
    fi
    sleep 1
  done

  printf '%s\n' "$result"
  return 1
}
```

## Regular Write

Regular write is the default. Required options are `connector`, `endpoints`, and `time-index`. `tags` declares GreptimeDB tag columns and must follow the Flink physical column order.

```bash
greptime_sql 'DROP TABLE IF EXISTS metrics_qs_regular'

cat >/tmp/greptime-flink-regular.sql <<SQL
CREATE TEMPORARY TABLE metrics_qs_regular_sink (
  host STRING,
  region STRING,
  cpu DOUBLE,
  ts TIMESTAMP(3) NOT NULL,
  PRIMARY KEY (host, region) NOT ENFORCED
) WITH (
  'connector' = 'greptimedb',
  'endpoints' = '$GREPTIME_GRPC_ENDPOINT',
  'database' = 'public',
  'table' = 'metrics_qs_regular',
  'time-index' = 'ts',
  'tags' = 'host;region',
  'batch.max-rows' = '2'
);

INSERT INTO metrics_qs_regular_sink VALUES
  ('host-1', 'eu-west', 0.25, TIMESTAMP '2026-04-28 15:00:00.000'),
  ('host-2', 'ap-south', 0.50, TIMESTAMP '2026-04-28 15:01:00.000'),
  ('host-3', 'us-east', 0.75, TIMESTAMP '2026-04-28 15:02:00.000');
SQL

run_flink_sql /tmp/greptime-flink-regular.sql
wait_for_rows metrics_qs_regular 3
greptime_sql 'SELECT host, region, cpu, ts FROM metrics_qs_regular ORDER BY host'
```

## Bulk Write

Bulk write requires `sink.write-mode=bulk`, `auto-create-table=false`, and a pre-created GreptimeDB table whose schema matches the Flink physical schema. It supports insert-only writes.

```bash
greptime_sql 'DROP TABLE IF EXISTS metrics_qs_bulk'
greptime_sql 'CREATE TABLE metrics_qs_bulk (
  host STRING,
  `region` STRING,
  cpu DOUBLE,
  ts TIMESTAMP(3) TIME INDEX,
  PRIMARY KEY(host, `region`)
)'

cat >/tmp/greptime-flink-bulk.sql <<SQL
CREATE TEMPORARY TABLE metrics_qs_bulk_sink (
  host STRING,
  region STRING,
  cpu DOUBLE,
  ts TIMESTAMP(3) NOT NULL,
  PRIMARY KEY (host, region) NOT ENFORCED
) WITH (
  'connector' = 'greptimedb',
  'endpoints' = '$GREPTIME_GRPC_ENDPOINT',
  'database' = 'public',
  'table' = 'metrics_qs_bulk',
  'time-index' = 'ts',
  'tags' = 'host;region',
  'sink.write-mode' = 'bulk',
  'auto-create-table' = 'false',
  'batch.max-rows' = '2'
);

INSERT INTO metrics_qs_bulk_sink VALUES
  ('bulk-host-1', 'eu-west', 1.25, TIMESTAMP '2026-04-28 16:00:00.000'),
  ('bulk-host-2', 'ap-south', 1.50, TIMESTAMP '2026-04-28 16:01:00.000'),
  ('bulk-host-3', 'us-east', 1.75, TIMESTAMP '2026-04-28 16:02:00.000');
SQL

run_flink_sql /tmp/greptime-flink-bulk.sql
wait_for_rows metrics_qs_bulk 3
greptime_sql 'SELECT host, region, cpu, ts FROM metrics_qs_bulk ORDER BY host'
```

## More Options

See [Connector Options](options.md) for changelog/retract semantics, type mappings, and option constraints.

## Cleanup

```bash
greptime_sql 'DROP TABLE IF EXISTS metrics_qs_regular'
greptime_sql 'DROP TABLE IF EXISTS metrics_qs_bulk'
```

If this guide started GreptimeDB with Docker:

```bash
docker rm -f "$GREPTIME_QUICKSTART_CONTAINER"
```

Remove local GreptimeDB data only when a clean state is required:

```bash
rm -rf "$GREPTIME_QUICKSTART_DATA_DIR"
```

## Troubleshooting

| Symptom | Check |
| --- | --- |
| `FLINK_HOME must point to Flink 1.20.x` | Set `FLINK_HOME` to your Flink `1.20.x` installation. |
| `No such file or directory: .../sql-client.sh` | Check `FLINK_HOME` and make sure Flink is installed locally. |
| `Connection refused: localhost/127.0.0.1:8081` | Start the local Flink cluster with `"$FLINK_HOME/bin/start-cluster.sh"`. |
| `Could not find any factory for identifier 'greptimedb'` | Make sure SQL Client uses `-j "$GREPTIME_FLINK_CONNECTOR_JAR"` and the jar is the shaded jar. |
| `wait_for_rows` times out | Check the failed job in Flink Web UI or TaskManager logs first. |
| `sink.write-mode=bulk requires auto-create-table=false` | Add `'auto-create-table' = 'false'` to the Bulk DDL. |
| `Table not found` in Bulk write | Pre-create the GreptimeDB table before running the Bulk insert. |
