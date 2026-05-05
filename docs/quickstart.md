# GreptimeDB Flink Connector QuickStart

## Prerequisites

- Java `11`
- Maven
- Flink `1.20.x`
- `FLINK_HOME` points to the Flink installation
- `curl`
- MySQL Connector/J, only when running source reads. The build commands below can download it when it is not already available.
- Docker, only when starting GreptimeDB locally

## Build

```bash
mvn package -DskipTests
export GREPTIME_FLINK_CONNECTOR_JAR="$(find target -maxdepth 1 -name '*-shaded.jar' | sort | tail -n 1)"
test -f "$GREPTIME_FLINK_CONNECTOR_JAR"
: "${FLINK_HOME:?FLINK_HOME must point to Flink 1.20.x}"
test -x "$FLINK_HOME/bin/sql-client.sh"
export MYSQL_CONNECTOR_VERSION="${MYSQL_CONNECTOR_VERSION:-8.4.0}"
export MYSQL_CONNECTOR_JAR="${MYSQL_CONNECTOR_JAR:-$(find "$FLINK_HOME/lib" -maxdepth 1 -name 'mysql-connector-j-*.jar' | sort | tail -n 1)}"
if [ -z "$MYSQL_CONNECTOR_JAR" ]; then
  mkdir -p target/quickstart-lib
  export MYSQL_CONNECTOR_JAR="target/quickstart-lib/mysql-connector-j-${MYSQL_CONNECTOR_VERSION}.jar"
  curl -L --fail -o "$MYSQL_CONNECTOR_JAR" \
    "https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/${MYSQL_CONNECTOR_VERSION}/mysql-connector-j-${MYSQL_CONNECTOR_VERSION}.jar"
fi
test -f "$MYSQL_CONNECTOR_JAR"
```

Use the shaded jar with Flink SQL Client. The thin jar does not include GreptimeDB Java Ingester runtime dependencies.
Source reads also need MySQL Connector/J because the source scans GreptimeDB through the MySQL-compatible JDBC endpoint. The commands below pass `MYSQL_CONNECTOR_JAR` to SQL Client with `-j`; the connector shaded jar does not bundle it.

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

## GreptimeDB SQL Helper

The helper below runs SQL directly against GreptimeDB over HTTP. It is used only to reset the quickstart tables and to pre-create the Bulk Write table.

```bash
greptime_sql() {
  local format="${2:-table}"

  curl -sS -X POST "$GREPTIME_HTTP_URL/v1/sql" \
    -H 'Content-Type: application/x-www-form-urlencoded' \
    --data-urlencode "format=$format" \
    --data-urlencode "sql=$1"
}
```

Reset the quickstart tables before running the examples:

```bash
greptime_sql 'DROP TABLE IF EXISTS metrics_qs_regular'
greptime_sql 'DROP TABLE IF EXISTS metrics_qs_bulk'
```

## Start Flink SQL Client

Start SQL Client once, then paste the SQL statements from the sections below. The examples assume GreptimeDB is reachable at local gRPC endpoint `127.0.0.1:4001` and MySQL endpoint `127.0.0.1:4002`; replace these literals when using another GreptimeDB instance.

```bash
"$FLINK_HOME/bin/sql-client.sh" embedded \
  -D execution.runtime-mode=batch \
  -D table.dml-sync=true \
  -D parallelism.default=1 \
  -j "$GREPTIME_FLINK_CONNECTOR_JAR" \
  -j "$MYSQL_CONNECTOR_JAR"
```

## Regular Write

Regular write is the default. Required options are `connector`, `endpoints`, and `time-index`. `tags` declares GreptimeDB tag columns and must follow the Flink physical column order.

Paste this into SQL Client:

```sql
CREATE TEMPORARY TABLE metrics_qs_regular_sink (
  host STRING,
  region STRING,
  cpu DOUBLE,
  ts TIMESTAMP(3) NOT NULL,
  PRIMARY KEY (host, region) NOT ENFORCED
) WITH (
  'connector' = 'greptimedb',
  'endpoints' = '127.0.0.1:4001',
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
```

Wait for the `INSERT` statement to finish, then read the rows back with the bounded source:

```sql
CREATE TEMPORARY TABLE metrics_qs_regular_source (
  host STRING,
  region STRING,
  cpu DOUBLE,
  ts TIMESTAMP(3)
) WITH (
  'connector' = 'greptimedb',
  'query.jdbc-url' = 'jdbc:mysql://127.0.0.1:4002/public?useSSL=false&allowPublicKeyRetrieval=true',
  'database' = 'public',
  'table' = 'metrics_qs_regular',
  'query.fetch-size' = '2'
);

SELECT host, region, cpu, ts FROM metrics_qs_regular_source ORDER BY host;
```

## Bulk Write

Bulk write requires `sink.write-mode=bulk`, `auto-create-table=false`, and a pre-created GreptimeDB table whose schema matches the Flink physical schema. It supports insert-only writes.

Run this in the shell before pasting the Bulk Write SQL into SQL Client:

```bash
greptime_sql 'CREATE TABLE metrics_qs_bulk (
  host STRING,
  `region` STRING,
  cpu DOUBLE,
  ts TIMESTAMP(3) TIME INDEX,
  PRIMARY KEY(host, `region`)
)'
```

Paste this into SQL Client:

```sql
CREATE TEMPORARY TABLE metrics_qs_bulk_sink (
  host STRING,
  region STRING,
  cpu DOUBLE,
  ts TIMESTAMP(3) NOT NULL,
  PRIMARY KEY (host, region) NOT ENFORCED
) WITH (
  'connector' = 'greptimedb',
  'endpoints' = '127.0.0.1:4001',
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
```

Wait for the `INSERT` statement to finish, then read the rows back with the bounded source:

```sql
CREATE TEMPORARY TABLE metrics_qs_bulk_source (
  host STRING,
  region STRING,
  cpu DOUBLE,
  ts TIMESTAMP(3)
) WITH (
  'connector' = 'greptimedb',
  'query.jdbc-url' = 'jdbc:mysql://127.0.0.1:4002/public?useSSL=false&allowPublicKeyRetrieval=true',
  'database' = 'public',
  'table' = 'metrics_qs_bulk',
  'query.fetch-size' = '2'
);

SELECT host, region, cpu, ts FROM metrics_qs_bulk_source ORDER BY host;
```

## Bounded Source

The source is a bounded batch scan backed by GreptimeDB's MySQL-compatible JDBC endpoint. It supports projection pushdown, projection reorder, empty projection for `COUNT(*)`, and best-effort `LIMIT` pushdown. Filters are evaluated by Flink.

Use the source DDL pattern below for existing GreptimeDB tables:

```sql
CREATE TEMPORARY TABLE metrics_source (
  host STRING,
  region STRING,
  cpu DOUBLE,
  ts TIMESTAMP(3)
) WITH (
  'connector' = 'greptimedb',
  'query.jdbc-url' = 'jdbc:mysql://127.0.0.1:4002/public?useSSL=false&allowPublicKeyRetrieval=true',
  'database' = 'public',
  'table' = 'metrics_qs_regular',
  'query.fetch-size' = '2'
);

SELECT cpu, host, ts
FROM metrics_source
WHERE region = 'eu-west'
ORDER BY host;

SELECT COUNT(*) FROM metrics_source;
```

## More Options

See [Connector Options](options.md) for source limitations, changelog/retract semantics, type mappings, and option constraints.

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
| `No suitable driver found for jdbc:mysql://...` | Put MySQL Connector/J in `$FLINK_HOME/lib/` and restart Flink, or start SQL Client with the driver jar through `-j`. |
| `Result retrieval cancelled` after a source `SELECT` | Make sure the preceding `INSERT` completed. Start SQL Client with `-D table.dml-sync=true` for this guide, then rerun the bounded source query. |
| `sink.write-mode=bulk requires auto-create-table=false` | Add `'auto-create-table' = 'false'` to the Bulk DDL. |
| `Table not found` in Bulk write | Pre-create the GreptimeDB table before running the Bulk insert. |
