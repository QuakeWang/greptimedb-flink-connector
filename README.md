# GreptimeDB Flink Connector

Flink SQL sink connector for writing `RowData` into GreptimeDB through the GreptimeDB Java Ingester.

## Status

This is a personal, unofficial project in active development. It currently provides a sink connector only.

- Target runtime: Flink `1.20.3`, Java `11`, GreptimeDB Java Ingester `0.15.0`
- Supported: Flink SQL Table sink, Regular insert/retract writes, Bulk insert-only writes
- Delivery: at-least-once

Bulk write uses a long-lived GreptimeDB bulk stream. `batch.max-rows` and `flush.interval-ms` send buffered rows with `writeNext`; the stream is completed on Flink checkpoint flush, end-of-input, or writer close. For streaming jobs that use `sink.write-mode=bulk`, enable checkpoints so the connector has regular completion boundaries. Without checkpoints, completion happens only when the bounded input ends or the writer closes.

Not implemented:

- Source connector
- Lookup connector
- Bulk update/delete/changelog writes
- Exactly-once delivery

## Documentation

- [QuickStart](docs/quickstart.md): build the connector, run local writes, and understand the supported sink modes.
- [Connector Options](docs/options.md): SQL options, defaults, and mode-specific constraints.

## Build

```bash
mvn package
```

Use the shaded jar from `target/*-shaded.jar` when deploying to Flink. The thin jar does not include the GreptimeDB Java Ingester runtime dependencies.

## Example

```sql
CREATE TABLE metrics_sink (
  host STRING,
  region STRING,
  cpu DOUBLE,
  ts TIMESTAMP(3) NOT NULL,
  PRIMARY KEY (host, region) NOT ENFORCED
) WITH (
  'connector' = 'greptimedb',
  'endpoints' = '127.0.0.1:4001',
  'database' = 'public',
  'table' = 'metrics',
  'time-index' = 'ts',
  'tags' = 'host;region',
  'batch.max-rows' = '1000',
  'flush.interval-ms' = '0'
);
```

`time-index` must reference a non-null `TIMESTAMP` or `TIMESTAMP_LTZ` column. `tags` define GreptimeDB tag columns and must follow the physical column order in the Flink table schema.
