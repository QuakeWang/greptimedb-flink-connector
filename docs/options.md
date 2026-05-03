# Connector Options

This document lists the SQL options, defaults, and option constraints implemented by the current GreptimeDB Flink Connector source code. Keep README focused on the project overview and minimal usage; use this document as the detailed option reference.

## Basic Usage

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
  'tags' = 'host;region'
);
```

`connector`, `endpoints`, and `time-index` are required. If `table` is not set, the connector uses the Flink catalog table name.

## Connection Options

| Option | Required | Default | Scope | Description |
| --- | --- | --- | --- | --- |
| `connector` | Yes | none | all | Must be `greptimedb`. |
| `endpoints` | Yes | none | all | GreptimeDB gRPC endpoints. Use semicolons in SQL, for example `127.0.0.1:4001;127.0.0.2:4001`. Each endpoint must be `host:port`. |
| `database` | No | `public` | all | GreptimeDB database. Must not be blank or contain leading/trailing whitespace. |
| `table` | No | Flink table name | all | GreptimeDB table. Must not be blank or contain leading/trailing whitespace. |
| `username` | No | none | all | Authentication username. Must be configured together with `password`; blank values are treated as unset. |
| `password` | No | none | all | Authentication password. Must be configured together with `username`; blank values are treated as unset. |

## Schema Options

| Option | Required | Default | Scope | Description |
| --- | --- | --- | --- | --- |
| `time-index` | Yes | none | all | GreptimeDB time index column. It must reference a physical column whose type is non-null `TIMESTAMP` or `TIMESTAMP_LTZ`. |
| `tags` | No | empty | all | GreptimeDB tag columns. Use semicolons in SQL. Each tag must exist, must be unique, must not equal `time-index`, and must follow the Flink physical column order. |

Timestamp precision currently supports only `0`, `3`, `6`, and `9`, mapped to GreptimeDB second, millisecond, microsecond, and nanosecond timestamps. Other precisions are rejected during table validation.

When `auto-create-table=true` or `sink.changelog-mode=retract`, a declared Flink `PRIMARY KEY` must exactly match the tag order derived from the physical column order. If no primary key is declared, the connector does not require one. When `auto-create-table=false` and the sink is insert-only regular write, the connector does not require the primary key to match `tags` because the target table is user-managed.

## Write Mode Options

| Option | Required | Default | Scope | Description |
| --- | --- | --- | --- | --- |
| `sink.write-mode` | No | `regular` | all | Write mode. Supported values: `regular`, `bulk`. |
| `sink.changelog-mode` | No | `insert-only` | all | Accepted changelog mode. Supported values: `insert-only`, `retract`. `retract` is supported only in regular write mode. |
| `sink.parallelism` | No | unset | all | Sink operator parallelism. Must be greater than `0`; retract mode requires `1`. If unset, retract mode uses `1` automatically. |
| `batch.max-rows` | No | `1000` | all | Maximum rows per flush. Must be greater than `0`. |
| `flush.interval-ms` | No | `0` | all | Periodic flush interval. `0` disables periodic flushing; non-zero values must be greater than `0`. |

`regular` is the default and uses the GreptimeDB Java Ingester Regular Write API. `bulk` uses the Bulk Write API for large insert-only writes, but it requires an existing target table with an exactly matching schema.

## Regular Write Options

| Option | Required | Default | Scope | Description |
| --- | --- | --- | --- | --- |
| `auto-create-table` | No | `true` | regular | Controls target table auto-creation through SDK hints. Bulk write requires `false`. |
| `ttl` | No | none | regular | Table TTL passed through SDK hints. Must not be blank or contain commas. Not supported in bulk write. |
| `append-mode` | No | `false` | regular | Append mode passed through SDK hints. Cannot be used together with `merge-mode`; cannot be `true` when `sink.changelog-mode=retract`. |
| `merge-mode` | No | none | regular | Merge mode passed through SDK hints. Supported values: `last_row`, `last_non_null`. Cannot be used together with `append-mode=true`. |
| `write.max-retries` | No | `1` | regular | Maximum SDK regular write retries. Must be greater than or equal to `0`. |
| `write.max-in-flight-points` | No | `655360` | regular | Maximum SDK regular write in-flight points. Must be greater than `0`. |
| `write.limit-policy` | No | `abort-on-blocking-timeout` | regular | SDK write limiter policy. Supported values: `abort`, `blocking`, `blocking-timeout`, `abort-on-blocking-timeout`. |
| `write.limit-timeout-ms` | No | `3000` | regular | Limiter timeout. Must be greater than or equal to `0`. |
| `write.compression` | No | `none` | regular | Regular write RPC compression. Supported values: `none`, `gzip`, `zstd`. |
| `rpc.timeout-ms` | No | `60000` | regular | Timeout while waiting for the regular write RPC future. Must be greater than `0`. |
| `route.refresh-period-s` | No | `600` | all | SDK route table background refresh period in seconds. Must be greater than or equal to `0`; `0` disables background refresh. |
| `route.health-timeout-ms` | No | `1000` | all | SDK route health check timeout. Must be greater than `0`. |

`sink.changelog-mode=insert-only` accepts only `INSERT` RowKind. `sink.changelog-mode=retract` accepts `INSERT`, `UPDATE_AFTER`, `UPDATE_BEFORE`, and `DELETE`: `INSERT` and `UPDATE_AFTER` are written as GreptimeDB inserts, while `UPDATE_BEFORE` and `DELETE` are written as GreptimeDB deletes. Delete payloads contain only row key columns: `tags` plus `time-index`.

## Bulk Write Options

| Option | Required | Default | Scope | Description |
| --- | --- | --- | --- | --- |
| `sink.write-mode` | Yes | `regular` | bulk | Must be explicitly set to `bulk`. |
| `auto-create-table` | Yes | `true` | bulk | Must be explicitly set to `false` because Bulk Write API does not create tables. |
| `bulk.column-buffer-size` | No | `1024` | bulk | Initial row capacity for each Arrow column buffer. Must be greater than `0`. |
| `bulk.timeout-ms-per-message` | No | `60000` | bulk | Timeout for one bulk stream operation. Must be greater than `0`. |
| `bulk.max-requests-in-flight` | No | `8` | bulk | Maximum in-flight requests for the bulk stream. Must be greater than `0`. |
| `bulk.allocator-init-reservation-bytes` | No | `0` | bulk | Initial Arrow allocator reservation. Must be greater than or equal to `0`. |
| `bulk.allocator-max-allocation-bytes` | No | `1073741824` | bulk | Maximum Arrow allocator allocation. Must be greater than `0` and greater than or equal to `bulk.allocator-init-reservation-bytes`. |

Bulk write supports only `INSERT` RowKind. It does not support `UPDATE_BEFORE`, `UPDATE_AFTER`, `DELETE`, or `sink.changelog-mode=retract`. The target table must already exist, and the GreptimeDB table schema must match the Flink physical schema.

Bulk write keeps one GreptimeDB bulk stream open between completion boundaries. `batch.max-rows` and `flush.interval-ms` flush the current buffer by calling `writeNext`, but they do not complete the stream. The connector calls `completed` on Flink checkpoint flush, end-of-input, and writer close. For streaming jobs, enable checkpoints when using bulk mode; without checkpoints, completion happens only when the bounded input ends or the writer closes, so batch or periodic flushes alone are not recovery boundaries.

Bulk write does not use Regular Write hints or compression. The following Regular Write options are rejected when explicitly configured with `sink.write-mode=bulk`:

- `write.max-retries`
- `write.max-in-flight-points`
- `write.limit-policy`
- `write.limit-timeout-ms`
- `write.compression`
- `rpc.timeout-ms`

## Type Support

| Flink logical type | GreptimeDB type |
| --- | --- |
| `BOOLEAN` | `Bool` |
| `TINYINT` | `Int8` |
| `SMALLINT` | `Int16` |
| `INTEGER` | `Int32` |
| `BIGINT` | `Int64` |
| `FLOAT` | `Float32` |
| `DOUBLE` | `Float64` |
| `CHAR`, `VARCHAR` | `String` |
| `BINARY`, `VARBINARY` | `Binary` |
| `DATE` | `Date` |
| `DECIMAL(p, s)` | `Decimal128`, `p <= 38` |
| `TIMESTAMP(0/3/6/9)` | second/millisecond/microsecond/nanosecond timestamp |
| `TIMESTAMP_LTZ(0/3/6/9)` | second/millisecond/microsecond/nanosecond timestamp |

Flink logical types not listed above are rejected.

## Option Constraints

| Scenario | Constraint |
| --- | --- |
| `append-mode=true` | Cannot be configured together with `merge-mode`. |
| `sink.changelog-mode=retract` | Supported only in `regular`; `sink.parallelism` must be `1`; `append-mode=true` is rejected; a declared `PRIMARY KEY` must match the physical `tags` order. |
| `sink.write-mode=bulk` | Requires `auto-create-table=false`; rejects `ttl`, `merge-mode`, `append-mode=true`, `sink.changelog-mode=retract`, and explicitly configured Regular Write options. |
| `auto-create-table=true` | A declared `PRIMARY KEY` must match the physical `tags` order. |
| `time-index` | Must be non-nullable, and timestamp precision must be one of `0`, `3`, `6`, or `9`. |
