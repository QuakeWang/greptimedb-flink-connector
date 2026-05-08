package io.greptime.flink.table;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import io.greptime.flink.cfg.GreptimeBulkWriteConfig;
import io.greptime.flink.cfg.GreptimeChangelogMode;
import io.greptime.flink.cfg.GreptimeSinkConfig;
import io.greptime.flink.cfg.GreptimeWriteMode;
import io.greptime.flink.query.GreptimeQueryConfig;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;

class GreptimeConnectorOptionsTest {
    @Test
    void createsRegularSinkConfigFromFactoryOptions() {
        Map<String, String> options = baseOptionMap();
        options.put(GreptimeConnectorOptions.TABLE.key(), "factory_metrics");
        options.put(GreptimeConnectorOptions.BATCH_MAX_ROWS.key(), "128");
        options.put(GreptimeConnectorOptions.FLUSH_INTERVAL_MS.key(), "500");
        options.put(GreptimeConnectorOptions.WRITE_MAX_RETRIES.key(), "3");
        options.put(GreptimeConnectorOptions.WRITE_MAX_IN_FLIGHT_POINTS.key(), "1024");
        options.put(
                GreptimeConnectorOptions.WRITE_LIMIT_POLICY.key(),
                GreptimeSinkConfig.WriteLimitPolicy.BLOCKING_TIMEOUT.optionValue());
        options.put(GreptimeConnectorOptions.WRITE_LIMIT_TIMEOUT_MS.key(), "2500");
        options.put(
                GreptimeConnectorOptions.WRITE_COMPRESSION.key(),
                GreptimeSinkConfig.WriteCompression.ZSTD.optionValue());
        options.put(GreptimeConnectorOptions.RPC_TIMEOUT_MS.key(), "30000");
        options.put(GreptimeConnectorOptions.ROUTE_REFRESH_PERIOD_S.key(), "60");
        options.put(GreptimeConnectorOptions.ROUTE_HEALTH_TIMEOUT_MS.key(), "500");

        GreptimeDynamicTableSink sink = createSink(options, resolvedSchemaWithoutPrimaryKey(), "identifier_metrics");
        GreptimeSinkConfig sinkConfig = sink.getSinkConfig();

        assertEquals("factory_metrics", sink.getTableSchema().getTableName());
        assertEquals(128, sinkConfig.getBatchMaxRows());
        assertEquals(500L, sinkConfig.getFlushIntervalMs());
        assertEquals(3, sinkConfig.getWriteMaxRetries());
        assertEquals(1024, sinkConfig.getWriteMaxInFlightPoints());
        assertEquals(GreptimeSinkConfig.WriteLimitPolicy.BLOCKING_TIMEOUT, sinkConfig.getWriteLimitPolicy());
        assertEquals(2500L, sinkConfig.getWriteLimitTimeoutMs());
        assertEquals(GreptimeSinkConfig.WriteCompression.ZSTD, sinkConfig.getWriteCompression());
        assertEquals(30000, sinkConfig.getRpcTimeoutMs());
        assertEquals(60L, sinkConfig.getRouteRefreshPeriodSeconds());
        assertEquals(500L, sinkConfig.getRouteHealthTimeoutMs());
        assertEquals(GreptimeWriteMode.REGULAR, sinkConfig.getWriteMode());
        assertEquals(GreptimeChangelogMode.INSERT_ONLY, sinkConfig.getChangelogMode());
        assertEquals(null, sink.getSinkParallelism());
    }

    @Test
    void createsBulkSinkConfigFromFactoryOptions() {
        Map<String, String> options = baseOptionMap();
        options.put(GreptimeConnectorOptions.SINK_WRITE_MODE.key(), "bulk");
        options.put(GreptimeConnectorOptions.AUTO_CREATE_TABLE.key(), "false");
        options.put(GreptimeConnectorOptions.BULK_COLUMN_BUFFER_SIZE.key(), "2048");
        options.put(GreptimeConnectorOptions.BULK_TIMEOUT_MS_PER_MESSAGE.key(), "30000");
        options.put(GreptimeConnectorOptions.BULK_MAX_REQUESTS_IN_FLIGHT.key(), "4");
        options.put(GreptimeConnectorOptions.BULK_ALLOCATOR_INIT_RESERVATION_BYTES.key(), "1024");
        options.put(GreptimeConnectorOptions.BULK_ALLOCATOR_MAX_ALLOCATION_BYTES.key(), "65536");

        GreptimeDynamicTableSink sink =
                createSink(options, resolvedSchemaWithPrimaryKey(List.of("host")), "bulk_metrics");
        GreptimeSinkConfig sinkConfig = sink.getSinkConfig();
        GreptimeBulkWriteConfig bulkWriteConfig = sinkConfig.getBulkWriteConfig();

        assertEquals("bulk_metrics", sink.getTableSchema().getTableName());
        assertEquals(GreptimeWriteMode.BULK, sinkConfig.getWriteMode());
        assertEquals(GreptimeChangelogMode.INSERT_ONLY, sinkConfig.getChangelogMode());
        assertEquals(2048, bulkWriteConfig.getColumnBufferSize());
        assertEquals(30000L, bulkWriteConfig.getTimeoutMsPerMessage());
        assertEquals(4, bulkWriteConfig.getMaxRequestsInFlight());
        assertEquals(1024L, bulkWriteConfig.getAllocatorInitReservationBytes());
        assertEquals(65536L, bulkWriteConfig.getAllocatorMaxAllocationBytes());
    }

    @Test
    void createsBulkPreflightConfigFromFactoryOptions() {
        Map<String, String> options = bulkPreflightOptionMap();
        options.put(GreptimeConnectorOptions.QUERY_JDBC_URL.key(), "jdbc:mysql://127.0.0.1:4002/public?useSSL=false");
        options.put(GreptimeConnectorOptions.QUERY_CONNECT_TIMEOUT_MS.key(), "123");
        options.put(GreptimeConnectorOptions.QUERY_SOCKET_TIMEOUT_MS.key(), "456");
        options.put(GreptimeConnectorOptions.QUERY_FETCH_SIZE.key(), "abc");

        GreptimeDynamicTableSink sink =
                createSink(options, resolvedSchemaWithPrimaryKey(List.of("host")), "bulk_metrics");

        assertTrue(sink.getPreflightConfig().isEnabled());
        assertEquals(
                "jdbc:mysql://127.0.0.1:4002/public?useSSL=false",
                sink.getPreflightConfig().getQueryConfig().orElseThrow().getJdbcUrl());
        assertEquals(
                123, sink.getPreflightConfig().getQueryConfig().orElseThrow().getConnectTimeoutMs());
        assertEquals(
                456, sink.getPreflightConfig().getQueryConfig().orElseThrow().getSocketTimeoutMs());
        assertEquals(
                GreptimeQueryConfig.DEFAULT_FETCH_SIZE,
                sink.getPreflightConfig().getQueryConfig().orElseThrow().getFetchSize());
        assertEquals(sink, sink.copy());
    }

    @Test
    void createsBulkPreflightConfigFromEnrichmentQueryOptions() {
        Map<String, String> options = bulkPreflightOptionMap();
        Map<String, String> enrichmentOptions = Map.of(
                GreptimeConnectorOptions.QUERY_JDBC_URL.key(),
                "jdbc:mysql://10.0.0.1:4002/public?useSSL=false",
                GreptimeConnectorOptions.QUERY_CONNECT_TIMEOUT_MS.key(),
                "111",
                GreptimeConnectorOptions.QUERY_SOCKET_TIMEOUT_MS.key(),
                "222");

        GreptimeDynamicTableSink sink =
                createSink(options, enrichmentOptions, resolvedSchemaWithPrimaryKey(List.of("host")), "bulk_metrics");

        assertTrue(sink.getPreflightConfig().isEnabled());
        assertEquals(
                "jdbc:mysql://10.0.0.1:4002/public?useSSL=false",
                sink.getPreflightConfig().getQueryConfig().orElseThrow().getJdbcUrl());
        assertEquals(
                111, sink.getPreflightConfig().getQueryConfig().orElseThrow().getConnectTimeoutMs());
        assertEquals(
                222, sink.getPreflightConfig().getQueryConfig().orElseThrow().getSocketTimeoutMs());
    }

    @Test
    void preflightEnabledBulkSinkRequiresJdbcUrl() {
        Map<String, String> options = bulkPreflightOptionMap();

        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> createSink(options, resolvedSchemaWithPrimaryKey(List.of("host")), "bulk_metrics"));

        assertEquals("`query.jdbc-url` is required when `preflight.enabled=true`", error.getMessage());
    }

    @Test
    void preflightEnabledBulkSinkValidatesQueryTimeouts() {
        Map<String, String> options = bulkPreflightOptionMap();
        options.put(GreptimeConnectorOptions.QUERY_JDBC_URL.key(), "jdbc:mysql://127.0.0.1:4002/public?useSSL=false");
        options.put(GreptimeConnectorOptions.QUERY_CONNECT_TIMEOUT_MS.key(), "-1");

        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> createSink(options, resolvedSchemaWithPrimaryKey(List.of("host")), "bulk_metrics"));

        assertEquals("`query.connect-timeout-ms` must be greater than 0", error.getMessage());
    }

    @Test
    void preflightDisabledSinkIgnoresInvalidQueryOptions() {
        Map<String, String> options = baseOptionMap();
        options.put(GreptimeConnectorOptions.QUERY_JDBC_URL.key(), "jdbc:postgresql://127.0.0.1:4003/public");
        options.put(GreptimeConnectorOptions.QUERY_CONNECT_TIMEOUT_MS.key(), "abc");
        options.put(GreptimeConnectorOptions.QUERY_FETCH_SIZE.key(), "abc");

        GreptimeDynamicTableSink sink = createSink(options, resolvedSchemaWithoutPrimaryKey(), "metrics");

        assertFalse(sink.getPreflightConfig().isEnabled());
    }

    @Test
    void preflightEnabledRegularAndRetractSinksFailUnsupportedMode() {
        Map<String, String> regularOptions = baseOptionMap();
        regularOptions.put(GreptimeConnectorOptions.PREFLIGHT_ENABLED.key(), "true");
        IllegalArgumentException regularError = assertThrows(
                IllegalArgumentException.class,
                () -> createSink(regularOptions, resolvedSchemaWithPrimaryKey(List.of("host")), "regular_metrics"));

        assertTrue(regularError.getMessage().contains("unsupported-mode"));
        assertTrue(regularError.getMessage().contains("mode=regular"));

        Map<String, String> retractOptions = baseOptionMap();
        retractOptions.put(GreptimeConnectorOptions.PREFLIGHT_ENABLED.key(), "true");
        retractOptions.put(GreptimeConnectorOptions.SINK_CHANGELOG_MODE.key(), "retract");
        IllegalArgumentException retractError = assertThrows(
                IllegalArgumentException.class,
                () -> createSink(retractOptions, resolvedSchemaWithPrimaryKey(List.of("host")), "retract_metrics"));

        assertTrue(retractError.getMessage().contains("unsupported-mode"));
        assertTrue(retractError.getMessage().contains("mode=retract"));
    }

    @Test
    void rejectsDuplicateTagColumns() {
        Configuration options = baseOptions(List.of("host", "host"));

        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> GreptimeConnectorOptions.validate(options, resolvedSchemaWithoutPrimaryKey()));

        assertEquals("Duplicate tag column: host", error.getMessage());
    }

    @Test
    void rejectsPrimaryKeyAndTagMismatch() {
        Configuration options = baseOptions(List.of("host"));

        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> GreptimeConnectorOptions.validate(
                        options, resolvedSchemaWithPrimaryKey(List.of("host", "region"))));

        assertEquals(
                "`PRIMARY KEY` must match the Greptime tag order derived from physical columns, PRIMARY KEY=[host, region], tags=[host], physicalTagOrder=[host]",
                error.getMessage());
    }

    @Test
    void acceptsPrimaryKeyAndTagMismatchWhenAutoCreateTableIsDisabled() {
        Configuration options = baseOptions(List.of("host"));
        options.set(GreptimeConnectorOptions.AUTO_CREATE_TABLE, false);

        assertDoesNotThrow(() ->
                GreptimeConnectorOptions.validate(options, resolvedSchemaWithPrimaryKey(List.of("host", "region"))));
    }

    @Test
    void acceptsMatchingPrimaryKeyAndTags() {
        Configuration options = baseOptions(List.of("host", "region"));

        assertDoesNotThrow(() ->
                GreptimeConnectorOptions.validate(options, resolvedSchemaWithPrimaryKey(List.of("host", "region"))));
    }

    @Test
    void rejectsUsernameWithoutPassword() {
        Configuration options = baseOptions(List.of("host"));
        options.set(GreptimeConnectorOptions.USERNAME, "greptime");

        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> GreptimeConnectorOptions.validate(options, resolvedSchemaWithoutPrimaryKey()));

        assertEquals("`username` and `password` must be configured together", error.getMessage());
    }

    @Test
    void rejectsPasswordWithoutUsername() {
        Configuration options = baseOptions(List.of("host"));
        options.set(GreptimeConnectorOptions.PASSWORD, "secret");

        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> GreptimeConnectorOptions.validate(options, resolvedSchemaWithoutPrimaryKey()));

        assertEquals("`username` and `password` must be configured together", error.getMessage());
    }

    @Test
    void rejectsBlankDatabaseAndTableOptions() {
        Configuration blankDatabase = baseOptions(List.of("host"));
        blankDatabase.set(GreptimeConnectorOptions.DATABASE, " ");
        assertEquals(
                "`database` must not be blank",
                assertThrows(
                                IllegalArgumentException.class,
                                () -> GreptimeConnectorOptions.validate(
                                        blankDatabase, resolvedSchemaWithoutPrimaryKey()))
                        .getMessage());

        Configuration blankTable = baseOptions(List.of("host"));
        blankTable.set(GreptimeConnectorOptions.TABLE, "");
        assertEquals(
                "`table` must not be blank",
                assertThrows(
                                IllegalArgumentException.class,
                                () -> GreptimeConnectorOptions.validate(blankTable, resolvedSchemaWithoutPrimaryKey()))
                        .getMessage());
    }

    @Test
    void rejectsIdentifierOptionsWithLeadingOrTrailingWhitespace() {
        Configuration databaseWithWhitespace = baseOptions(List.of("host"));
        databaseWithWhitespace.set(GreptimeConnectorOptions.DATABASE, " public ");
        assertEquals(
                "`database` must not have leading or trailing whitespace",
                assertThrows(
                                IllegalArgumentException.class,
                                () -> GreptimeConnectorOptions.validate(
                                        databaseWithWhitespace, resolvedSchemaWithoutPrimaryKey()))
                        .getMessage());

        Configuration tableWithWhitespace = baseOptions(List.of("host"));
        tableWithWhitespace.set(GreptimeConnectorOptions.TABLE, " metrics ");
        assertEquals(
                "`table` must not have leading or trailing whitespace",
                assertThrows(
                                IllegalArgumentException.class,
                                () -> GreptimeConnectorOptions.validate(
                                        tableWithWhitespace, resolvedSchemaWithoutPrimaryKey()))
                        .getMessage());
    }

    @Test
    void rejectsInvalidHintOptions() {
        Configuration blankTtl = baseOptions(List.of("host"));
        blankTtl.set(GreptimeConnectorOptions.TTL, " ");
        assertEquals(
                "`ttl` must not be blank",
                assertThrows(
                                IllegalArgumentException.class,
                                () -> GreptimeConnectorOptions.validate(blankTtl, resolvedSchemaWithoutPrimaryKey()))
                        .getMessage());

        Configuration ttlWithComma = baseOptions(List.of("host"));
        ttlWithComma.set(GreptimeConnectorOptions.TTL, "7d,forever");
        assertEquals(
                "`ttl` must not contain ','",
                assertThrows(
                                IllegalArgumentException.class,
                                () -> GreptimeConnectorOptions.validate(
                                        ttlWithComma, resolvedSchemaWithoutPrimaryKey()))
                        .getMessage());

        Configuration blankMergeMode = baseOptions(List.of("host"));
        blankMergeMode.set(GreptimeConnectorOptions.MERGE_MODE, "");
        assertEquals(
                "`merge-mode` must not be blank",
                assertThrows(
                                IllegalArgumentException.class,
                                () -> GreptimeConnectorOptions.validate(
                                        blankMergeMode, resolvedSchemaWithoutPrimaryKey()))
                        .getMessage());

        Configuration mergeModeWithComma = baseOptions(List.of("host"));
        mergeModeWithComma.set(GreptimeConnectorOptions.MERGE_MODE, "last_row,last_non_null");
        assertEquals(
                "`merge-mode` must not contain ','",
                assertThrows(
                                IllegalArgumentException.class,
                                () -> GreptimeConnectorOptions.validate(
                                        mergeModeWithComma, resolvedSchemaWithoutPrimaryKey()))
                        .getMessage());

        Configuration invalidMergeMode = baseOptions(List.of("host"));
        invalidMergeMode.set(GreptimeConnectorOptions.MERGE_MODE, "unknown");
        assertEquals(
                "`merge-mode` must be one of [last_row, last_non_null], but was: unknown",
                assertThrows(
                                IllegalArgumentException.class,
                                () -> GreptimeConnectorOptions.validate(
                                        invalidMergeMode, resolvedSchemaWithoutPrimaryKey()))
                        .getMessage());
    }

    @Test
    void acceptsSupportedHintOptions() {
        Configuration options = baseOptions(List.of("host"));
        options.set(GreptimeConnectorOptions.TTL, "7d");
        options.set(GreptimeConnectorOptions.MERGE_MODE, "LAST_NON_NULL");

        assertDoesNotThrow(() -> GreptimeConnectorOptions.validate(options, resolvedSchemaWithoutPrimaryKey()));
    }

    @Test
    void treatsBlankCredentialsAsAnonymousAccess() {
        Configuration options = baseOptions(List.of("host"));
        options.set(GreptimeConnectorOptions.USERNAME, " ");
        options.set(GreptimeConnectorOptions.PASSWORD, "");

        assertDoesNotThrow(() -> GreptimeConnectorOptions.validate(options, resolvedSchemaWithoutPrimaryKey()));
    }

    @Test
    void rejectsNegativeFlushIntervalMs() {
        Configuration options = baseOptions(List.of("host"));
        options.set(GreptimeConnectorOptions.FLUSH_INTERVAL_MS, -1L);

        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> GreptimeConnectorOptions.validate(options, resolvedSchemaWithoutPrimaryKey()));

        assertEquals("`flush.interval-ms` must be greater than or equal to 0", error.getMessage());
    }

    @Test
    void rejectsNegativeRouteRefreshPeriodS() {
        Configuration options = baseOptions(List.of("host"));
        options.set(GreptimeConnectorOptions.ROUTE_REFRESH_PERIOD_S, -1L);

        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> GreptimeConnectorOptions.validate(options, resolvedSchemaWithoutPrimaryKey()));

        assertEquals("`route.refresh-period-s` must be greater than or equal to 0", error.getMessage());
    }

    @Test
    void acceptsZeroRouteRefreshPeriodS() {
        Configuration options = baseOptions(List.of("host"));
        options.set(GreptimeConnectorOptions.ROUTE_REFRESH_PERIOD_S, 0L);

        assertDoesNotThrow(() -> GreptimeConnectorOptions.validate(options, resolvedSchemaWithoutPrimaryKey()));
    }

    @Test
    void rejectsNonPositiveBatchMaxRows() {
        Configuration options = baseOptions(List.of("host"));
        options.set(GreptimeConnectorOptions.BATCH_MAX_ROWS, 0);

        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> GreptimeConnectorOptions.validate(options, resolvedSchemaWithoutPrimaryKey()));

        assertEquals("`batch.max-rows` must be greater than 0", error.getMessage());
    }

    @Test
    void rejectsUnsupportedWriteMode() {
        Configuration options = baseOptions(List.of("host"));
        options.set(GreptimeConnectorOptions.SINK_WRITE_MODE, "streaming");

        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> GreptimeConnectorOptions.validate(options, resolvedSchemaWithoutPrimaryKey()));

        assertTrue(error.getMessage().contains("`sink.write-mode` must be one of"));
        assertTrue(error.getMessage().contains("streaming"));
    }

    @Test
    void rejectsUnsupportedChangelogMode() {
        Configuration options = baseOptions(List.of("host"));
        options.set(GreptimeConnectorOptions.SINK_CHANGELOG_MODE, "upsert");

        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> GreptimeConnectorOptions.validate(options, resolvedSchemaWithoutPrimaryKey()));

        assertTrue(error.getMessage().contains("`sink.changelog-mode` must be one of"));
        assertTrue(error.getMessage().contains("upsert"));
    }

    @Test
    void rejectsInvalidSinkParallelism() {
        Configuration options = baseOptions(List.of("host"));
        options.set(GreptimeConnectorOptions.SINK_PARALLELISM, 0);

        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> GreptimeConnectorOptions.validate(options, resolvedSchemaWithoutPrimaryKey()));

        assertEquals("`sink.parallelism` must be greater than 0", error.getMessage());
    }

    @Test
    void rejectsRetractModeUnsupportedCombinations() {
        Configuration bulk = bulkOptions();
        bulk.set(GreptimeConnectorOptions.SINK_CHANGELOG_MODE, "retract");
        assertEquals(
                "`sink.changelog-mode=retract` is not supported when `sink.write-mode=bulk`",
                assertThrows(
                                IllegalArgumentException.class,
                                () -> GreptimeConnectorOptions.validate(bulk, resolvedSchemaWithoutPrimaryKey()))
                        .getMessage());

        Configuration appendMode = baseOptions(List.of("host"));
        appendMode.set(GreptimeConnectorOptions.SINK_CHANGELOG_MODE, "retract");
        appendMode.set(GreptimeConnectorOptions.APPEND_MODE, true);
        assertEquals(
                "`append-mode=true` cannot be used with `sink.changelog-mode=retract`",
                assertThrows(
                                IllegalArgumentException.class,
                                () -> GreptimeConnectorOptions.validate(appendMode, resolvedSchemaWithoutPrimaryKey()))
                        .getMessage());

        Configuration parallelism = baseOptions(List.of("host"));
        parallelism.set(GreptimeConnectorOptions.SINK_CHANGELOG_MODE, "retract");
        parallelism.set(GreptimeConnectorOptions.SINK_PARALLELISM, 2);
        assertEquals(
                "`sink.parallelism` must be 1 when `sink.changelog-mode=retract`",
                assertThrows(
                                IllegalArgumentException.class,
                                () -> GreptimeConnectorOptions.validate(parallelism, resolvedSchemaWithoutPrimaryKey()))
                        .getMessage());
    }

    @Test
    void rejectsPrimaryKeyAndTagMismatchInRetractModeEvenWhenAutoCreateTableIsDisabled() {
        Configuration options = baseOptions(List.of("host"));
        options.set(GreptimeConnectorOptions.AUTO_CREATE_TABLE, false);
        options.set(GreptimeConnectorOptions.SINK_CHANGELOG_MODE, "retract");

        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> GreptimeConnectorOptions.validate(
                        options, resolvedSchemaWithPrimaryKey(List.of("host", "region"))));

        assertEquals(
                "`PRIMARY KEY` must match the Greptime tag order derived from physical columns, PRIMARY KEY=[host, region], tags=[host], physicalTagOrder=[host]",
                error.getMessage());
    }

    @Test
    void negotiatesChangelogModeAndSinkParallelism() {
        Map<String, String> retractOptions = baseOptionMap();
        retractOptions.put(GreptimeConnectorOptions.SINK_CHANGELOG_MODE.key(), "retract");
        GreptimeDynamicTableSink retractSink =
                createSink(retractOptions, resolvedSchemaWithPrimaryKey(List.of("host")), "metrics_retract");

        ChangelogMode retractMode = retractSink.getChangelogMode(ChangelogMode.insertOnly());
        assertTrue(retractMode.contains(RowKind.INSERT));
        assertTrue(retractMode.contains(RowKind.UPDATE_BEFORE));
        assertTrue(retractMode.contains(RowKind.UPDATE_AFTER));
        assertTrue(retractMode.contains(RowKind.DELETE));
        assertEquals(1, retractSink.getSinkParallelism());
        assertEquals(
                Integer.valueOf(1),
                ((SinkV2Provider) retractSink.getSinkRuntimeProvider(null))
                        .getParallelism()
                        .orElseThrow());

        Map<String, String> insertOnlyOptions = baseOptionMap();
        insertOnlyOptions.put(GreptimeConnectorOptions.SINK_PARALLELISM.key(), "3");
        GreptimeDynamicTableSink insertOnlySink =
                createSink(insertOnlyOptions, resolvedSchemaWithoutPrimaryKey(), "metrics_insert_only");

        assertEquals(ChangelogMode.insertOnly(), insertOnlySink.getChangelogMode(ChangelogMode.all()));
        assertEquals(3, insertOnlySink.getSinkParallelism());
        assertEquals(
                Integer.valueOf(3),
                ((SinkV2Provider) insertOnlySink.getSinkRuntimeProvider(null))
                        .getParallelism()
                        .orElseThrow());

        GreptimeDynamicTableSink defaultSink =
                createSink(baseOptionMap(), resolvedSchemaWithoutPrimaryKey(), "metrics_default");
        assertEquals(null, defaultSink.getSinkParallelism());
        assertFalse(((SinkV2Provider) defaultSink.getSinkRuntimeProvider(null))
                .getParallelism()
                .isPresent());
    }

    @Test
    void rejectsBulkWriteModeUnsupportedHints() {
        Configuration autoCreateTable = bulkOptions();
        autoCreateTable.set(GreptimeConnectorOptions.AUTO_CREATE_TABLE, true);
        assertEquals(
                "`sink.write-mode=bulk` requires `auto-create-table=false` because Bulk Write does not create tables",
                assertThrows(
                                IllegalArgumentException.class,
                                () -> GreptimeConnectorOptions.validate(
                                        autoCreateTable, resolvedSchemaWithoutPrimaryKey()))
                        .getMessage());

        Configuration ttl = bulkOptions();
        ttl.set(GreptimeConnectorOptions.TTL, "7d");
        assertEquals(
                "`ttl` is not supported when `sink.write-mode=bulk`",
                assertThrows(
                                IllegalArgumentException.class,
                                () -> GreptimeConnectorOptions.validate(ttl, resolvedSchemaWithoutPrimaryKey()))
                        .getMessage());

        Configuration appendMode = bulkOptions();
        appendMode.set(GreptimeConnectorOptions.APPEND_MODE, true);
        assertEquals(
                "`append-mode=true` is not supported when `sink.write-mode=bulk`",
                assertThrows(
                                IllegalArgumentException.class,
                                () -> GreptimeConnectorOptions.validate(appendMode, resolvedSchemaWithoutPrimaryKey()))
                        .getMessage());

        Configuration mergeMode = bulkOptions();
        mergeMode.set(GreptimeConnectorOptions.MERGE_MODE, "last_row");
        assertEquals(
                "`merge-mode` is not supported when `sink.write-mode=bulk`",
                assertThrows(
                                IllegalArgumentException.class,
                                () -> GreptimeConnectorOptions.validate(mergeMode, resolvedSchemaWithoutPrimaryKey()))
                        .getMessage());
    }

    @Test
    void rejectsExplicitRegularWriteOptionsInBulkMode() {
        Configuration compression = bulkOptions();
        compression.set(
                GreptimeConnectorOptions.WRITE_COMPRESSION, GreptimeSinkConfig.WriteCompression.NONE.optionValue());
        assertEquals(
                "Regular Write options are not supported when `sink.write-mode=bulk`: `write.compression`",
                assertThrows(
                                IllegalArgumentException.class,
                                () -> GreptimeConnectorOptions.validate(compression, resolvedSchemaWithoutPrimaryKey()))
                        .getMessage());

        Configuration rpcTimeout = bulkOptions();
        rpcTimeout.set(GreptimeConnectorOptions.RPC_TIMEOUT_MS, 30000);
        assertEquals(
                "Regular Write options are not supported when `sink.write-mode=bulk`: `rpc.timeout-ms`",
                assertThrows(
                                IllegalArgumentException.class,
                                () -> GreptimeConnectorOptions.validate(rpcTimeout, resolvedSchemaWithoutPrimaryKey()))
                        .getMessage());
    }

    @Test
    void rejectsTagsThatDoNotFollowPhysicalColumnOrder() {
        Configuration options = baseOptions(List.of("region", "host"));

        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> GreptimeConnectorOptions.validate(options, resolvedSchemaWithoutPrimaryKey()));

        assertEquals(
                "`tags` must follow the physical column order of tag columns, tags=[region, host], physicalTagOrder=[host, region]",
                error.getMessage());
    }

    @Test
    void rejectsPrimaryKeyThatDoesNotMatchPhysicalTagOrder() {
        Configuration options = baseOptions(List.of("host", "region"));

        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> GreptimeConnectorOptions.validate(
                        options, resolvedSchemaWithPrimaryKey(List.of("region", "host"))));

        assertEquals(
                "`PRIMARY KEY` must match the Greptime tag order derived from physical columns, PRIMARY KEY=[region, host], tags=[host, region], physicalTagOrder=[host, region]",
                error.getMessage());
    }

    private static Configuration baseOptions(List<String> tags) {
        Configuration options = new Configuration();
        options.set(GreptimeConnectorOptions.TIME_INDEX, "ts");
        options.set(GreptimeConnectorOptions.TAGS, tags);
        return options;
    }

    private static Map<String, String> baseOptionMap() {
        Map<String, String> options = new HashMap<>();
        options.put(GreptimeConnectorOptions.ENDPOINTS.key(), "127.0.0.1:4001");
        options.put(GreptimeConnectorOptions.TIME_INDEX.key(), "ts");
        options.put(GreptimeConnectorOptions.TAGS.key(), "host");
        return options;
    }

    private static Map<String, String> bulkPreflightOptionMap() {
        Map<String, String> options = baseOptionMap();
        options.put(GreptimeConnectorOptions.SINK_WRITE_MODE.key(), "bulk");
        options.put(GreptimeConnectorOptions.AUTO_CREATE_TABLE.key(), "false");
        options.put(GreptimeConnectorOptions.PREFLIGHT_ENABLED.key(), "true");
        return options;
    }

    private static Configuration bulkOptions() {
        Configuration options = baseOptions(List.of("host"));
        options.set(GreptimeConnectorOptions.SINK_WRITE_MODE, "bulk");
        options.set(GreptimeConnectorOptions.AUTO_CREATE_TABLE, false);
        options.set(
                GreptimeConnectorOptions.BULK_COLUMN_BUFFER_SIZE, GreptimeBulkWriteConfig.DEFAULT_COLUMN_BUFFER_SIZE);
        return options;
    }

    private static GreptimeDynamicTableSink createSink(
            Map<String, String> options, ResolvedSchema resolvedSchema, String objectName) {
        return createSink(options, Map.of(), resolvedSchema, objectName);
    }

    private static GreptimeDynamicTableSink createSink(
            Map<String, String> options,
            Map<String, String> enrichmentOptions,
            ResolvedSchema resolvedSchema,
            String objectName) {
        DynamicTableSink sink = new GreptimeDynamicTableFactory()
                .createDynamicTableSink(new TestFactoryContext(options, enrichmentOptions, resolvedSchema, objectName));
        assertTrue(sink instanceof GreptimeDynamicTableSink);
        return (GreptimeDynamicTableSink) sink;
    }

    private static ResolvedSchema resolvedSchemaWithPrimaryKey(List<String> primaryKeyColumns) {
        return new ResolvedSchema(columns(), List.of(), UniqueConstraint.primaryKey("pk", primaryKeyColumns));
    }

    private static ResolvedSchema resolvedSchemaWithoutPrimaryKey() {
        return new ResolvedSchema(columns(), List.of(), null);
    }

    private static List<Column> columns() {
        return List.of(
                Column.physical("host", DataTypes.STRING()),
                Column.physical("region", DataTypes.STRING()),
                Column.physical("cpu", DataTypes.DOUBLE()),
                Column.physical("ts", DataTypes.TIMESTAMP(3).notNull()));
    }

    private static final class TestFactoryContext implements DynamicTableFactory.Context {
        private final ObjectIdentifier objectIdentifier;
        private final ResolvedCatalogTable catalogTable;
        private final Map<String, String> enrichmentOptions;
        private final Configuration configuration = new Configuration();

        private TestFactoryContext(Map<String, String> options, ResolvedSchema resolvedSchema, String objectName) {
            this(options, Map.of(), resolvedSchema, objectName);
        }

        private TestFactoryContext(
                Map<String, String> options,
                Map<String, String> enrichmentOptions,
                ResolvedSchema resolvedSchema,
                String objectName) {
            CatalogTable table = CatalogTable.newBuilder()
                    .schema(Schema.newBuilder()
                            .fromResolvedSchema(resolvedSchema)
                            .build())
                    .options(options)
                    .build();
            this.objectIdentifier = ObjectIdentifier.of("default_catalog", "default_database", objectName);
            this.catalogTable = new ResolvedCatalogTable(table, resolvedSchema);
            this.enrichmentOptions = Map.copyOf(enrichmentOptions);
        }

        @Override
        public ObjectIdentifier getObjectIdentifier() {
            return objectIdentifier;
        }

        @Override
        public ResolvedCatalogTable getCatalogTable() {
            return catalogTable;
        }

        @Override
        public Map<String, String> getEnrichmentOptions() {
            return enrichmentOptions;
        }

        @Override
        public ReadableConfig getConfiguration() {
            return configuration;
        }

        @Override
        public ClassLoader getClassLoader() {
            return Thread.currentThread().getContextClassLoader();
        }

        @Override
        public boolean isTemporary() {
            return false;
        }
    }
}
