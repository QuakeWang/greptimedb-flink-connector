package io.greptime.flink.table;

import io.greptime.flink.cfg.GreptimeBulkWriteConfig;
import io.greptime.flink.cfg.GreptimeChangelogMode;
import io.greptime.flink.cfg.GreptimeSinkConfig;
import io.greptime.flink.cfg.GreptimeWriteMode;
import io.greptime.flink.preflight.GreptimePreflightConfig;
import io.greptime.flink.query.GreptimeQueryConfig;
import io.greptime.flink.query.GreptimeQueryDialect;
import io.greptime.flink.sink.schema.GreptimeTableSchema;
import io.greptime.flink.source.GreptimeDynamicTableSource;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;

public final class GreptimeDynamicTableFactory implements DynamicTableSinkFactory, DynamicTableSourceFactory {
    @Override
    public String factoryIdentifier() {
        return GreptimeConnectorOptions.IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Set.of();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return GreptimeConnectorOptions.allOptions();
    }

    @Override
    public Set<ConfigOption<?>> forwardOptions() {
        Set<ConfigOption<?>> options = GreptimeConnectorOptions.sourceForwardOptions();
        options.addAll(GreptimeConnectorOptions.sinkForwardOptions());
        return options;
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        rejectSourcePreflightOption(context);
        ReadableConfig options = validateFactoryOptions(
                context, GreptimeConnectorOptions.sourceOptions(), GreptimeConnectorOptions.sourceForwardOptions());
        ResolvedSchema resolvedSchema = context.getCatalogTable().getResolvedSchema();
        DataType physicalRowDataType = resolvedSchema.toPhysicalRowDataType();
        String tableName = options.getOptional(GreptimeConnectorOptions.TABLE)
                .orElseGet(() -> context.getObjectIdentifier().getObjectName());
        String jdbcUrl =
                options.getOptional(GreptimeConnectorOptions.QUERY_JDBC_URL).orElse(null);
        if (jdbcUrl != null) {
            GreptimeQueryDialect.JdbcUrlInspection inspection =
                    GreptimeQueryDialect.MYSQL.inspectSensitiveMaterial(jdbcUrl);
            if (inspection.isMalformed()) {
                return GreptimeDynamicTableSource.withDeferredValidationFailure(
                        GreptimeDynamicTableSource.DeferredValidationFailure.malformedJdbcUrl(
                                inspection.malformedMessage()),
                        physicalRowDataType);
            }
            if (inspection.isSensitive()) {
                return GreptimeDynamicTableSource.withDeferredValidationFailure(
                        GreptimeDynamicTableSource.DeferredValidationFailure.sensitiveJdbcUrl(
                                GreptimeQueryDialect.MYSQL.redactJdbcUrl(jdbcUrl)),
                        physicalRowDataType);
            }
        }
        GreptimeQueryConfig queryConfig = GreptimeConnectorOptions.createQueryConfig(options, tableName);

        return new GreptimeDynamicTableSource(queryConfig, physicalRowDataType);
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        ReadableConfig options = validateFactoryOptions(
                context, GreptimeConnectorOptions.sinkOptions(), GreptimeConnectorOptions.sinkForwardOptions());
        ResolvedSchema resolvedSchema = context.getCatalogTable().getResolvedSchema();
        DataType physicalRowDataType = resolvedSchema.toPhysicalRowDataType();
        GreptimeConnectorOptions.validateSinkFactoryOptions(options, resolvedSchema);

        String tableName = options.getOptional(GreptimeConnectorOptions.TABLE)
                .orElseGet(() -> context.getObjectIdentifier().getObjectName());
        List<String> tags = options.get(GreptimeConnectorOptions.TAGS);
        GreptimeBulkWriteConfig bulkWriteConfig = GreptimeBulkWriteConfig.builder()
                .columnBufferSize(options.get(GreptimeConnectorOptions.BULK_COLUMN_BUFFER_SIZE))
                .timeoutMsPerMessage(options.get(GreptimeConnectorOptions.BULK_TIMEOUT_MS_PER_MESSAGE))
                .maxRequestsInFlight(options.get(GreptimeConnectorOptions.BULK_MAX_REQUESTS_IN_FLIGHT))
                .allocatorInitReservationBytes(
                        options.get(GreptimeConnectorOptions.BULK_ALLOCATOR_INIT_RESERVATION_BYTES))
                .allocatorMaxAllocationBytes(options.get(GreptimeConnectorOptions.BULK_ALLOCATOR_MAX_ALLOCATION_BYTES))
                .build();

        GreptimeSinkConfig sinkConfig = GreptimeSinkConfig.builder()
                .endpoints(options.get(GreptimeConnectorOptions.ENDPOINTS))
                .database(options.get(GreptimeConnectorOptions.DATABASE))
                .credentials(
                        options.getOptional(GreptimeConnectorOptions.USERNAME).orElse(null),
                        options.getOptional(GreptimeConnectorOptions.PASSWORD).orElse(null))
                .batchMaxRows(options.get(GreptimeConnectorOptions.BATCH_MAX_ROWS))
                .flushIntervalMs(options.get(GreptimeConnectorOptions.FLUSH_INTERVAL_MS))
                .autoCreateTable(options.get(GreptimeConnectorOptions.AUTO_CREATE_TABLE))
                .ttl(options.getOptional(GreptimeConnectorOptions.TTL).orElse(null))
                .appendMode(options.getOptional(GreptimeConnectorOptions.APPEND_MODE)
                        .orElse(null))
                .mergeMode(
                        options.getOptional(GreptimeConnectorOptions.MERGE_MODE).orElse(null))
                .writeMaxRetries(options.get(GreptimeConnectorOptions.WRITE_MAX_RETRIES))
                .writeMaxInFlightPoints(options.get(GreptimeConnectorOptions.WRITE_MAX_IN_FLIGHT_POINTS))
                .writeLimitPolicy(GreptimeSinkConfig.WriteLimitPolicy.fromOptionValue(
                        options.get(GreptimeConnectorOptions.WRITE_LIMIT_POLICY)))
                .writeLimitTimeoutMs(options.get(GreptimeConnectorOptions.WRITE_LIMIT_TIMEOUT_MS))
                .writeCompression(GreptimeSinkConfig.WriteCompression.fromOptionValue(
                        options.get(GreptimeConnectorOptions.WRITE_COMPRESSION)))
                .rpcTimeoutMs(options.get(GreptimeConnectorOptions.RPC_TIMEOUT_MS))
                .routeRefreshPeriodSeconds(options.get(GreptimeConnectorOptions.ROUTE_REFRESH_PERIOD_S))
                .routeHealthTimeoutMs(options.get(GreptimeConnectorOptions.ROUTE_HEALTH_TIMEOUT_MS))
                .writeMode(GreptimeWriteMode.fromOptionValue(options.get(GreptimeConnectorOptions.SINK_WRITE_MODE)))
                .changelogMode(GreptimeChangelogMode.fromOptionValue(
                        options.get(GreptimeConnectorOptions.SINK_CHANGELOG_MODE)))
                .bulkWriteConfig(bulkWriteConfig)
                .build();

        GreptimeTableSchema tableSchema = GreptimeTableSchema.from(
                tableName, physicalRowDataType, options.get(GreptimeConnectorOptions.TIME_INDEX), tags);
        Integer sinkParallelism = effectiveSinkParallelism(options, sinkConfig.getChangelogMode());
        GreptimePreflightConfig preflightConfig = GreptimeConnectorOptions.createSinkPreflightConfig(
                options, context.getCatalogTable().getOptions(), context.getEnrichmentOptions(), tableName);

        return new GreptimeDynamicTableSink(sinkConfig, tableSchema, sinkParallelism, preflightConfig);
    }

    private static void rejectSourcePreflightOption(Context context) {
        String key = GreptimeConnectorOptions.PREFLIGHT_ENABLED.key();
        if (context.getCatalogTable().getOptions().containsKey(key)
                || context.getEnrichmentOptions().containsKey(key)) {
            throw new IllegalArgumentException("`preflight.enabled` is not supported for GreptimeDB source yet");
        }
    }

    private static Integer effectiveSinkParallelism(ReadableConfig options, GreptimeChangelogMode changelogMode) {
        return options.getOptional(GreptimeConnectorOptions.SINK_PARALLELISM)
                .orElse(changelogMode == GreptimeChangelogMode.RETRACT ? 1 : null);
    }

    private static ReadableConfig validateFactoryOptions(
            Context context, Set<ConfigOption<?>> typedOptions, Set<ConfigOption<?>> forwardOptions) {
        Configuration options = Configuration.fromMap(context.getCatalogTable().getOptions());
        mergeForwardOptions(options, context.getEnrichmentOptions(), forwardOptions);
        FactoryUtil.validateFactoryOptions(Set.of(), typedOptions, options);
        FactoryUtil.validateUnconsumedKeys(GreptimeConnectorOptions.IDENTIFIER, options.keySet(), consumedOptionKeys());
        FactoryUtil.validateWatermarkOptions(GreptimeConnectorOptions.IDENTIFIER, options);
        return options;
    }

    private static void mergeForwardOptions(
            Configuration options, Map<String, String> enrichmentOptions, Set<ConfigOption<?>> forwardOptions) {
        Configuration enrichment = Configuration.fromMap(enrichmentOptions);
        for (ConfigOption<?> option : forwardOptions) {
            mergeForwardOption(options, enrichment, option);
        }
    }

    private static <T> void mergeForwardOption(
            Configuration options, Configuration enrichment, ConfigOption<T> option) {
        enrichment.getOptional(option).ifPresent(value -> options.set(option, value));
    }

    private static Set<String> consumedOptionKeys() {
        Set<String> keys = new HashSet<>();
        for (ConfigOption<?> option : GreptimeConnectorOptions.allOptions()) {
            keys.add(option.key());
        }
        keys.add(FactoryUtil.CONNECTOR.key());
        keys.add(FactoryUtil.PROPERTY_VERSION.key());
        keys.add(FactoryUtil.WATERMARK_EMIT_STRATEGY.key());
        keys.add(FactoryUtil.WATERMARK_ALIGNMENT_GROUP.key());
        keys.add(FactoryUtil.WATERMARK_ALIGNMENT_MAX_DRIFT.key());
        keys.add(FactoryUtil.WATERMARK_ALIGNMENT_UPDATE_INTERVAL.key());
        keys.add(FactoryUtil.SOURCE_IDLE_TIMEOUT.key());
        return keys;
    }
}
