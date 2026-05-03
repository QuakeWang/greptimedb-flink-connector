package io.greptime.flink.table;

import io.greptime.flink.cfg.GreptimeBulkWriteConfig;
import io.greptime.flink.cfg.GreptimeChangelogMode;
import io.greptime.flink.cfg.GreptimeSinkConfig;
import io.greptime.flink.cfg.GreptimeWriteMode;
import io.greptime.flink.sink.schema.GreptimeTableSchema;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;

public final class GreptimeDynamicTableFactory implements DynamicTableSinkFactory {
    @Override
    public String factoryIdentifier() {
        return GreptimeConnectorOptions.IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(GreptimeConnectorOptions.ENDPOINTS);
        options.add(GreptimeConnectorOptions.TIME_INDEX);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(GreptimeConnectorOptions.DATABASE);
        options.add(GreptimeConnectorOptions.TABLE);
        options.add(GreptimeConnectorOptions.USERNAME);
        options.add(GreptimeConnectorOptions.PASSWORD);
        options.add(GreptimeConnectorOptions.TAGS);
        options.add(GreptimeConnectorOptions.AUTO_CREATE_TABLE);
        options.add(GreptimeConnectorOptions.APPEND_MODE);
        options.add(GreptimeConnectorOptions.MERGE_MODE);
        options.add(GreptimeConnectorOptions.TTL);
        options.add(GreptimeConnectorOptions.BATCH_MAX_ROWS);
        options.add(GreptimeConnectorOptions.FLUSH_INTERVAL_MS);
        options.add(GreptimeConnectorOptions.SINK_WRITE_MODE);
        options.add(GreptimeConnectorOptions.SINK_CHANGELOG_MODE);
        options.add(GreptimeConnectorOptions.SINK_PARALLELISM);
        options.add(GreptimeConnectorOptions.BULK_COLUMN_BUFFER_SIZE);
        options.add(GreptimeConnectorOptions.BULK_TIMEOUT_MS_PER_MESSAGE);
        options.add(GreptimeConnectorOptions.BULK_MAX_REQUESTS_IN_FLIGHT);
        options.add(GreptimeConnectorOptions.BULK_ALLOCATOR_INIT_RESERVATION_BYTES);
        options.add(GreptimeConnectorOptions.BULK_ALLOCATOR_MAX_ALLOCATION_BYTES);
        options.add(GreptimeConnectorOptions.WRITE_MAX_RETRIES);
        options.add(GreptimeConnectorOptions.WRITE_MAX_IN_FLIGHT_POINTS);
        options.add(GreptimeConnectorOptions.WRITE_LIMIT_POLICY);
        options.add(GreptimeConnectorOptions.WRITE_LIMIT_TIMEOUT_MS);
        options.add(GreptimeConnectorOptions.WRITE_COMPRESSION);
        options.add(GreptimeConnectorOptions.RPC_TIMEOUT_MS);
        options.add(GreptimeConnectorOptions.ROUTE_REFRESH_PERIOD_S);
        options.add(GreptimeConnectorOptions.ROUTE_HEALTH_TIMEOUT_MS);
        return options;
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        helper.validate();

        ReadableConfig options = helper.getOptions();
        ResolvedSchema resolvedSchema = context.getCatalogTable().getResolvedSchema();
        DataType physicalRowDataType = resolvedSchema.toPhysicalRowDataType();
        GreptimeConnectorOptions.validate(options, resolvedSchema);

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

        return new GreptimeDynamicTableSink(sinkConfig, tableSchema, sinkParallelism);
    }

    private static Integer effectiveSinkParallelism(ReadableConfig options, GreptimeChangelogMode changelogMode) {
        return options.getOptional(GreptimeConnectorOptions.SINK_PARALLELISM)
                .orElse(changelogMode == GreptimeChangelogMode.RETRACT ? 1 : null);
    }
}
