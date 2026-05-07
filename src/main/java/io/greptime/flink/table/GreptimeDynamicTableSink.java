package io.greptime.flink.table;

import io.greptime.flink.cfg.GreptimeChangelogMode;
import io.greptime.flink.cfg.GreptimeSinkConfig;
import io.greptime.flink.preflight.GreptimePreflightConfig;
import io.greptime.flink.sink.GreptimeSink;
import io.greptime.flink.sink.schema.GreptimeTableSchema;
import java.util.Objects;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;

public final class GreptimeDynamicTableSink implements DynamicTableSink {
    private final GreptimeSinkConfig sinkConfig;
    private final GreptimeTableSchema tableSchema;
    private final Integer sinkParallelism;
    private final GreptimePreflightConfig preflightConfig;

    GreptimeDynamicTableSink(GreptimeSinkConfig sinkConfig, GreptimeTableSchema tableSchema, Integer sinkParallelism) {
        this(sinkConfig, tableSchema, sinkParallelism, GreptimePreflightConfig.disabled());
    }

    GreptimeDynamicTableSink(
            GreptimeSinkConfig sinkConfig,
            GreptimeTableSchema tableSchema,
            Integer sinkParallelism,
            GreptimePreflightConfig preflightConfig) {
        this.sinkConfig = sinkConfig;
        this.tableSchema = tableSchema;
        this.sinkParallelism = sinkParallelism;
        this.preflightConfig = preflightConfig;
    }

    GreptimeSinkConfig getSinkConfig() {
        return sinkConfig;
    }

    GreptimeTableSchema getTableSchema() {
        return tableSchema;
    }

    Integer getSinkParallelism() {
        return sinkParallelism;
    }

    GreptimePreflightConfig getPreflightConfig() {
        return preflightConfig;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        if (sinkConfig.getChangelogMode() == GreptimeChangelogMode.RETRACT) {
            return ChangelogMode.newBuilder()
                    .addContainedKind(RowKind.INSERT)
                    .addContainedKind(RowKind.UPDATE_BEFORE)
                    .addContainedKind(RowKind.UPDATE_AFTER)
                    .addContainedKind(RowKind.DELETE)
                    .build();
        }
        return ChangelogMode.insertOnly();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        GreptimeSink<RowData> sink = new GreptimeSink<>(sinkConfig, tableSchema, preflightConfig);
        if (sinkParallelism == null) {
            return SinkV2Provider.of(sink);
        }
        return SinkV2Provider.of(sink, sinkParallelism);
    }

    @Override
    public DynamicTableSink copy() {
        return new GreptimeDynamicTableSink(sinkConfig, tableSchema, sinkParallelism, preflightConfig);
    }

    @Override
    public String asSummaryString() {
        return "GreptimeDB Table Sink";
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof GreptimeDynamicTableSink)) {
            return false;
        }
        GreptimeDynamicTableSink that = (GreptimeDynamicTableSink) other;
        return Objects.equals(sinkConfig, that.sinkConfig)
                && Objects.equals(tableSchema, that.tableSchema)
                && Objects.equals(sinkParallelism, that.sinkParallelism)
                && Objects.equals(preflightConfig, that.preflightConfig);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sinkConfig, tableSchema, sinkParallelism, preflightConfig);
    }
}
