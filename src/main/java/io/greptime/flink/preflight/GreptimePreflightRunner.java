package io.greptime.flink.preflight;

import io.greptime.flink.cfg.GreptimeChangelogMode;
import io.greptime.flink.cfg.GreptimeSinkConfig;
import io.greptime.flink.cfg.GreptimeWriteMode;
import io.greptime.flink.metadata.GreptimeMetadataClient;
import io.greptime.flink.metadata.GreptimeTableMetadata;
import io.greptime.flink.query.GreptimeQueryConfig;
import io.greptime.flink.sink.schema.GreptimeTableSchema;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public final class GreptimePreflightRunner {
    private final GreptimeTableInspector inspector;

    public GreptimePreflightRunner() {
        this(new GreptimeTableInspector());
    }

    GreptimePreflightRunner(GreptimeTableInspector inspector) {
        this.inspector = inspector;
    }

    public void runSink(
            GreptimePreflightConfig preflightConfig, GreptimeSinkConfig sinkConfig, GreptimeTableSchema tableSchema)
            throws IOException {
        if (!preflightConfig.isEnabled()) {
            return;
        }
        if (sinkConfig.getWriteMode() != GreptimeWriteMode.BULK
                || sinkConfig.getChangelogMode() != GreptimeChangelogMode.INSERT_ONLY) {
            throw failure(
                    mode(sinkConfig),
                    sinkConfig.getDatabase(),
                    tableSchema.getTableName(),
                    List.of(PreflightFinding.of(
                            "unsupported-mode",
                            "sink mode",
                            mode(sinkConfig),
                            "bulk",
                            "`preflight.enabled=true` is currently supported only for bulk insert-only sink")));
        }

        GreptimeQueryConfig queryConfig = preflightConfig
                .getQueryConfig()
                .orElseThrow(() -> new IOException("GreptimeDB preflight enabled without query config"));
        Optional<GreptimeTableMetadata> metadata =
                new GreptimeMetadataClient(queryConfig).loadTable(sinkConfig.getDatabase(), tableSchema.getTableName());
        List<PreflightFinding> findings = inspector.inspectBulkSink(sinkConfig.getDatabase(), tableSchema, metadata);
        if (!findings.isEmpty()) {
            throw failure(mode(sinkConfig), sinkConfig.getDatabase(), tableSchema.getTableName(), findings);
        }
    }

    private static IOException failure(String mode, String database, String table, List<PreflightFinding> findings) {
        String details = findings.stream().map(PreflightFinding::format).collect(Collectors.joining("\n"));
        return new IOException("GreptimeDB preflight failed: mode="
                + mode
                + ", table="
                + database
                + "."
                + table
                + ", reason="
                + findings.get(0).getCategory()
                + "\n"
                + details);
    }

    private static String mode(GreptimeSinkConfig sinkConfig) {
        if (sinkConfig.getChangelogMode() == GreptimeChangelogMode.RETRACT) {
            return "retract";
        }
        return sinkConfig.getWriteMode().optionValue();
    }
}
