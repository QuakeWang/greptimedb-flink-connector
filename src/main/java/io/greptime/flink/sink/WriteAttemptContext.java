package io.greptime.flink.sink;

import io.greptime.flink.cfg.GreptimeChangelogMode;
import io.greptime.flink.cfg.GreptimeWriteMode;

final class WriteAttemptContext {
    private final GreptimeWriteMode writeMode;
    private final GreptimeChangelogMode changelogMode;
    private final GreptimeRowOperation writeOperation;
    private final String tableName;
    private final String database;
    private final int endpoints;
    private final int rows;
    private final FlushReason flushReason;
    private final int batchMaxRows;
    private final long flushIntervalMs;
    private final String writeHints;
    private final String writeSettings;

    WriteAttemptContext(
            GreptimeWriteMode writeMode,
            GreptimeChangelogMode changelogMode,
            GreptimeRowOperation writeOperation,
            String tableName,
            String database,
            int endpoints,
            int rows,
            FlushReason flushReason,
            int batchMaxRows,
            long flushIntervalMs,
            String writeHints,
            String writeSettings) {
        this.writeMode = writeMode;
        this.changelogMode = changelogMode;
        this.writeOperation = writeOperation;
        this.tableName = tableName;
        this.database = database;
        this.endpoints = endpoints;
        this.rows = rows;
        this.flushReason = flushReason;
        this.batchMaxRows = batchMaxRows;
        this.flushIntervalMs = flushIntervalMs;
        this.writeHints = writeHints;
        this.writeSettings = writeSettings;
    }

    String failureMessage(String prefix) {
        return prefix + ": " + summary();
    }

    String writeFailedMessage(int code, String error) {
        return failureMessage("GreptimeDB write failed") + ", code=" + code + ", error=" + error;
    }

    String partialFailureMessage(long success, long failure) {
        return failureMessage("GreptimeDB write partially failed") + ", success=" + success + ", failure=" + failure;
    }

    private String summary() {
        return "writeMode="
                + writeMode.optionValue()
                + ", changelogMode="
                + changelogMode.optionValue()
                + ", writeOp="
                + writeOperation.metricName()
                + ", table="
                + tableName
                + ", database="
                + database
                + ", rows="
                + rows
                + ", flushReason="
                + flushReason.metricName()
                + ", batchMaxRows="
                + batchMaxRows
                + ", flushIntervalMs="
                + flushIntervalMs
                + ", endpoints="
                + endpoints
                + ", writeHints="
                + writeHints
                + ", writeSettings="
                + writeSettings;
    }
}
