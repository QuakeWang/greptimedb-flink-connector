package io.greptime.flink.sink;

final class BulkWriteAttemptContext {
    private final String tableName;
    private final String database;
    private final int endpoints;
    private final long rows;
    private final Integer affectedRows;
    private final long bytesUsed;
    private final FlushReason flushReason;
    private final int batchMaxRows;
    private final long flushIntervalMs;
    private final String bulkWriteSettings;

    BulkWriteAttemptContext(
            String tableName,
            String database,
            int endpoints,
            long rows,
            Integer affectedRows,
            long bytesUsed,
            FlushReason flushReason,
            int batchMaxRows,
            long flushIntervalMs,
            String bulkWriteSettings) {
        this.tableName = tableName;
        this.database = database;
        this.endpoints = endpoints;
        this.rows = rows;
        this.affectedRows = affectedRows;
        this.bytesUsed = bytesUsed;
        this.flushReason = flushReason;
        this.batchMaxRows = batchMaxRows;
        this.flushIntervalMs = flushIntervalMs;
        this.bulkWriteSettings = bulkWriteSettings;
    }

    String failureMessage(String prefix) {
        return prefix + ": " + summary();
    }

    String affectedRowsMismatchMessage(int actualAffectedRows) {
        return failureMessage("GreptimeDB bulk write affected rows mismatch")
                + ", actualAffectedRows="
                + actualAffectedRows;
    }

    private String summary() {
        return "writeMode=bulk"
                + ", table="
                + tableName
                + ", database="
                + database
                + ", rows="
                + rows
                + ", affectedRows="
                + (affectedRows == null ? "unknown" : affectedRows)
                + ", bytesUsed="
                + bytesUsed
                + ", flushReason="
                + flushReason.metricName()
                + ", batchMaxRows="
                + batchMaxRows
                + ", flushIntervalMs="
                + flushIntervalMs
                + ", endpoints="
                + endpoints
                + ", "
                + bulkWriteSettings;
    }
}
