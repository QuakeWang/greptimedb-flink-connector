package io.greptime.flink.sink;

import java.util.Objects;
import java.util.function.IntSupplier;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;

final class GreptimeBulkSinkMetrics {
    static final String BULK_FLUSH_BYTES_TOTAL = "greptimedb.bulk.flush.bytes.total";
    static final String BULK_FLUSH_LAST_BYTES = "greptimedb.bulk.flush.last.bytes";
    static final String BULK_AFFECTED_ROWS_TOTAL = "greptimedb.bulk.affected.rows.total";
    static final String BULK_STREAM_READY_FALSE_TOTAL = "greptimedb.bulk.stream.ready.false.total";
    static final String BULK_COMPLETED_TOTAL = "greptimedb.bulk.completed.total";
    static final String BULK_COMPLETED_FAILURE_TOTAL = "greptimedb.bulk.completed.failure.total";

    private final GreptimeSinkMetrics common;
    private final Counter bulkFlushBytesTotal;
    private final Counter bulkAffectedRowsTotal;
    private final Counter bulkStreamReadyFalseTotal;
    private final Counter bulkCompletedTotal;
    private final Counter bulkCompletedFailureTotal;

    private volatile long lastBulkFlushBytes;

    GreptimeBulkSinkMetrics(SinkWriterMetricGroup metricGroup, IntSupplier currentRowsSupplier) {
        SinkWriterMetricGroup metrics = Objects.requireNonNull(metricGroup, "metricGroup");
        this.common = new GreptimeSinkMetrics(metrics, currentRowsSupplier);
        metrics.gauge(BULK_FLUSH_LAST_BYTES, () -> lastBulkFlushBytes);
        this.bulkFlushBytesTotal = metrics.counter(BULK_FLUSH_BYTES_TOTAL);
        this.bulkAffectedRowsTotal = metrics.counter(BULK_AFFECTED_ROWS_TOTAL);
        this.bulkStreamReadyFalseTotal = metrics.counter(BULK_STREAM_READY_FALSE_TOTAL);
        this.bulkCompletedTotal = metrics.counter(BULK_COMPLETED_TOTAL);
        this.bulkCompletedFailureTotal = metrics.counter(BULK_COMPLETED_FAILURE_TOTAL);
    }

    void recordFlushSuccess(FlushReason reason, int rows, long durationMs, long bytesUsed, int affectedRows) {
        common.recordFlushSuccess(reason, rows, durationMs);
        runMetricUpdate(() -> {
            lastBulkFlushBytes = bytesUsed;
            bulkFlushBytesTotal.inc(bytesUsed);
            bulkAffectedRowsTotal.inc(affectedRows);
        });
    }

    void recordFlushFailure(FlushReason reason, int rows, long durationMs) {
        common.recordFlushFailure(reason, rows, durationMs);
    }

    void recordEmptyFlush(FlushReason reason) {
        common.recordEmptyFlush(reason);
    }

    void recordStreamReadyFalse() {
        runMetricUpdate(bulkStreamReadyFalseTotal::inc);
    }

    void recordCompletedSuccess() {
        runMetricUpdate(bulkCompletedTotal::inc);
    }

    void recordCompletedFailure() {
        runMetricUpdate(bulkCompletedFailureTotal::inc);
    }

    private static void runMetricUpdate(Runnable update) {
        try {
            update.run();
        } catch (RuntimeException ignored) {
            // Metrics must not change write semantics.
        }
    }
}
