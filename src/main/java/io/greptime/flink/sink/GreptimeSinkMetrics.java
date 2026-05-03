package io.greptime.flink.sink;

import java.util.EnumMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.IntSupplier;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;

final class GreptimeSinkMetrics {
    static final String BUFFER_ROWS = "greptimedb.buffer.rows";
    static final String FLUSH_TOTAL = "greptimedb.flush.total";
    static final String FLUSH_SUCCESS_TOTAL = "greptimedb.flush.success.total";
    static final String FLUSH_FAILURE_TOTAL = "greptimedb.flush.failure.total";
    static final String FLUSH_EMPTY_TOTAL = "greptimedb.flush.empty.total";
    static final String FLUSH_ROWS_TOTAL = "greptimedb.flush.rows.total";
    static final String FLUSH_DURATION_MS_TOTAL = "greptimedb.flush.duration.ms.total";
    static final String FLUSH_LAST_ROWS = "greptimedb.flush.last.rows";
    static final String FLUSH_LAST_DURATION_MS = "greptimedb.flush.last.duration.ms";

    private static final String FLUSH_REASON_PREFIX = "greptimedb.flush.reason.";
    private static final String FLUSH_REASON_SUFFIX = ".total";
    private static final String WRITE_OPERATION_PREFIX = "greptimedb.write.operation.";
    private static final String WRITE_OPERATION_ROWS_SUFFIX = ".rows.total";

    private final Counter numRecordsSend;
    private final Counter numRecordsSendErrors;
    private final Counter flushTotal;
    private final Counter flushSuccessTotal;
    private final Counter flushFailureTotal;
    private final Counter flushEmptyTotal;
    private final Counter flushRowsTotal;
    private final Counter flushDurationMsTotal;
    private final Map<FlushReason, Counter> flushReasonCounters = new EnumMap<>(FlushReason.class);
    private final Map<GreptimeRowOperation, Counter> writeOperationRowCounters =
            new EnumMap<>(GreptimeRowOperation.class);

    private volatile long currentSendTimeMs;
    private volatile long lastFlushRows;
    private volatile long lastFlushDurationMs;

    GreptimeSinkMetrics(SinkWriterMetricGroup metricGroup, IntSupplier currentRowsSupplier) {
        Objects.requireNonNull(currentRowsSupplier, "currentRowsSupplier");
        SinkWriterMetricGroup metrics = Objects.requireNonNull(metricGroup, "metricGroup");
        this.numRecordsSend = metrics.getNumRecordsSendCounter();
        this.numRecordsSendErrors = metrics.getNumRecordsSendErrorsCounter();
        metrics.setCurrentSendTimeGauge(() -> currentSendTimeMs);
        metrics.gauge(BUFFER_ROWS, () -> (long) currentRowsSupplier.getAsInt());
        metrics.gauge(FLUSH_LAST_ROWS, () -> lastFlushRows);
        metrics.gauge(FLUSH_LAST_DURATION_MS, () -> lastFlushDurationMs);
        this.flushTotal = metrics.counter(FLUSH_TOTAL);
        this.flushSuccessTotal = metrics.counter(FLUSH_SUCCESS_TOTAL);
        this.flushFailureTotal = metrics.counter(FLUSH_FAILURE_TOTAL);
        this.flushEmptyTotal = metrics.counter(FLUSH_EMPTY_TOTAL);
        this.flushRowsTotal = metrics.counter(FLUSH_ROWS_TOTAL);
        this.flushDurationMsTotal = metrics.counter(FLUSH_DURATION_MS_TOTAL);
        for (FlushReason reason : FlushReason.values()) {
            flushReasonCounters.put(reason, metrics.counter(reasonMetricName(reason)));
        }
        for (GreptimeRowOperation operation : GreptimeRowOperation.values()) {
            writeOperationRowCounters.put(operation, metrics.counter(writeOperationRowsMetricName(operation)));
        }
    }

    void recordFlushSuccess(FlushReason reason, GreptimeRowOperation operation, int rows, long durationMs) {
        runMetricUpdate(() -> {
            recordNonEmptyFlush(reason, rows, durationMs);
            numRecordsSend.inc(rows);
            writeOperationRowCounters.get(operation).inc(rows);
            flushSuccessTotal.inc();
            flushRowsTotal.inc(rows);
            flushDurationMsTotal.inc(durationMs);
        });
    }

    void recordFlushSuccess(FlushReason reason, int rows, long durationMs) {
        runMetricUpdate(() -> {
            recordNonEmptyFlush(reason, rows, durationMs);
            numRecordsSend.inc(rows);
            flushSuccessTotal.inc();
            flushRowsTotal.inc(rows);
            flushDurationMsTotal.inc(durationMs);
        });
    }

    void recordFlushFailure(FlushReason reason, int rows, long durationMs) {
        runMetricUpdate(() -> {
            recordNonEmptyFlush(reason, rows, durationMs);
            numRecordsSendErrors.inc(rows);
            flushFailureTotal.inc();
        });
    }

    void recordEmptyFlush(FlushReason reason) {
        runMetricUpdate(() -> {
            flushReasonCounters.get(reason).inc();
            flushEmptyTotal.inc();
        });
    }

    private void recordNonEmptyFlush(FlushReason reason, int rows, long durationMs) {
        currentSendTimeMs = durationMs;
        lastFlushRows = rows;
        lastFlushDurationMs = durationMs;
        flushTotal.inc();
        flushReasonCounters.get(reason).inc();
    }

    private static String reasonMetricName(FlushReason reason) {
        return FLUSH_REASON_PREFIX + reason.metricName() + FLUSH_REASON_SUFFIX;
    }

    private static String writeOperationRowsMetricName(GreptimeRowOperation operation) {
        return WRITE_OPERATION_PREFIX + operation.metricName() + WRITE_OPERATION_ROWS_SUFFIX;
    }

    private static void runMetricUpdate(Runnable update) {
        try {
            update.run();
        } catch (RuntimeException ignored) {
            // Metrics must not change write semantics.
        }
    }
}
