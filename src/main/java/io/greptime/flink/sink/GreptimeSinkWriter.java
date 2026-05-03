package io.greptime.flink.sink;

import io.greptime.flink.cfg.GreptimeSinkConfig;
import io.greptime.flink.sink.schema.GreptimeRowDataConverter;
import io.greptime.flink.sink.schema.GreptimeTableSchema;
import io.greptime.models.Err;
import io.greptime.models.Result;
import io.greptime.models.Table;
import io.greptime.models.WriteOk;
import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.common.operators.ProcessingTimeService;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.table.data.RowData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class GreptimeSinkWriter<IN extends RowData> implements SinkWriter<IN> {
    private static final Logger LOG = LoggerFactory.getLogger(GreptimeSinkWriter.class);

    private final GreptimeWriteClient client;
    private final GreptimeSinkConfig sinkConfig;
    private final GreptimeTableSchema tableSchema;
    private final GreptimeRowDataConverter insertConverter;
    private final GreptimeRowDataConverter deleteConverter;
    private final ProcessingTimeService processingTimeService;
    private final MailboxExecutor mailboxExecutor;
    private final GreptimeSinkMetrics metrics;

    private PendingRegularBatch currentBatch;
    private volatile int currentRows;
    private ScheduledFuture<?> periodicFlushTimer;
    private volatile State state = State.OPEN;

    GreptimeSinkWriter(
            GreptimeWriteClient client,
            GreptimeSinkConfig sinkConfig,
            GreptimeTableSchema tableSchema,
            GreptimeRowDataConverter insertConverter,
            GreptimeRowDataConverter deleteConverter,
            ProcessingTimeService processingTimeService,
            MailboxExecutor mailboxExecutor,
            SinkWriterMetricGroup metricGroup) {
        this.client = Objects.requireNonNull(client, "client");
        this.sinkConfig = Objects.requireNonNull(sinkConfig, "sinkConfig");
        this.tableSchema = Objects.requireNonNull(tableSchema, "tableSchema");
        this.insertConverter = Objects.requireNonNull(insertConverter, "insertConverter");
        this.deleteConverter = Objects.requireNonNull(deleteConverter, "deleteConverter");
        this.processingTimeService = processingTimeService;
        this.mailboxExecutor = mailboxExecutor;
        this.metrics = new GreptimeSinkMetrics(metricGroup, () -> currentRows);
        if (sinkConfig.isFlushIntervalEnabled()) {
            Objects.requireNonNull(processingTimeService, "processingTimeService");
            Objects.requireNonNull(mailboxExecutor, "mailboxExecutor");
            scheduleNextPeriodicFlush();
        }
    }

    @Override
    public void write(IN element, Context context) throws IOException, InterruptedException {
        ensureOpen();
        GreptimeRowOperation operation;
        try {
            operation = GreptimeRowOperation.fromRowKind(element.getRowKind(), sinkConfig.getChangelogMode());
        } catch (IOException e) {
            markFailedAndClearBatch();
            throw e;
        }
        if (currentBatch != null && currentBatch.operation != operation) {
            flushInternal(FlushReason.OPERATION_CHANGED);
        }
        PendingRegularBatch batch = currentBatch == null ? newPendingBatch(operation) : currentBatch;
        try {
            batch.addRow(element);
        } catch (IllegalArgumentException e) {
            markFailedAndClearBatch();
            throw new IOException(
                    "Invalid row for GreptimeDB table " + tableSchema.getTableName() + ": " + e.getMessage(), e);
        }
        currentBatch = batch;
        currentRows = batch.rows;
        if (currentRows >= sinkConfig.getBatchMaxRows()) {
            flushInternal(FlushReason.BATCH_FULL);
        }
    }

    @Override
    public void flush(boolean endOfInput) throws IOException, InterruptedException {
        if (state == State.CLOSED) {
            return;
        }
        ensureOpen();
        flushInternal(endOfInput ? FlushReason.END_OF_INPUT : FlushReason.FLINK_FLUSH);
    }

    @Override
    public void close() throws Exception {
        if (state == State.CLOSED) {
            return;
        }

        State previousState = state;
        state = State.CLOSED;
        cancelPeriodicFlushTimer();
        try {
            if (previousState == State.OPEN && currentRows > 0) {
                flushInternal(FlushReason.CLOSE);
            }
        } finally {
            state = State.CLOSED;
            client.shutdownGracefully();
        }
    }

    private PendingRegularBatch newPendingBatch(GreptimeRowOperation operation) {
        if (operation == GreptimeRowOperation.DELETE) {
            return new PendingRegularBatch(
                    operation, Table.from(tableSchema.toGreptimeDeleteTableSchema()), deleteConverter);
        }
        return new PendingRegularBatch(operation, Table.from(tableSchema.toGreptimeTableSchema()), insertConverter);
    }

    private void scheduleNextPeriodicFlush() {
        if (!sinkConfig.isFlushIntervalEnabled() || state != State.OPEN) {
            return;
        }

        long nextTimestamp = processingTimeService.getCurrentProcessingTime() + sinkConfig.getFlushIntervalMs();
        periodicFlushTimer = processingTimeService.registerTimer(nextTimestamp, this::onPeriodicFlush);
    }

    private void onPeriodicFlush(long timestamp) {
        if (state != State.OPEN) {
            return;
        }
        mailboxExecutor.execute(
                () -> {
                    if (state != State.OPEN) {
                        return;
                    }
                    flushInternal(FlushReason.PERIODIC);
                    scheduleNextPeriodicFlush();
                },
                "GreptimeDB periodic flush");
    }

    private void cancelPeriodicFlushTimer() {
        if (periodicFlushTimer != null) {
            periodicFlushTimer.cancel(false);
            periodicFlushTimer = null;
        }
    }

    private void flushInternal(FlushReason reason) throws IOException, InterruptedException {
        PendingRegularBatch batch = currentBatch;
        int rows = batch == null ? 0 : batch.rows;
        if (rows == 0) {
            metrics.recordEmptyFlush(reason);
            return;
        }

        long startNanos = System.nanoTime();
        WriteAttemptContext attempt = newWriteAttemptContext(reason, batch.operation, rows);
        batch.table.complete();

        try {
            Result<WriteOk, Err> result = waitForWrite(
                    client.write(batch.table, batch.operation.toWriteOp(), sinkConfig.createWriteContext()),
                    sinkConfig.getRpcTimeoutMs(),
                    attempt);
            WriteOk writeOk = requireSuccessfulWrite(result, attempt);
            long durationMs = elapsedMillis(startNanos);
            metrics.recordFlushSuccess(reason, batch.operation, rows, durationMs);
            LOG.debug(
                    "Wrote {} {} rows into GreptimeDB table {} (reason={}, durationMs={}, success={}, failure={})",
                    rows,
                    batch.operation.metricName(),
                    tableSchema.getTableName(),
                    reason.metricName(),
                    durationMs,
                    writeOk.getSuccess(),
                    writeOk.getFailure());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            markFailed();
            metrics.recordFlushFailure(reason, rows, elapsedMillis(startNanos));
            throw e;
        } catch (IOException e) {
            markFailed();
            metrics.recordFlushFailure(reason, rows, elapsedMillis(startNanos));
            throw e;
        } catch (Exception e) {
            markFailed();
            metrics.recordFlushFailure(reason, rows, elapsedMillis(startNanos));
            throw new IOException(attempt.failureMessage("Failed to write rows to GreptimeDB"), e);
        }

        currentBatch = null;
        currentRows = 0;
    }

    private static Result<WriteOk, Err> waitForWrite(
            CompletableFuture<Result<WriteOk, Err>> writeFuture, int timeoutMs, WriteAttemptContext attempt)
            throws Exception {
        try {
            return writeFuture.get(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            writeFuture.cancel(true);
            throw new IOException(
                    attempt.failureMessage("Timed out after " + timeoutMs + " ms while writing rows to GreptimeDB"), e);
        } catch (InterruptedException e) {
            writeFuture.cancel(true);
            throw e;
        }
    }

    private WriteAttemptContext newWriteAttemptContext(FlushReason reason, GreptimeRowOperation operation, int rows) {
        return new WriteAttemptContext(
                sinkConfig.getWriteMode(),
                sinkConfig.getChangelogMode(),
                operation,
                tableSchema.getTableName(),
                sinkConfig.getDatabase(),
                sinkConfig.getEndpoints().size(),
                rows,
                reason,
                sinkConfig.getBatchMaxRows(),
                sinkConfig.getFlushIntervalMs(),
                sinkConfig.describeWriteHints(),
                sinkConfig.describeWriteSettings());
    }

    private static long elapsedMillis(long startNanos) {
        return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
    }

    static WriteOk requireSuccessfulWrite(Result<WriteOk, Err> result, WriteAttemptContext attempt) throws IOException {
        if (!result.isOk()) {
            Err err = result.getErr();
            Throwable cause = err.getError();
            String errorMessage = cause == null ? "unknown" : cause.getMessage();
            throw new IOException(attempt.writeFailedMessage(err.getCode(), errorMessage), cause);
        }

        WriteOk writeOk = result.getOk();
        if (writeOk.getFailure() > 0) {
            throw new IOException(attempt.partialFailureMessage(writeOk.getSuccess(), writeOk.getFailure()));
        }

        return writeOk;
    }

    private void ensureOpen() throws IOException {
        if (state != State.OPEN) {
            throw new IOException(
                    "GreptimeDB sink writer is not open: " + state.name().toLowerCase());
        }
    }

    private void markFailed() {
        state = State.FAILED;
    }

    private void markFailedAndClearBatch() {
        currentBatch = null;
        currentRows = 0;
        markFailed();
    }

    private enum State {
        OPEN,
        FAILED,
        CLOSED
    }

    private static final class PendingRegularBatch {
        private final GreptimeRowOperation operation;
        private final Table table;
        private final GreptimeRowDataConverter converter;
        private int rows;

        private PendingRegularBatch(GreptimeRowOperation operation, Table table, GreptimeRowDataConverter converter) {
            this.operation = operation;
            this.table = table;
            this.converter = converter;
        }

        private void addRow(RowData row) {
            table.addRow(converter.convert(row));
            rows++;
        }
    }
}
