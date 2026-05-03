package io.greptime.flink.sink;

import io.greptime.flink.cfg.GreptimeBulkWriteConfig;
import io.greptime.flink.cfg.GreptimeSinkConfig;
import io.greptime.flink.sink.schema.GreptimeRowDataConverter;
import io.greptime.flink.sink.schema.GreptimeTableSchema;
import io.greptime.models.Table;
import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.common.operators.ProcessingTimeService;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class GreptimeBulkSinkWriter<IN extends RowData> implements SinkWriter<IN> {
    private static final Logger LOG = LoggerFactory.getLogger(GreptimeBulkSinkWriter.class);

    private final GreptimeBulkWriteClient client;
    private final GreptimeSinkConfig sinkConfig;
    private final GreptimeBulkWriteConfig bulkWriteConfig;
    private final GreptimeTableSchema tableSchema;
    private final GreptimeRowDataConverter converter;
    private final ProcessingTimeService processingTimeService;
    private final MailboxExecutor mailboxExecutor;
    private final GreptimeBulkSinkMetrics metrics;

    private Table.TableBufferRoot currentTable;
    private volatile int currentRows;
    private long streamRows;
    private long streamBytesUsed;
    private ScheduledFuture<?> periodicFlushTimer;
    private volatile State state = State.OPEN;

    GreptimeBulkSinkWriter(
            GreptimeBulkWriteClient client,
            GreptimeSinkConfig sinkConfig,
            GreptimeTableSchema tableSchema,
            GreptimeRowDataConverter converter,
            ProcessingTimeService processingTimeService,
            MailboxExecutor mailboxExecutor,
            SinkWriterMetricGroup metricGroup) {
        this.client = Objects.requireNonNull(client, "client");
        this.sinkConfig = Objects.requireNonNull(sinkConfig, "sinkConfig");
        this.bulkWriteConfig = sinkConfig.getBulkWriteConfig();
        this.tableSchema = Objects.requireNonNull(tableSchema, "tableSchema");
        this.converter = Objects.requireNonNull(converter, "converter");
        this.processingTimeService = processingTimeService;
        this.mailboxExecutor = mailboxExecutor;
        this.metrics = new GreptimeBulkSinkMetrics(metricGroup, () -> currentRows);
        if (sinkConfig.isFlushIntervalEnabled()) {
            Objects.requireNonNull(processingTimeService, "processingTimeService");
            Objects.requireNonNull(mailboxExecutor, "mailboxExecutor");
            scheduleNextPeriodicFlush();
        }
    }

    @Override
    public void write(IN element, Context context) throws IOException, InterruptedException {
        ensureOpenForWrite();
        if (element.getRowKind() != RowKind.INSERT) {
            markFailedAndClearBuffer();
            throw new IOException(
                    "GreptimeDB bulk sink only supports INSERT RowKind, but got: " + element.getRowKind());
        }
        Object[] values;
        try {
            values = converter.convert(element);
        } catch (IllegalArgumentException e) {
            markFailedAndClearBuffer();
            throw new IOException(
                    "Invalid row for GreptimeDB table " + tableSchema.getTableName() + ": " + e.getMessage(), e);
        }
        ensureCurrentTable();
        currentTable.addRow(values);
        currentRows++;
        if (currentRows >= sinkConfig.getBatchMaxRows()) {
            flushInternal(FlushReason.BATCH_FULL, true);
        }
    }

    @Override
    public void flush(boolean endOfInput) throws IOException, InterruptedException {
        if (state != State.OPEN) {
            if (state == State.FAILED) {
                throw notOpenException();
            }
            return;
        }
        if (endOfInput) {
            completeStream(FlushReason.END_OF_INPUT);
        } else {
            checkpointStream(FlushReason.FLINK_FLUSH);
        }
    }

    @Override
    public void close() throws Exception {
        if (state == State.CLOSED) {
            return;
        }

        Exception failure = null;
        try {
            if (state == State.OPEN) {
                completeStream(FlushReason.CLOSE);
            }
        } catch (Exception e) {
            failure = e;
        }

        Exception cleanupFailure = releaseResources();
        if (failure != null) {
            if (cleanupFailure != null) {
                failure.addSuppressed(cleanupFailure);
            }
            throw failure;
        }
        if (cleanupFailure != null) {
            throw cleanupFailure;
        }
    }

    private Table.TableBufferRoot newTableBuffer() {
        return client.newTableBuffer(bulkWriteConfig.getColumnBufferSize());
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
                    flushInternal(FlushReason.PERIODIC, true);
                    scheduleNextPeriodicFlush();
                },
                "GreptimeDB bulk periodic flush");
    }

    private void cancelPeriodicFlushTimer() {
        if (periodicFlushTimer != null) {
            periodicFlushTimer.cancel(false);
            periodicFlushTimer = null;
        }
    }

    private void completeStream(FlushReason reason) throws IOException, InterruptedException {
        cancelPeriodicFlushTimer();
        flushInternal(reason, false);
        if (streamRows == 0L) {
            state = State.COMPLETED;
            return;
        }

        BulkWriteAttemptContext attempt = newCompletedAttemptContext(reason);
        completeClientStream(attempt);
        state = State.COMPLETED;
    }

    private void checkpointStream(FlushReason reason) throws IOException, InterruptedException {
        flushInternal(reason, false);
        if (streamRows == 0L) {
            return;
        }

        BulkWriteAttemptContext attempt = newCompletedAttemptContext(reason);
        completeClientStream(attempt);
        resetCompletedStream();
    }

    private void completeClientStream(BulkWriteAttemptContext attempt) throws IOException, InterruptedException {
        try {
            client.completed(bulkWriteConfig.getTimeoutMsPerMessage(), TimeUnit.MILLISECONDS);
            metrics.recordCompletedSuccess();
        } catch (IOException e) {
            markFailed();
            metrics.recordCompletedFailure();
            throw new IOException(attempt.failureMessage("Failed to complete GreptimeDB bulk write stream"), e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            markFailed();
            metrics.recordCompletedFailure();
            throw e;
        } catch (Exception e) {
            markFailed();
            metrics.recordCompletedFailure();
            throw new IOException(attempt.failureMessage("Failed to complete GreptimeDB bulk write stream"), e);
        }
    }

    private void ensureCurrentTable() throws IOException, InterruptedException {
        if (currentTable != null) {
            return;
        }
        try {
            if (streamRows == 0L) {
                client.startNewStream(bulkWriteConfig.getTimeoutMsPerMessage(), TimeUnit.MILLISECONDS);
            }
            currentTable = newTableBuffer();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            markFailed();
            throw e;
        } catch (Exception e) {
            markFailed();
            throw new IOException(streamFailureMessage("Failed to start GreptimeDB bulk write stream"), e);
        }
    }

    private void flushInternal(FlushReason reason, boolean createNextBuffer) throws IOException, InterruptedException {
        int rows = currentRows;
        if (rows == 0) {
            metrics.recordEmptyFlush(reason);
            return;
        }

        long startNanos = System.nanoTime();
        long bytesUsed = 0L;
        Integer affectedRows = null;
        try {
            currentTable.complete();
            bytesUsed = currentTable.bytesUsed();
            if (!client.isStreamReady()) {
                metrics.recordStreamReadyFalse();
            }
            affectedRows = client.writeNext(bulkWriteConfig.getTimeoutMsPerMessage(), TimeUnit.MILLISECONDS);
            // The SDK reports one affected row per accepted input row; a mismatch would hide data loss.
            if (affectedRows != rows) {
                throw new IOException(newAttemptContext(reason, rows, affectedRows, bytesUsed)
                        .affectedRowsMismatchMessage(affectedRows));
            }

            long durationMs = elapsedMillis(startNanos);
            metrics.recordFlushSuccess(reason, rows, durationMs, bytesUsed, affectedRows);
            streamRows += rows;
            streamBytesUsed += bytesUsed;
            LOG.debug(
                    "Bulk wrote {} rows into GreptimeDB table {} (reason={}, durationMs={}, bytesUsed={})",
                    rows,
                    tableSchema.getTableName(),
                    reason.metricName(),
                    durationMs,
                    bytesUsed);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            markFlushFailure(reason, rows, startNanos);
            throw e;
        } catch (IOException e) {
            markFlushFailure(reason, rows, startNanos);
            throw e;
        } catch (Exception e) {
            markFlushFailure(reason, rows, startNanos);
            throw new IOException(
                    newAttemptContext(reason, rows, affectedRows, bytesUsed)
                            .failureMessage("Failed to bulk write rows to GreptimeDB"),
                    e);
        }

        currentRows = 0;
        if (createNextBuffer) {
            currentTable = null;
        }
    }

    private void resetCompletedStream() {
        currentTable = null;
        streamRows = 0L;
        streamBytesUsed = 0L;
    }

    private void markFlushFailure(FlushReason reason, int rows, long startNanos) {
        markFailed();
        metrics.recordFlushFailure(reason, rows, elapsedMillis(startNanos));
    }

    private void markFailed() {
        state = State.FAILED;
    }

    private void markFailedAndClearBuffer() {
        currentTable = null;
        currentRows = 0;
        markFailed();
    }

    private Exception releaseResources() {
        cancelPeriodicFlushTimer();
        Exception failure = null;
        try {
            client.closeStream();
        } catch (Exception e) {
            failure = e;
        }
        try {
            client.shutdownClient();
        } catch (RuntimeException e) {
            if (failure == null) {
                failure = e;
            } else {
                failure.addSuppressed(e);
            }
        } finally {
            state = State.CLOSED;
        }
        return failure;
    }

    private void ensureOpenForWrite() throws IOException {
        if (state != State.OPEN) {
            throw notOpenException();
        }
    }

    private IOException notOpenException() {
        return new IOException(
                "GreptimeDB bulk sink writer is not open: " + state.name().toLowerCase());
    }

    private BulkWriteAttemptContext newAttemptContext(
            FlushReason reason, long rows, Integer affectedRows, long bytesUsed) {
        return new BulkWriteAttemptContext(
                tableSchema.getTableName(),
                sinkConfig.getDatabase(),
                sinkConfig.getEndpoints().size(),
                rows,
                affectedRows,
                bytesUsed,
                reason,
                sinkConfig.getBatchMaxRows(),
                sinkConfig.getFlushIntervalMs(),
                sinkConfig.getBulkWriteConfig().describe());
    }

    private BulkWriteAttemptContext newCompletedAttemptContext(FlushReason reason) {
        return newAttemptContext(reason, streamRows, null, streamBytesUsed);
    }

    private String streamFailureMessage(String prefix) {
        return prefix
                + ": writeMode=bulk"
                + ", table="
                + tableSchema.getTableName()
                + ", database="
                + sinkConfig.getDatabase()
                + ", endpoints="
                + sinkConfig.getEndpoints().size()
                + ", "
                + sinkConfig.getBulkWriteConfig().describe();
    }

    private static long elapsedMillis(long startNanos) {
        return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
    }

    private enum State {
        OPEN,
        FAILED,
        COMPLETED,
        CLOSED
    }
}
