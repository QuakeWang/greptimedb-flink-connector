package io.greptime.flink.sink;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import io.greptime.BulkStreamWriter;
import io.greptime.flink.cfg.GreptimeBulkWriteConfig;
import io.greptime.flink.cfg.GreptimeSinkConfig;
import io.greptime.flink.cfg.GreptimeWriteMode;
import io.greptime.flink.sink.schema.GreptimeRowDataConverter;
import io.greptime.flink.sink.schema.GreptimeTableSchema;
import io.greptime.models.Table;
import io.greptime.v1.Database;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.common.operators.ProcessingTimeService;
import org.apache.flink.metrics.CharacterFilter;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.metrics.groups.OperatorIOMetricGroup;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.traces.SpanBuilder;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.function.ThrowingRunnable;
import org.junit.jupiter.api.Test;

class GreptimeBulkSinkWriterTest {
    @Test
    void flushesBatchWhenBatchMaxRowsIsReached() throws Exception {
        RecordingBulkWriteClient client = new RecordingBulkWriteClient();
        TestSinkWriterMetricGroup metricGroup = new TestSinkWriterMetricGroup();
        GreptimeBulkSinkWriter<RowData> writer =
                newWriter(client, new TestProcessingTimeService(), new TestMailboxExecutor(), metricGroup, 2, 0L);

        writer.write(row("host-1"), null);
        writer.write(row("host-2"), null);

        assertEquals(List.of(2), client.writeRows);
        assertEquals(List.of(2), client.writeAffectedRows);
        assertEquals(List.of(2048L), client.writeBytes);
        assertEquals(2L, metricGroup.numRecordsSendCount());
        assertEquals(1L, metricGroup.counterValue(GreptimeSinkMetrics.FLUSH_TOTAL));
        assertEquals(1L, metricGroup.counterValue(GreptimeSinkMetrics.FLUSH_SUCCESS_TOTAL));
        assertEquals(2L, metricGroup.counterValue(GreptimeSinkMetrics.FLUSH_ROWS_TOTAL));
        assertEquals(2048L, metricGroup.counterValue(GreptimeBulkSinkMetrics.BULK_FLUSH_BYTES_TOTAL));
        assertEquals(2048L, metricGroup.gaugeValue(GreptimeBulkSinkMetrics.BULK_FLUSH_LAST_BYTES));
        assertEquals(2L, metricGroup.counterValue(GreptimeBulkSinkMetrics.BULK_AFFECTED_ROWS_TOTAL));
        assertEquals(0L, metricGroup.gaugeValue(GreptimeSinkMetrics.BUFFER_ROWS));
    }

    @Test
    void periodicFlushUsesMailboxAndReschedulesTimer() throws Exception {
        RecordingBulkWriteClient client = new RecordingBulkWriteClient();
        TestProcessingTimeService timeService = new TestProcessingTimeService();
        TestMailboxExecutor mailboxExecutor = new TestMailboxExecutor();
        GreptimeBulkSinkWriter<RowData> writer =
                newWriter(client, timeService, mailboxExecutor, new TestSinkWriterMetricGroup(), 100, 1000L);

        writer.write(row("host-1"), null);
        timeService.fireNextTimer();

        assertEquals(List.of(), client.writeRows);
        assertEquals(1, mailboxExecutor.pendingActions());

        mailboxExecutor.drain();

        assertEquals(List.of(1), client.writeRows);
        assertEquals(2000L, timeService.nextTimestamp());
    }

    @Test
    void recordsEmptyFlushWithoutSendingBulkMessage() throws Exception {
        RecordingBulkWriteClient client = new RecordingBulkWriteClient();
        TestSinkWriterMetricGroup metricGroup = new TestSinkWriterMetricGroup();
        GreptimeBulkSinkWriter<RowData> writer =
                newWriter(client, new TestProcessingTimeService(), new TestMailboxExecutor(), metricGroup, 100, 0L);

        writer.flush(false);

        assertEquals(List.of(), client.writeRows);
        assertEquals(0, client.completedCalls);
        assertEquals(0, client.startNewStreamCalls);
        assertEquals(1L, metricGroup.counterValue(GreptimeSinkMetrics.FLUSH_EMPTY_TOTAL));
        assertEquals(1L, metricGroup.counterValue("greptimedb.flush.reason.flink_flush.total"));
        assertEquals(0L, metricGroup.counterValue(GreptimeSinkMetrics.FLUSH_TOTAL));
    }

    @Test
    void doesNotStartBulkStreamUntilFirstValidWrite() throws Exception {
        RecordingBulkWriteClient client = new RecordingBulkWriteClient();
        GreptimeBulkSinkWriter<RowData> writer =
                newWriter(client, new TestProcessingTimeService(), new TestMailboxExecutor(), 100, 0L);

        assertEquals(0, client.startNewStreamCalls);
        assertEquals(0, client.newTableBufferCalls);

        writer.write(row("host-1"), null);

        assertEquals(1, client.startNewStreamCalls);
        assertEquals(1, client.newTableBufferCalls);
    }

    @Test
    void rejectsNonInsertRows() {
        RecordingBulkWriteClient client = new RecordingBulkWriteClient();
        GreptimeBulkSinkWriter<RowData> writer =
                newWriter(client, new TestProcessingTimeService(), new TestMailboxExecutor(), 100, 0L);

        IOException error =
                assertThrows(IOException.class, () -> writer.write(row("host-1", RowKind.UPDATE_AFTER), null));

        assertEquals("GreptimeDB bulk sink only supports INSERT RowKind, but got: UPDATE_AFTER", error.getMessage());
        assertEquals(List.of(), client.writeRows);
    }

    @Test
    void rejectsNullTimeIndexBeforeSendingBatch() {
        RecordingBulkWriteClient client = new RecordingBulkWriteClient();
        GreptimeBulkSinkWriter<RowData> writer =
                newWriter(client, new TestProcessingTimeService(), new TestMailboxExecutor(), 100, 0L);

        IOException error = assertThrows(IOException.class, () -> writer.write(rowWithNullTimestamp("host-1"), null));

        assertEquals(
                "Invalid row for GreptimeDB table metrics: time-index column must not be null: table=metrics, column=ts",
                error.getMessage());
        assertEquals(List.of(), client.writeRows);
    }

    @Test
    void flushEndOfInputFlushesRowsAndCompletesStream() throws Exception {
        RecordingBulkWriteClient client = new RecordingBulkWriteClient();
        TestSinkWriterMetricGroup metricGroup = new TestSinkWriterMetricGroup();
        GreptimeBulkSinkWriter<RowData> writer =
                newWriter(client, new TestProcessingTimeService(), new TestMailboxExecutor(), metricGroup, 100, 0L);

        writer.write(row("host-1"), null);
        writer.flush(true);

        assertEquals(List.of(1), client.writeRows);
        assertEquals(1, client.completedCalls);
        assertEquals(1L, metricGroup.counterValue(GreptimeBulkSinkMetrics.BULK_COMPLETED_TOTAL));
        assertThrows(IOException.class, () -> writer.write(row("host-2"), null));
    }

    @Test
    void checkpointFlushCompletesCurrentStreamAndStartsNextStream() throws Exception {
        RecordingBulkWriteClient client = new RecordingBulkWriteClient();
        TestSinkWriterMetricGroup metricGroup = new TestSinkWriterMetricGroup();
        GreptimeBulkSinkWriter<RowData> writer =
                newWriter(client, new TestProcessingTimeService(), new TestMailboxExecutor(), metricGroup, 100, 0L);

        writer.write(row("host-1"), null);
        writer.flush(false);
        assertEquals(1, client.startNewStreamCalls);

        writer.write(row("host-2"), null);
        writer.flush(false);

        assertEquals(List.of(1, 1), client.writeRows);
        assertEquals(2, client.completedCalls);
        assertEquals(2, client.startNewStreamCalls);
        assertEquals(2L, metricGroup.counterValue(GreptimeBulkSinkMetrics.BULK_COMPLETED_TOTAL));
    }

    @Test
    void checkpointFlushCompletesPreviouslyFlushedStream() throws Exception {
        RecordingBulkWriteClient client = new RecordingBulkWriteClient();
        GreptimeBulkSinkWriter<RowData> writer =
                newWriter(client, new TestProcessingTimeService(), new TestMailboxExecutor(), 1, 0L);

        writer.write(row("host-1"), null);
        writer.flush(false);

        assertEquals(List.of(1), client.writeRows);
        assertEquals(1, client.completedCalls);
        assertEquals(1, client.startNewStreamCalls);
    }

    @Test
    void checkpointFlushSurfacesCompletedFailureWithContextAndMetric() throws Exception {
        RecordingBulkWriteClient client = new RecordingBulkWriteClient();
        client.completedFailure = new IllegalStateException("server completed failed");
        TestSinkWriterMetricGroup metricGroup = new TestSinkWriterMetricGroup();
        GreptimeBulkSinkWriter<RowData> writer =
                newWriter(client, new TestProcessingTimeService(), new TestMailboxExecutor(), metricGroup, 100, 0L);

        writer.write(row("host-1"), null);
        IOException error = assertThrows(IOException.class, () -> writer.flush(false));

        assertTrue(error.getMessage().startsWith("Failed to complete GreptimeDB bulk write stream: writeMode=bulk"));
        assertTrue(error.getMessage().contains("table=metrics"));
        assertTrue(error.getMessage().contains("rows=1"));
        assertTrue(error.getMessage().contains("bytesUsed=1024"));
        assertTrue(error.getMessage().contains("flushReason=flink_flush"));
        assertEquals(1, client.startNewStreamCalls);
        assertEquals(1L, metricGroup.counterValue(GreptimeBulkSinkMetrics.BULK_COMPLETED_FAILURE_TOTAL));
        writer.close();
        assertEquals(1, client.completedCalls);
        assertEquals(1, client.closeStreamCalls);
        assertEquals(1, client.shutdownClientCalls);
    }

    @Test
    void closeFlushesRowsCompletesStreamAndReleasesResources() throws Exception {
        RecordingBulkWriteClient client = new RecordingBulkWriteClient();
        GreptimeBulkSinkWriter<RowData> writer =
                newWriter(client, new TestProcessingTimeService(), new TestMailboxExecutor(), 100, 0L);

        writer.write(row("host-1"), null);
        writer.close();

        assertEquals(List.of(1), client.writeRows);
        assertEquals(1, client.completedCalls);
        assertEquals(1, client.closeStreamCalls);
        assertEquals(1, client.shutdownClientCalls);
    }

    @Test
    void closeDoesNotCloseSdkStreamAgainAfterCompleted() throws Exception {
        CompleteClosesBulkStreamWriter sdkWriter = new CompleteClosesBulkStreamWriter();
        GreptimeDbBulkWriteClient client = new GreptimeDbBulkWriteClient(sdkWriter);
        GreptimeBulkSinkWriter<RowData> writer = newWriter(
                client,
                new TestProcessingTimeService(),
                new TestMailboxExecutor(),
                new TestSinkWriterMetricGroup(),
                100,
                0L,
                GreptimeBulkWriteConfig.defaults());

        writer.write(row("host-1"), null);
        writer.close();

        assertEquals(1, sdkWriter.completedCalls);
        assertEquals(1, sdkWriter.closeCalls);
    }

    @Test
    void checkpointFlushRotatesSdkBulkStream() throws Exception {
        CompleteClosesBulkStreamWriter firstSdkWriter = new CompleteClosesBulkStreamWriter();
        CompleteClosesBulkStreamWriter secondSdkWriter = new CompleteClosesBulkStreamWriter();
        Queue<CompleteClosesBulkStreamWriter> sdkWriters = new ArrayDeque<>();
        sdkWriters.add(firstSdkWriter);
        sdkWriters.add(secondSdkWriter);
        GreptimeDbBulkWriteClient client = new GreptimeDbBulkWriteClient(sdkWriters::remove, () -> {});
        GreptimeBulkSinkWriter<RowData> writer = newWriter(
                client,
                new TestProcessingTimeService(),
                new TestMailboxExecutor(),
                new TestSinkWriterMetricGroup(),
                100,
                0L,
                GreptimeBulkWriteConfig.defaults());

        writer.write(row("host-1"), null);
        writer.flush(false);
        writer.write(row("host-2"), null);
        writer.close();

        assertEquals(1, firstSdkWriter.completedCalls);
        assertEquals(1, firstSdkWriter.closeCalls);
        assertEquals(1, firstSdkWriter.currentTable.rowCount());
        assertEquals(1, secondSdkWriter.completedCalls);
        assertEquals(1, secondSdkWriter.closeCalls);
        assertEquals(1, secondSdkWriter.currentTable.rowCount());
    }

    @Test
    void failedFlushFollowedByCloseOnlyReleasesResources() throws Exception {
        RecordingBulkWriteClient client = new RecordingBulkWriteClient();
        client.nextWriteFuture = failedFuture(new IllegalStateException("network unavailable"));
        GreptimeBulkSinkWriter<RowData> writer =
                newWriter(client, new TestProcessingTimeService(), new TestMailboxExecutor(), 1, 0L);

        IOException error = assertThrows(IOException.class, () -> writer.write(row("host-1"), null));
        writer.close();

        assertTrue(error.getMessage().startsWith("Failed to bulk write rows to GreptimeDB: writeMode=bulk"));
        assertTrue(error.getMessage().contains("table=metrics"));
        assertTrue(error.getMessage().contains("rows=1"));
        assertTrue(error.getMessage().contains("affectedRows=unknown"));
        assertTrue(error.getMessage().contains("bytesUsed=1024"));
        assertTrue(error.getMessage().contains("flushReason=batch_full"));
        assertTrue(error.getMessage().contains("bulkMaxRequestsInFlight=8"));
        assertEquals(List.of(1), client.writeRows);
        assertEquals(0, client.completedCalls);
        assertEquals(1, client.closeStreamCalls);
        assertEquals(1, client.shutdownClientCalls);
    }

    @Test
    void failsWhenAffectedRowsDoNotMatchInputRows() throws Exception {
        RecordingBulkWriteClient client = new RecordingBulkWriteClient();
        client.nextAffectedRows = 1;
        TestSinkWriterMetricGroup metricGroup = new TestSinkWriterMetricGroup();
        GreptimeBulkSinkWriter<RowData> writer =
                newWriter(client, new TestProcessingTimeService(), new TestMailboxExecutor(), metricGroup, 2, 0L);

        writer.write(row("host-1"), null);
        IOException error = assertThrows(IOException.class, () -> writer.write(row("host-2"), null));

        assertTrue(error.getMessage().startsWith("GreptimeDB bulk write affected rows mismatch: writeMode=bulk"));
        assertTrue(error.getMessage().contains("rows=2"));
        assertTrue(error.getMessage().contains("affectedRows=1"));
        assertTrue(error.getMessage().contains("actualAffectedRows=1"));
        assertEquals(2L, metricGroup.numRecordsSendErrorsCount());
        assertEquals(1L, metricGroup.counterValue(GreptimeSinkMetrics.FLUSH_FAILURE_TOTAL));
    }

    @Test
    void rejectsFlushAfterFailureWithoutCompletingStream() throws Exception {
        RecordingBulkWriteClient client = new RecordingBulkWriteClient();
        client.nextAffectedRows = 0;
        GreptimeBulkSinkWriter<RowData> writer =
                newWriter(client, new TestProcessingTimeService(), new TestMailboxExecutor(), 1, 0L);

        assertThrows(IOException.class, () -> writer.write(row("host-1"), null));

        IOException error = assertThrows(IOException.class, () -> writer.flush(false));

        assertEquals("GreptimeDB bulk sink writer is not open: failed", error.getMessage());
        assertEquals(List.of(1), client.writeRows);
        assertEquals(0, client.completedCalls);
    }

    @Test
    void surfacesCompletedFailureWithStreamTotals() throws Exception {
        RecordingBulkWriteClient client = new RecordingBulkWriteClient();
        client.completedFailure = new IllegalStateException("server completed failed");
        GreptimeBulkSinkWriter<RowData> writer =
                newWriter(client, new TestProcessingTimeService(), new TestMailboxExecutor(), 2, 0L);

        writer.write(row("host-1"), null);
        writer.write(row("host-2"), null);
        writer.write(row("host-3"), null);

        IOException error = assertThrows(IOException.class, () -> writer.flush(true));

        assertTrue(error.getMessage().startsWith("Failed to complete GreptimeDB bulk write stream: writeMode=bulk"));
        assertTrue(error.getMessage().contains("rows=3"));
        assertTrue(error.getMessage().contains("bytesUsed=3072"));
        assertEquals(List.of(2, 1), client.writeRows);
        writer.close();
        assertEquals(1, client.completedCalls);
        assertEquals(1, client.closeStreamCalls);
        assertEquals(1, client.shutdownClientCalls);
    }

    @Test
    void recordsStreamNotReadyMetric() throws Exception {
        RecordingBulkWriteClient client = new RecordingBulkWriteClient();
        client.streamReady = false;
        TestSinkWriterMetricGroup metricGroup = new TestSinkWriterMetricGroup();
        GreptimeBulkSinkWriter<RowData> writer =
                newWriter(client, new TestProcessingTimeService(), new TestMailboxExecutor(), metricGroup, 1, 0L);

        writer.write(row("host-1"), null);

        assertEquals(1L, metricGroup.counterValue(GreptimeBulkSinkMetrics.BULK_STREAM_READY_FALSE_TOTAL));
    }

    @Test
    void timesOutWhenWriteNextInvocationBlocks() throws Exception {
        BlockingBulkStreamWriter sdkWriter = new BlockingBulkStreamWriter();
        GreptimeDbBulkWriteClient client = new GreptimeDbBulkWriteClient(sdkWriter);
        TestSinkWriterMetricGroup metricGroup = new TestSinkWriterMetricGroup();
        GreptimeBulkSinkWriter<RowData> writer = newWriter(
                client,
                new TestProcessingTimeService(),
                new TestMailboxExecutor(),
                metricGroup,
                1,
                0L,
                GreptimeBulkWriteConfig.builder().timeoutMsPerMessage(50L).build());

        try {
            long startNanos = System.nanoTime();
            IOException error = assertThrows(IOException.class, () -> writer.write(row("host-1"), null));
            long durationMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);

            assertTrue(durationMs < 2000L, "flush waited for blocked writeNext(), durationMs=" + durationMs);
            assertTrue(error.getMessage().startsWith("Failed to bulk write rows to GreptimeDB: writeMode=bulk"));
            assertTrue(hasCause(error, TimeoutException.class));
            assertEquals(1L, metricGroup.counterValue(GreptimeSinkMetrics.FLUSH_FAILURE_TOTAL));
            assertEquals(1, sdkWriter.closeCalls);
        } finally {
            writer.close();
        }
    }

    @Test
    void timesOutWhenCompletedBlocks() throws Exception {
        BlockingCompletedBulkStreamWriter sdkWriter = new BlockingCompletedBulkStreamWriter();
        GreptimeDbBulkWriteClient client = new GreptimeDbBulkWriteClient(sdkWriter);
        TestSinkWriterMetricGroup metricGroup = new TestSinkWriterMetricGroup();
        GreptimeBulkSinkWriter<RowData> writer = newWriter(
                client,
                new TestProcessingTimeService(),
                new TestMailboxExecutor(),
                metricGroup,
                100,
                0L,
                GreptimeBulkWriteConfig.builder().timeoutMsPerMessage(50L).build());

        try {
            writer.write(row("host-1"), null);

            long startNanos = System.nanoTime();
            IOException error = assertThrows(IOException.class, () -> writer.flush(true));
            long durationMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);

            assertTrue(durationMs < 2000L, "completed waited for blocked SDK completed(), durationMs=" + durationMs);
            assertTrue(
                    error.getMessage().startsWith("Failed to complete GreptimeDB bulk write stream: writeMode=bulk"));
            assertTrue(hasCause(error, TimeoutException.class));
            assertEquals(1L, metricGroup.counterValue(GreptimeBulkSinkMetrics.BULK_COMPLETED_FAILURE_TOTAL));
            assertEquals(1, sdkWriter.completedCalls);
            assertEquals(1, sdkWriter.closeCalls);
        } finally {
            writer.close();
        }
    }

    private static GreptimeBulkSinkWriter<RowData> newWriter(
            RecordingBulkWriteClient client,
            TestProcessingTimeService timeService,
            TestMailboxExecutor mailboxExecutor,
            int batchMaxRows,
            long flushIntervalMs) {
        return newWriter(
                client, timeService, mailboxExecutor, new TestSinkWriterMetricGroup(), batchMaxRows, flushIntervalMs);
    }

    private static GreptimeBulkSinkWriter<RowData> newWriter(
            RecordingBulkWriteClient client,
            TestProcessingTimeService timeService,
            TestMailboxExecutor mailboxExecutor,
            TestSinkWriterMetricGroup metricGroup,
            int batchMaxRows,
            long flushIntervalMs) {
        return newWriter(
                client,
                timeService,
                mailboxExecutor,
                metricGroup,
                batchMaxRows,
                flushIntervalMs,
                GreptimeBulkWriteConfig.defaults());
    }

    private static GreptimeBulkSinkWriter<RowData> newWriter(
            GreptimeBulkWriteClient client,
            TestProcessingTimeService timeService,
            TestMailboxExecutor mailboxExecutor,
            TestSinkWriterMetricGroup metricGroup,
            int batchMaxRows,
            long flushIntervalMs,
            GreptimeBulkWriteConfig bulkWriteConfig) {
        GreptimeTableSchema tableSchema = tableSchema();
        GreptimeSinkConfig sinkConfig = GreptimeSinkConfig.builder()
                .endpoints(List.of("127.0.0.1:4001"))
                .batchMaxRows(batchMaxRows)
                .flushIntervalMs(flushIntervalMs)
                .autoCreateTable(false)
                .writeMode(GreptimeWriteMode.BULK)
                .bulkWriteConfig(bulkWriteConfig)
                .build();
        return new GreptimeBulkSinkWriter<>(
                client,
                sinkConfig,
                tableSchema,
                GreptimeRowDataConverter.forSchema(tableSchema),
                timeService,
                mailboxExecutor,
                metricGroup);
    }

    private static GreptimeTableSchema tableSchema() {
        return GreptimeTableSchema.from(
                "metrics",
                DataTypes.ROW(
                        DataTypes.FIELD("host", DataTypes.STRING()),
                        DataTypes.FIELD("cpu", DataTypes.DOUBLE()),
                        DataTypes.FIELD("ts", DataTypes.TIMESTAMP(3).notNull())),
                "ts",
                List.of("host"));
    }

    private static RowData row(String host) {
        return row(host, RowKind.INSERT);
    }

    private static RowData row(String host, RowKind rowKind) {
        GenericRowData row =
                GenericRowData.of(StringData.fromString(host), 0.5d, TimestampData.fromEpochMillis(1700000000000L));
        row.setRowKind(rowKind);
        return row;
    }

    private static RowData rowWithNullTimestamp(String host) {
        GenericRowData row = GenericRowData.of(StringData.fromString(host), 0.5d, null);
        row.setRowKind(RowKind.INSERT);
        return row;
    }

    private static <T> CompletableFuture<T> failedFuture(Throwable cause) {
        CompletableFuture<T> future = new CompletableFuture<>();
        future.completeExceptionally(cause);
        return future;
    }

    private static boolean hasCause(Throwable error, Class<? extends Throwable> expectedType) {
        Throwable current = error;
        while (current != null) {
            if (expectedType.isInstance(current)) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    private static final class RecordingBulkWriteClient implements GreptimeBulkWriteClient {
        private final List<Integer> writeRows = new ArrayList<>();
        private final List<Integer> writeAffectedRows = new ArrayList<>();
        private final List<Long> writeBytes = new ArrayList<>();
        private RecordingTable currentTable;
        private CompletableFuture<Integer> nextWriteFuture;
        private Integer nextAffectedRows;
        private RuntimeException completedFailure;
        private boolean streamReady = true;
        private boolean streamOpen;
        private int newTableBufferCalls;
        private int completedCalls;
        private int startNewStreamCalls;
        private int closeStreamCalls;
        private int shutdownClientCalls;

        @Override
        public Table.TableBufferRoot newTableBuffer(int columnBufferSize) {
            if (!streamOpen) {
                throw new IllegalStateException("stream is not open");
            }
            newTableBufferCalls++;
            currentTable = new RecordingTable("metrics");
            return currentTable;
        }

        @Override
        public int writeNext(long timeout, TimeUnit unit) throws Exception {
            writeRows.add(currentTable.rowCount());
            writeBytes.add(currentTable.bytesUsed());
            CompletableFuture<Integer> future;
            if (nextWriteFuture != null) {
                future = nextWriteFuture;
                nextWriteFuture = null;
            } else {
                int affectedRows = nextAffectedRows == null ? currentTable.rowCount() : nextAffectedRows;
                writeAffectedRows.add(affectedRows);
                nextAffectedRows = null;
                future = CompletableFuture.completedFuture(affectedRows);
            }
            return future.get(timeout, unit);
        }

        @Override
        public void completed(long timeout, TimeUnit unit) {
            completedCalls++;
            if (completedFailure != null) {
                throw completedFailure;
            }
            streamOpen = false;
        }

        @Override
        public void startNewStream(long timeout, TimeUnit unit) {
            if (streamOpen) {
                throw new IllegalStateException("stream is already open");
            }
            startNewStreamCalls++;
            streamOpen = true;
        }

        @Override
        public boolean isStreamReady() {
            return streamReady;
        }

        @Override
        public void closeStream() {
            closeStreamCalls++;
            streamOpen = false;
        }

        @Override
        public void shutdownClient() {
            shutdownClientCalls++;
        }
    }

    private static final class CompleteClosesBulkStreamWriter implements BulkStreamWriter {
        private RecordingTable currentTable;
        private int completedCalls;
        private int closeCalls;
        private boolean streamClosed;

        @Override
        public Table.TableBufferRoot tableBufferRoot(int columnBufferSize) {
            currentTable = new RecordingTable("metrics");
            return currentTable;
        }

        @Override
        public CompletableFuture<Integer> writeNext() {
            return CompletableFuture.completedFuture(currentTable.rowCount());
        }

        @Override
        public void completed() throws Exception {
            completedCalls++;
            close();
        }

        @Override
        public void close() throws IOException {
            closeCalls++;
            if (streamClosed) {
                throw new IOException("stream already closed");
            }
            streamClosed = true;
        }
    }

    private static final class BlockingBulkStreamWriter implements BulkStreamWriter {
        private static final long MAX_BLOCK_MS = 5000L;

        private final CountDownLatch releaseWriteNext = new CountDownLatch(1);
        private RecordingTable currentTable;
        private int closeCalls;

        @Override
        public Table.TableBufferRoot tableBufferRoot(int columnBufferSize) {
            currentTable = new RecordingTable("metrics");
            return currentTable;
        }

        @Override
        public CompletableFuture<Integer> writeNext() {
            long deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(MAX_BLOCK_MS);
            boolean interrupted = false;
            while (System.nanoTime() < deadlineNanos) {
                try {
                    if (releaseWriteNext.await(10L, TimeUnit.MILLISECONDS)) {
                        break;
                    }
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
            return CompletableFuture.completedFuture(currentTable.rowCount());
        }

        @Override
        public void completed() {}

        @Override
        public boolean isStreamReady() {
            return true;
        }

        @Override
        public void close() {
            closeCalls++;
            releaseWriteNext.countDown();
        }
    }

    private static final class BlockingCompletedBulkStreamWriter implements BulkStreamWriter {
        private static final long MAX_BLOCK_MS = 5000L;

        private final CountDownLatch releaseCompleted = new CountDownLatch(1);
        private RecordingTable currentTable;
        private int completedCalls;
        private int closeCalls;

        @Override
        public Table.TableBufferRoot tableBufferRoot(int columnBufferSize) {
            currentTable = new RecordingTable("metrics");
            return currentTable;
        }

        @Override
        public CompletableFuture<Integer> writeNext() {
            return CompletableFuture.completedFuture(currentTable.rowCount());
        }

        @Override
        public void completed() {
            completedCalls++;
            long deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(MAX_BLOCK_MS);
            boolean interrupted = false;
            while (System.nanoTime() < deadlineNanos) {
                try {
                    if (releaseCompleted.await(10L, TimeUnit.MILLISECONDS)) {
                        break;
                    }
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }

        @Override
        public void close() {
            closeCalls++;
            releaseCompleted.countDown();
        }
    }

    private static final class RecordingTable implements Table.TableBufferRoot {
        private final String tableName;
        private int rows;
        private boolean completed;

        private RecordingTable(String tableName) {
            this.tableName = tableName;
        }

        @Override
        public String tableName() {
            return tableName;
        }

        @Override
        public int rowCount() {
            return rows;
        }

        @Override
        public int columnCount() {
            return 3;
        }

        @Override
        public long bytesUsed() {
            return rows * 1024L;
        }

        @Override
        public Table addRow(Object... values) {
            checkNumValues(values.length);
            rows++;
            return this;
        }

        @Override
        public Table subRange(int from, int to) {
            throw new UnsupportedOperationException("subRange is not used by bulk writer tests");
        }

        @Override
        public Table complete() {
            completed = true;
            return this;
        }

        @Override
        public boolean isCompleted() {
            return completed;
        }

        @Override
        public Database.RowInsertRequest intoRowInsertRequest() {
            throw new UnsupportedOperationException("Row insert request is not used by bulk writer tests");
        }

        @Override
        public Database.RowDeleteRequest intoRowDeleteRequest() {
            throw new UnsupportedOperationException("Row delete request is not used by bulk writer tests");
        }
    }

    private static final class TestSinkWriterMetricGroup implements SinkWriterMetricGroup {
        private final Map<String, Counter> counters = new HashMap<>();
        private final Map<String, Gauge<?>> gauges = new HashMap<>();
        private final Counter numRecordsOutErrors = new SimpleCounter();
        private final Counter numRecordsSendErrors = new SimpleCounter();
        private final Counter numRecordsSend = new SimpleCounter();
        private final Counter numBytesSend = new SimpleCounter();
        private Gauge<Long> currentSendTimeGauge;

        @Override
        public OperatorIOMetricGroup getIOMetricGroup() {
            return null;
        }

        @Override
        public Counter getNumRecordsOutErrorsCounter() {
            return numRecordsOutErrors;
        }

        @Override
        public Counter getNumRecordsSendErrorsCounter() {
            return numRecordsSendErrors;
        }

        @Override
        public Counter getNumRecordsSendCounter() {
            return numRecordsSend;
        }

        @Override
        public Counter getNumBytesSendCounter() {
            return numBytesSend;
        }

        @Override
        public void setCurrentSendTimeGauge(Gauge<Long> currentSendTimeGauge) {
            this.currentSendTimeGauge = currentSendTimeGauge;
        }

        @Override
        public void addSpan(SpanBuilder spanBuilder) {}

        @Override
        public Counter counter(String name) {
            return counters.computeIfAbsent(name, ignored -> new SimpleCounter());
        }

        @Override
        public <C extends Counter> C counter(String name, C counter) {
            counters.put(name, counter);
            return counter;
        }

        @Override
        public <T, G extends Gauge<T>> G gauge(String name, G gauge) {
            gauges.put(name, gauge);
            return gauge;
        }

        @Override
        public <H extends Histogram> H histogram(String name, H histogram) {
            return histogram;
        }

        @Override
        public <M extends Meter> M meter(String name, M meter) {
            return meter;
        }

        @Override
        public MetricGroup addGroup(String name) {
            return this;
        }

        @Override
        public MetricGroup addGroup(String key, String value) {
            return this;
        }

        @Override
        public String[] getScopeComponents() {
            return new String[0];
        }

        @Override
        public Map<String, String> getAllVariables() {
            return Collections.emptyMap();
        }

        @Override
        public String getMetricIdentifier(String metricName) {
            return metricName;
        }

        @Override
        public String getMetricIdentifier(String metricName, CharacterFilter filter) {
            return metricName;
        }

        private long counterValue(String name) {
            Counter counter = counters.get(name);
            return counter == null ? 0L : counter.getCount();
        }

        private long numRecordsSendCount() {
            return numRecordsSend.getCount();
        }

        private long numRecordsSendErrorsCount() {
            return numRecordsSendErrors.getCount();
        }

        private long gaugeValue(String name) {
            Gauge<?> gauge = gauges.get(name);
            if (gauge == null) {
                return 0L;
            }
            return ((Number) gauge.getValue()).longValue();
        }
    }

    private static final class TestProcessingTimeService implements ProcessingTimeService {
        private long currentProcessingTime;
        private TestScheduledFuture nextTimer;

        @Override
        public long getCurrentProcessingTime() {
            return currentProcessingTime;
        }

        @Override
        public ScheduledFuture<?> registerTimer(long timestamp, ProcessingTimeCallback target) {
            nextTimer = new TestScheduledFuture(timestamp, target);
            return nextTimer;
        }

        private long nextTimestamp() {
            return nextTimer.timestamp;
        }

        private void fireNextTimer() throws Exception {
            TestScheduledFuture timer = nextTimer;
            nextTimer = null;
            currentProcessingTime = timer.timestamp;
            timer.fire();
        }
    }

    private static final class TestScheduledFuture implements ScheduledFuture<Void> {
        private final long timestamp;
        private final ProcessingTimeService.ProcessingTimeCallback callback;
        private boolean cancelled;
        private boolean done;

        private TestScheduledFuture(long timestamp, ProcessingTimeService.ProcessingTimeCallback callback) {
            this.timestamp = timestamp;
            this.callback = callback;
        }

        private void fire() throws Exception {
            if (cancelled) {
                return;
            }
            done = true;
            callback.onProcessingTime(timestamp);
        }

        @Override
        public long getDelay(TimeUnit unit) {
            return unit.convert(timestamp, TimeUnit.MILLISECONDS);
        }

        @Override
        public int compareTo(Delayed other) {
            return Long.compare(getDelay(TimeUnit.MILLISECONDS), other.getDelay(TimeUnit.MILLISECONDS));
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            if (done) {
                return false;
            }
            cancelled = true;
            done = true;
            return true;
        }

        @Override
        public boolean isCancelled() {
            return cancelled;
        }

        @Override
        public boolean isDone() {
            return done;
        }

        @Override
        public Void get() throws InterruptedException, ExecutionException {
            return null;
        }

        @Override
        public Void get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
            return null;
        }
    }

    private static final class TestMailboxExecutor implements MailboxExecutor {
        private final Queue<ThrowingRunnable<? extends Exception>> actions = new ArrayDeque<>();

        @Override
        public void execute(
                MailOptions mailOptions,
                ThrowingRunnable<? extends Exception> command,
                String descriptionFormat,
                Object... descriptionArgs) {
            actions.add(command);
        }

        @Override
        public void yield() throws InterruptedException, FlinkRuntimeException {}

        @Override
        public boolean tryYield() throws FlinkRuntimeException {
            return false;
        }

        @Override
        public boolean shouldInterrupt() {
            return false;
        }

        private int pendingActions() {
            return actions.size();
        }

        private void drain() throws Exception {
            ThrowingRunnable<? extends Exception> action;
            while ((action = actions.poll()) != null) {
                action.run();
            }
        }
    }
}
