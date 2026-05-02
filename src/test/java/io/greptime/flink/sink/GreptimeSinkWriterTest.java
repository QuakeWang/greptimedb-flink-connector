package io.greptime.flink.sink;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import io.greptime.WriteOp;
import io.greptime.flink.cfg.GreptimeChangelogMode;
import io.greptime.flink.cfg.GreptimeSinkConfig;
import io.greptime.flink.cfg.GreptimeWriteMode;
import io.greptime.flink.sink.schema.GreptimeRowDataConverter;
import io.greptime.flink.sink.schema.GreptimeTableSchema;
import io.greptime.models.Err;
import io.greptime.models.Result;
import io.greptime.models.Table;
import io.greptime.models.WriteOk;
import io.greptime.rpc.Context;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class GreptimeSinkWriterTest {
    private static final String DEFAULT_WRITE_SETTINGS =
            "writeMaxRetries=1,writeMaxInFlightPoints=655360,writeLimitPolicy=abort-on-blocking-timeout,writeLimitTimeoutMs=3000,writeCompression=none,rpcTimeoutMs=60000,routeRefreshPeriodSeconds=600,routeHealthTimeoutMs=1000";

    @Test
    void flushesPartialBatchOnInterval() throws Exception {
        RecordingWriteClient client = new RecordingWriteClient();
        TestProcessingTimeService timeService = new TestProcessingTimeService();
        TestMailboxExecutor mailboxExecutor = new TestMailboxExecutor();
        GreptimeSinkWriter<RowData> writer = newWriter(client, timeService, mailboxExecutor, 100, 1000L);

        writer.write(row("host-1"), null);
        timeService.fireNextTimer();

        assertEquals(List.of(), client.rowCounts);
        assertEquals(1, mailboxExecutor.pendingActions());

        mailboxExecutor.drain();

        assertEquals(List.of(1), client.rowCounts);
        assertEquals(2000L, timeService.nextTimestamp());
    }

    @Test
    void doesNotFlushEmptyBufferOnInterval() throws Exception {
        RecordingWriteClient client = new RecordingWriteClient();
        TestProcessingTimeService timeService = new TestProcessingTimeService();
        TestMailboxExecutor mailboxExecutor = new TestMailboxExecutor();
        TestSinkWriterMetricGroup metricGroup = new TestSinkWriterMetricGroup();
        newWriter(client, timeService, mailboxExecutor, metricGroup, 100, 1000L);

        timeService.fireNextTimer();
        mailboxExecutor.drain();

        assertEquals(List.of(), client.rowCounts);
        assertEquals(2000L, timeService.nextTimestamp());
        assertEquals(1L, metricGroup.counterValue(GreptimeSinkMetrics.FLUSH_EMPTY_TOTAL));
        assertEquals(1L, metricGroup.counterValue("greptimedb.flush.reason.periodic.total"));
        assertEquals(0L, metricGroup.counterValue(GreptimeSinkMetrics.FLUSH_TOTAL));
    }

    @Test
    void keepsBatchMaxRowsFlushBehavior() throws Exception {
        RecordingWriteClient client = new RecordingWriteClient();
        TestProcessingTimeService timeService = new TestProcessingTimeService();
        TestMailboxExecutor mailboxExecutor = new TestMailboxExecutor();
        TestSinkWriterMetricGroup metricGroup = new TestSinkWriterMetricGroup();
        GreptimeSinkWriter<RowData> writer = newWriter(client, timeService, mailboxExecutor, metricGroup, 2, 1000L);

        writer.write(row("host-1"), null);
        writer.write(row("host-2"), null);

        assertEquals(List.of(2), client.rowCounts);
        assertEquals(2L, metricGroup.numRecordsSendCount());
        assertEquals(1L, metricGroup.counterValue(GreptimeSinkMetrics.FLUSH_TOTAL));
        assertEquals(1L, metricGroup.counterValue(GreptimeSinkMetrics.FLUSH_SUCCESS_TOTAL));
        assertEquals(2L, metricGroup.counterValue(GreptimeSinkMetrics.FLUSH_ROWS_TOTAL));
        assertEquals(1L, metricGroup.counterValue("greptimedb.flush.reason.batch_full.total"));
        assertEquals(2L, metricGroup.gaugeValue(GreptimeSinkMetrics.FLUSH_LAST_ROWS));
        assertEquals(0L, metricGroup.gaugeValue(GreptimeSinkMetrics.BUFFER_ROWS));
        assertTrue(metricGroup.currentSendTime() >= 0L);

        timeService.fireNextTimer();
        mailboxExecutor.drain();

        assertEquals(List.of(2), client.rowCounts);
    }

    @Test
    void createsFreshWriteContextForEveryFlush() throws Exception {
        RecordingWriteClient client = new RecordingWriteClient();
        GreptimeSinkWriter<RowData> writer =
                newWriter(client, new TestProcessingTimeService(), new TestMailboxExecutor(), 1, 0L);

        writer.write(row("host-1"), null);
        writer.write(row("host-2"), null);

        assertEquals(2, client.contexts.size());
        assertNotSame(client.contexts.get(0), client.contexts.get(1));
    }

    @Test
    void closeCancelsTimerAndFlushesRemainingRows() throws Exception {
        RecordingWriteClient client = new RecordingWriteClient();
        TestProcessingTimeService timeService = new TestProcessingTimeService();
        TestMailboxExecutor mailboxExecutor = new TestMailboxExecutor();
        TestSinkWriterMetricGroup metricGroup = new TestSinkWriterMetricGroup();
        GreptimeSinkWriter<RowData> writer = newWriter(client, timeService, mailboxExecutor, metricGroup, 100, 1000L);

        writer.write(row("host-1"), null);
        timeService.fireNextTimer();
        writer.close();
        mailboxExecutor.drain();

        assertEquals(List.of(1), client.rowCounts);
        assertEquals(true, client.shutdown);
    }

    @ParameterizedTest
    @EnumSource(
            value = RowKind.class,
            names = {"UPDATE_BEFORE", "UPDATE_AFTER", "DELETE"})
    void rejectsNonInsertRows(RowKind rowKind) {
        RecordingWriteClient client = new RecordingWriteClient();
        GreptimeSinkWriter<RowData> writer =
                newWriter(client, new TestProcessingTimeService(), new TestMailboxExecutor(), 100, 0L);

        IOException error = assertThrows(IOException.class, () -> writer.write(row("host-1", rowKind), null));

        assertEquals("GreptimeDB sink only supports INSERT RowKind, but got: " + rowKind, error.getMessage());
        assertEquals(List.of(), client.rowCounts);
    }

    @Test
    void mapsRetractRowsAndFlushesWhenOperationChanges() throws Exception {
        RecordingWriteClient client = new RecordingWriteClient();
        TestSinkWriterMetricGroup metricGroup = new TestSinkWriterMetricGroup();
        GreptimeSinkWriter<RowData> writer = newWriter(
                client,
                new TestProcessingTimeService(),
                new TestMailboxExecutor(),
                metricGroup,
                100,
                0L,
                GreptimeSinkConfig.DEFAULT_RPC_TIMEOUT_MS,
                GreptimeChangelogMode.RETRACT);

        writer.write(row("host-1", RowKind.INSERT), null);
        writer.write(row("host-1", RowKind.DELETE), null);
        writer.write(row("host-2", RowKind.UPDATE_BEFORE), null);
        writer.write(row("host-2", RowKind.UPDATE_AFTER), null);
        writer.flush(false);

        assertEquals(List.of(1, 2, 1), client.rowCounts);
        assertEquals(List.of(WriteOp.Insert, WriteOp.Delete, WriteOp.Insert), client.writeOps);
        assertEquals(List.of(3, 2, 3), client.columnCounts);
        assertEquals(2L, metricGroup.counterValue("greptimedb.flush.reason.operation_changed.total"));
        assertEquals(2L, metricGroup.counterValue("greptimedb.write.operation.insert.rows.total"));
        assertEquals(2L, metricGroup.counterValue("greptimedb.write.operation.delete.rows.total"));
    }

    @Test
    void rejectsNullDeleteKeyBeforeSendingBatch() {
        RecordingWriteClient client = new RecordingWriteClient();
        GreptimeSinkWriter<RowData> writer = newWriter(
                client,
                new TestProcessingTimeService(),
                new TestMailboxExecutor(),
                new TestSinkWriterMetricGroup(),
                100,
                0L,
                GreptimeSinkConfig.DEFAULT_RPC_TIMEOUT_MS,
                GreptimeChangelogMode.RETRACT);

        IOException error = assertThrows(IOException.class, () -> writer.write(rowWithNullHost(RowKind.DELETE), null));

        assertEquals(
                "Invalid row for GreptimeDB table metrics: row key column must not be null: table=metrics, column=host",
                error.getMessage());
        assertEquals(List.of(), client.rowCounts);
    }

    @Test
    void failsWriterAfterInvalidDeleteKey() {
        RecordingWriteClient client = new RecordingWriteClient();
        GreptimeSinkWriter<RowData> writer = newWriter(
                client,
                new TestProcessingTimeService(),
                new TestMailboxExecutor(),
                new TestSinkWriterMetricGroup(),
                100,
                0L,
                GreptimeSinkConfig.DEFAULT_RPC_TIMEOUT_MS,
                GreptimeChangelogMode.RETRACT);

        assertThrows(IOException.class, () -> writer.write(rowWithNullHost(RowKind.DELETE), null));

        IOException writeError = assertThrows(IOException.class, () -> writer.write(row("host-1"), null));
        IOException flushError = assertThrows(IOException.class, () -> writer.flush(false));

        assertEquals("GreptimeDB sink writer is not open: failed", writeError.getMessage());
        assertEquals("GreptimeDB sink writer is not open: failed", flushError.getMessage());
        assertEquals(List.of(), client.rowCounts);
    }

    @Test
    void rejectsNullTimeIndexBeforeSendingBatch() {
        RecordingWriteClient client = new RecordingWriteClient();
        GreptimeSinkWriter<RowData> writer =
                newWriter(client, new TestProcessingTimeService(), new TestMailboxExecutor(), 100, 0L);

        IOException error = assertThrows(IOException.class, () -> writer.write(rowWithNullTimestamp("host-1"), null));

        assertEquals(
                "Invalid row for GreptimeDB table metrics: time-index column must not be null: table=metrics, column=ts",
                error.getMessage());
        assertEquals(List.of(), client.rowCounts);
    }

    @Test
    void rejectsErrResult() {
        IllegalStateException cause = new IllegalStateException("rate limited");
        Result<WriteOk, Err> result = Err.writeErr(503, cause, null).mapToResult();

        IOException error =
                assertThrows(IOException.class, () -> GreptimeSinkWriter.requireSuccessfulWrite(result, attempt(3)));

        assertEquals(
                "GreptimeDB write failed: writeMode=regular, changelogMode=insert-only, writeOp=insert, table=metrics, database=public, rows=3, flushReason=batch_full, batchMaxRows=2, flushIntervalMs=500, endpoints=1, writeHints=auto_create_table=true, writeSettings="
                        + DEFAULT_WRITE_SETTINGS
                        + ", code=503, error=rate limited",
                error.getMessage());
        assertSame(cause, error.getCause());
    }

    @Test
    void rejectsPartialFailure() {
        Result<WriteOk, Err> result = Result.ok(WriteOk.ok(2, 1));

        IOException error =
                assertThrows(IOException.class, () -> GreptimeSinkWriter.requireSuccessfulWrite(result, attempt(3)));

        assertEquals(
                "GreptimeDB write partially failed: writeMode=regular, changelogMode=insert-only, writeOp=insert, table=metrics, database=public, rows=3, flushReason=batch_full, batchMaxRows=2, flushIntervalMs=500, endpoints=1, writeHints=auto_create_table=true, writeSettings="
                        + DEFAULT_WRITE_SETTINGS
                        + ", success=2, failure=1",
                error.getMessage());
    }

    @Test
    void acceptsAffectedRowsDifferentFromInputRowsWhenNoFailures() throws Exception {
        WriteOk ok = WriteOk.ok(0, 0);

        WriteOk returned = GreptimeSinkWriter.requireSuccessfulWrite(Result.ok(ok), attempt(3));

        assertSame(ok, returned);
    }

    @Test
    void recordsFailureMetricsAndContextWhenWriteResultFails() {
        RecordingWriteClient client = new RecordingWriteClient();
        client.nextResult = Err.writeErr(503, new IllegalStateException("rate limited"), null)
                .mapToResult();
        TestProcessingTimeService timeService = new TestProcessingTimeService();
        TestMailboxExecutor mailboxExecutor = new TestMailboxExecutor();
        TestSinkWriterMetricGroup metricGroup = new TestSinkWriterMetricGroup();
        GreptimeSinkWriter<RowData> writer = newWriter(client, timeService, mailboxExecutor, metricGroup, 1, 500L);

        IOException error = assertThrows(IOException.class, () -> writer.write(row("host-1"), null));

        assertTrue(error.getMessage().contains("table=metrics"));
        assertTrue(error.getMessage().contains("database=public"));
        assertTrue(error.getMessage().contains("rows=1"));
        assertTrue(error.getMessage().contains("flushReason=batch_full"));
        assertTrue(error.getMessage().contains("changelogMode=insert-only"));
        assertTrue(error.getMessage().contains("writeOp=insert"));
        assertTrue(error.getMessage().contains("writeHints=auto_create_table=true"));
        assertTrue(error.getMessage().contains("writeSettings=" + DEFAULT_WRITE_SETTINGS));
        assertEquals(1L, metricGroup.numRecordsSendErrorsCount());
        assertEquals(1L, metricGroup.counterValue(GreptimeSinkMetrics.FLUSH_TOTAL));
        assertEquals(1L, metricGroup.counterValue(GreptimeSinkMetrics.FLUSH_FAILURE_TOTAL));
        assertEquals(1L, metricGroup.counterValue("greptimedb.flush.reason.batch_full.total"));
    }

    @Test
    void wrapsFutureFailureWithAttemptContext() {
        RecordingWriteClient client = new RecordingWriteClient();
        CompletableFuture<Result<WriteOk, Err>> failedFuture = new CompletableFuture<>();
        failedFuture.completeExceptionally(new IllegalStateException("network unavailable"));
        client.nextFuture = failedFuture;
        TestProcessingTimeService timeService = new TestProcessingTimeService();
        TestMailboxExecutor mailboxExecutor = new TestMailboxExecutor();
        TestSinkWriterMetricGroup metricGroup = new TestSinkWriterMetricGroup();
        GreptimeSinkWriter<RowData> writer = newWriter(client, timeService, mailboxExecutor, metricGroup, 1, 500L);

        IOException error = assertThrows(IOException.class, () -> writer.write(row("host-1"), null));

        assertTrue(error.getMessage().startsWith("Failed to write rows to GreptimeDB: writeMode=regular"));
        assertTrue(error.getMessage().contains("table=metrics"));
        assertTrue(error.getMessage().contains("flushReason=batch_full"));
        assertEquals(1L, metricGroup.numRecordsSendErrorsCount());
        assertEquals(1L, metricGroup.counterValue(GreptimeSinkMetrics.FLUSH_FAILURE_TOTAL));
    }

    @Test
    void boundsWriteFutureWaitByRpcTimeout() {
        RecordingWriteClient client = new RecordingWriteClient();
        CompletableFuture<Result<WriteOk, Err>> pendingFuture = new CompletableFuture<>();
        client.nextFuture = pendingFuture;
        TestProcessingTimeService timeService = new TestProcessingTimeService();
        TestMailboxExecutor mailboxExecutor = new TestMailboxExecutor();
        TestSinkWriterMetricGroup metricGroup = new TestSinkWriterMetricGroup();
        GreptimeSinkWriter<RowData> writer = newWriter(client, timeService, mailboxExecutor, metricGroup, 1, 0L, 1);

        IOException error = assertThrows(IOException.class, () -> writer.write(row("host-1"), null));

        assertTrue(error.getMessage()
                .startsWith("Timed out after 1 ms while writing rows to GreptimeDB: writeMode=regular"));
        assertTrue(error.getMessage().contains("table=metrics"));
        assertTrue(pendingFuture.isCancelled());
        assertEquals(1L, metricGroup.numRecordsSendErrorsCount());
        assertEquals(1L, metricGroup.counterValue(GreptimeSinkMetrics.FLUSH_FAILURE_TOTAL));
    }

    @Test
    void cancelsWriteFutureWhenFlushWaitIsInterrupted() throws Exception {
        RecordingWriteClient client = new RecordingWriteClient();
        BlockingGetWriteFuture pendingFuture = new BlockingGetWriteFuture();
        client.nextFuture = pendingFuture;
        GreptimeSinkWriter<RowData> writer =
                newWriter(client, new TestProcessingTimeService(), new TestMailboxExecutor(), 1, 0L);

        interruptBlockedCall(() -> writer.write(row("host-1"), null), pendingFuture::awaitTimedGet);

        assertTrue(pendingFuture.isCancelled());
        assertEquals(List.of(1), client.rowCounts);
        IOException error = assertThrows(IOException.class, () -> writer.flush(false));
        assertEquals("GreptimeDB sink writer is not open: failed", error.getMessage());
    }

    @Test
    void closeDoesNotRetryBufferAfterFlushFailure() throws Exception {
        RecordingWriteClient client = new RecordingWriteClient();
        client.nextResult = Err.writeErr(503, new IllegalStateException("rate limited"), null)
                .mapToResult();
        TestProcessingTimeService timeService = new TestProcessingTimeService();
        TestMailboxExecutor mailboxExecutor = new TestMailboxExecutor();
        GreptimeSinkWriter<RowData> writer =
                newWriter(client, timeService, mailboxExecutor, new TestSinkWriterMetricGroup(), 1, 500L);

        assertThrows(IOException.class, () -> writer.write(row("host-1"), null));
        writer.close();

        assertEquals(List.of(1), client.rowCounts);
        assertEquals(true, client.shutdown);
    }

    @Test
    void rejectsFlushAfterFlushFailureWithoutRetryingBuffer() throws Exception {
        RecordingWriteClient client = new RecordingWriteClient();
        client.nextResult = Err.writeErr(503, new IllegalStateException("rate limited"), null)
                .mapToResult();
        GreptimeSinkWriter<RowData> writer =
                newWriter(client, new TestProcessingTimeService(), new TestMailboxExecutor(), 100, 0L);

        writer.write(row("host-1"), null);
        assertThrows(IOException.class, () -> writer.flush(false));

        IOException error = assertThrows(IOException.class, () -> writer.flush(false));

        assertEquals("GreptimeDB sink writer is not open: failed", error.getMessage());
        assertEquals(List.of(1), client.rowCounts);
    }

    @Test
    void rejectsWriteAfterFlushFailureBeforeTouchingCompletedBuffer() throws Exception {
        RecordingWriteClient client = new RecordingWriteClient();
        client.nextResult = Err.writeErr(503, new IllegalStateException("rate limited"), null)
                .mapToResult();
        GreptimeSinkWriter<RowData> writer =
                newWriter(client, new TestProcessingTimeService(), new TestMailboxExecutor(), 100, 0L);

        writer.write(row("host-1"), null);
        assertThrows(IOException.class, () -> writer.flush(false));

        IOException error = assertThrows(IOException.class, () -> writer.write(row("host-2"), null));

        assertEquals("GreptimeDB sink writer is not open: failed", error.getMessage());
        assertEquals(List.of(1), client.rowCounts);
    }

    private static void interruptBlockedCall(ThrowingRunnable<Exception> call, BlockedWait blockedWait)
            throws Exception {
        AtomicReference<Throwable> failure = new AtomicReference<>();
        AtomicBoolean interruptPreserved = new AtomicBoolean();
        Thread caller = new Thread(
                () -> {
                    try {
                        call.run();
                    } catch (Throwable t) {
                        interruptPreserved.set(Thread.currentThread().isInterrupted());
                        failure.set(t);
                    }
                },
                "greptimedb-sink-writer-test-caller");

        caller.start();
        assertTrue(blockedWait.await(2000L), "caller did not reach the blocking operation");
        caller.interrupt();
        caller.join(2000L);

        assertTrue(!caller.isAlive(), "caller did not stop after interrupt");
        assertTrue(
                failure.get() instanceof InterruptedException,
                "expected InterruptedException, but got " + failure.get());
        assertTrue(interruptPreserved.get(), "interrupt status was not preserved");
    }

    private static GreptimeSinkWriter<RowData> newWriter(
            RecordingWriteClient client,
            TestProcessingTimeService timeService,
            TestMailboxExecutor mailboxExecutor,
            int batchMaxRows,
            long flushIntervalMs) {
        return newWriter(
                client, timeService, mailboxExecutor, new TestSinkWriterMetricGroup(), batchMaxRows, flushIntervalMs);
    }

    private static GreptimeSinkWriter<RowData> newWriter(
            RecordingWriteClient client,
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
                GreptimeSinkConfig.DEFAULT_RPC_TIMEOUT_MS,
                GreptimeChangelogMode.INSERT_ONLY);
    }

    private static GreptimeSinkWriter<RowData> newWriter(
            RecordingWriteClient client,
            TestProcessingTimeService timeService,
            TestMailboxExecutor mailboxExecutor,
            TestSinkWriterMetricGroup metricGroup,
            int batchMaxRows,
            long flushIntervalMs,
            int rpcTimeoutMs) {
        return newWriter(
                client,
                timeService,
                mailboxExecutor,
                metricGroup,
                batchMaxRows,
                flushIntervalMs,
                rpcTimeoutMs,
                GreptimeChangelogMode.INSERT_ONLY);
    }

    private static GreptimeSinkWriter<RowData> newWriter(
            RecordingWriteClient client,
            TestProcessingTimeService timeService,
            TestMailboxExecutor mailboxExecutor,
            TestSinkWriterMetricGroup metricGroup,
            int batchMaxRows,
            long flushIntervalMs,
            int rpcTimeoutMs,
            GreptimeChangelogMode changelogMode) {
        GreptimeTableSchema tableSchema = tableSchema();
        GreptimeSinkConfig sinkConfig = GreptimeSinkConfig.builder()
                .endpoints(List.of("127.0.0.1:4001"))
                .batchMaxRows(batchMaxRows)
                .flushIntervalMs(flushIntervalMs)
                .rpcTimeoutMs(rpcTimeoutMs)
                .changelogMode(changelogMode)
                .build();
        return new GreptimeSinkWriter<>(
                client,
                sinkConfig,
                tableSchema,
                GreptimeRowDataConverter.forSchema(tableSchema),
                GreptimeRowDataConverter.forKeyColumns(tableSchema),
                timeService,
                mailboxExecutor,
                metricGroup);
    }

    private static WriteAttemptContext attempt(int rows) {
        return new WriteAttemptContext(
                GreptimeWriteMode.REGULAR,
                GreptimeChangelogMode.INSERT_ONLY,
                GreptimeRowOperation.INSERT,
                "metrics",
                "public",
                1,
                rows,
                FlushReason.BATCH_FULL,
                2,
                500L,
                "auto_create_table=true",
                DEFAULT_WRITE_SETTINGS);
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

    private static RowData rowWithNullHost(RowKind rowKind) {
        GenericRowData row = GenericRowData.of(null, 0.5d, TimestampData.fromEpochMillis(1700000000000L));
        row.setRowKind(rowKind);
        return row;
    }

    private static RowData rowWithNullTimestamp(String host) {
        GenericRowData row = GenericRowData.of(StringData.fromString(host), 0.5d, null);
        row.setRowKind(RowKind.INSERT);
        return row;
    }

    private static final class RecordingWriteClient implements GreptimeWriteClient {
        private final List<Integer> rowCounts = new ArrayList<>();
        private final List<Integer> columnCounts = new ArrayList<>();
        private final List<WriteOp> writeOps = new ArrayList<>();
        private final List<Context> contexts = new ArrayList<>();
        private CompletableFuture<Result<WriteOk, Err>> nextFuture;
        private Result<WriteOk, Err> nextResult;
        private boolean shutdown;

        @Override
        public CompletableFuture<Result<WriteOk, Err>> write(
                Table table, WriteOp writeOp, io.greptime.rpc.Context context) {
            rowCounts.add(table.rowCount());
            columnCounts.add(table.columnCount());
            writeOps.add(writeOp);
            contexts.add(context);
            if (nextFuture != null) {
                return nextFuture;
            }
            if (nextResult != null) {
                return CompletableFuture.completedFuture(nextResult);
            }
            return CompletableFuture.completedFuture(Result.ok(WriteOk.ok(table.rowCount(), 0)));
        }

        @Override
        public void shutdownGracefully() {
            shutdown = true;
        }
    }

    private static final class BlockingGetWriteFuture extends CompletableFuture<Result<WriteOk, Err>> {
        private final CountDownLatch timedGetCalled = new CountDownLatch(1);

        @Override
        public Result<WriteOk, Err> get(long timeout, TimeUnit unit)
                throws InterruptedException, ExecutionException, TimeoutException {
            timedGetCalled.countDown();
            return super.get(timeout, unit);
        }

        private boolean awaitTimedGet(long timeoutMs) throws InterruptedException {
            return timedGetCalled.await(timeoutMs, TimeUnit.MILLISECONDS);
        }
    }

    private interface BlockedWait {
        boolean await(long timeoutMs) throws Exception;
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

        private long currentSendTime() {
            return currentSendTimeGauge.getValue();
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
