package io.greptime.flink.sink;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import io.greptime.BulkStreamWriter;
import io.greptime.models.Table;
import io.greptime.v1.Database;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class GreptimeDbBulkWriteClientTest {
    @Test
    void doesNotCreateBulkStreamWriterInConstructor() {
        AtomicInteger created = new AtomicInteger();

        GreptimeDbBulkWriteClient client = new GreptimeDbBulkWriteClient(
                () -> {
                    created.incrementAndGet();
                    return new PendingWriteNextBulkStreamWriter();
                },
                () -> {});

        try {
            assertEquals(0, created.get());
        } finally {
            client.shutdownClient();
        }
    }

    @Test
    void surfacesBulkStreamWriterCreationFailureWhenOpeningStream() {
        IllegalStateException creationFailure = new IllegalStateException("route failed");
        GreptimeDbBulkWriteClient client = new GreptimeDbBulkWriteClient(
                () -> {
                    throw creationFailure;
                },
                () -> {});

        try {
            IllegalStateException error =
                    assertThrows(IllegalStateException.class, () -> client.startNewStream(50L, TimeUnit.MILLISECONDS));

            assertSame(creationFailure, error);
        } finally {
            client.shutdownClient();
        }
    }

    @Test
    void concurrentStartNewStreamOpensOnlyOneWriter() throws Exception {
        AtomicInteger created = new AtomicInteger();
        CountDownLatch callersReady = new CountDownLatch(2);
        CountDownLatch startCalls = new CountDownLatch(1);
        CountDownLatch createStarted = new CountDownLatch(1);
        CountDownLatch releaseCreate = new CountDownLatch(1);
        GreptimeDbBulkWriteClient client = new GreptimeDbBulkWriteClient(
                () -> {
                    created.incrementAndGet();
                    createStarted.countDown();
                    awaitLatchUnchecked(releaseCreate);
                    return new PendingWriteNextBulkStreamWriter(CompletableFuture.completedFuture(0));
                },
                () -> {});
        AtomicReference<Throwable> firstFailure = new AtomicReference<>();
        AtomicReference<Throwable> secondFailure = new AtomicReference<>();
        Thread first = startStreamThread(client, callersReady, startCalls, firstFailure);
        Thread second = startStreamThread(client, callersReady, startCalls, secondFailure);

        try {
            first.start();
            second.start();
            assertTrue(callersReady.await(2000L, TimeUnit.MILLISECONDS), "callers did not become ready");
            startCalls.countDown();
            assertTrue(createStarted.await(2000L, TimeUnit.MILLISECONDS), "writer creation did not start");
            Thread.sleep(100L);
            releaseCreate.countDown();
            first.join(2000L);
            second.join(2000L);

            assertTrue(!first.isAlive(), "first caller did not finish");
            assertTrue(!second.isAlive(), "second caller did not finish");
            assertNull(firstFailure.get());
            assertNull(secondFailure.get());
            assertEquals(1, created.get());
        } finally {
            releaseCreate.countDown();
            client.shutdownClient();
        }
    }

    @Test
    void usesUniqueStreamOperationThreadName() throws Exception {
        AtomicReference<String> threadName = new AtomicReference<>();
        GreptimeDbBulkWriteClient client = new GreptimeDbBulkWriteClient(
                () -> {
                    threadName.set(Thread.currentThread().getName());
                    return new PendingWriteNextBulkStreamWriter(CompletableFuture.completedFuture(0));
                },
                () -> {});

        try {
            client.startNewStream(50L, TimeUnit.MILLISECONDS);

            assertTrue(
                    threadName.get().matches("greptimedb-bulk-stream-operation-\\d+"),
                    "unexpected thread name: " + threadName.get());
        } finally {
            client.shutdownClient();
        }
    }

    @Test
    void timesOutWhenBulkStreamWriterCreationBlocks() throws Exception {
        BlockingBulkStreamWriterFactory writerFactory = new BlockingBulkStreamWriterFactory();
        GreptimeDbBulkWriteClient client = new GreptimeDbBulkWriteClient(writerFactory::create, () -> {});

        try {
            long startNanos = System.nanoTime();
            TimeoutException error =
                    assertThrows(TimeoutException.class, () -> client.startNewStream(50L, TimeUnit.MILLISECONDS));
            long durationMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);

            assertTrue(durationMs < 2000L, "stream open waited for blocked factory, durationMs=" + durationMs);
            assertEquals("Timed out after 50 ms while opening GreptimeDB bulk stream", error.getMessage());
        } finally {
            client.shutdownClient();
            writerFactory.release();
        }
    }

    @Test
    void closesLateCreatedWriterAfterOpenTimeout() throws Exception {
        PendingWriteNextBulkStreamWriter sdkWriter = new PendingWriteNextBulkStreamWriter();
        BlockingBulkStreamWriterFactory writerFactory = new BlockingBulkStreamWriterFactory(sdkWriter);
        GreptimeDbBulkWriteClient client = new GreptimeDbBulkWriteClient(writerFactory::create, () -> {});

        try {
            assertThrows(TimeoutException.class, () -> client.startNewStream(50L, TimeUnit.MILLISECONDS));

            writerFactory.release();

            assertTrue(sdkWriter.awaitClose(2000L), "late-created SDK writer was not closed");
            assertEquals(1, sdkWriter.closeCalls);
        } finally {
            client.shutdownClient();
            writerFactory.release();
        }
    }

    @Test
    void closesStreamWhenWriteNextResultTimesOut() throws Exception {
        PendingWriteNextBulkStreamWriter sdkWriter = new PendingWriteNextBulkStreamWriter();
        GreptimeDbBulkWriteClient client = new GreptimeDbBulkWriteClient(sdkWriter);

        try {
            client.startNewStream(50L, TimeUnit.MILLISECONDS);
            client.newTableBuffer(1024).addRow("host-1", 0.5d, 1700000000000L);

            TimeoutException error =
                    assertThrows(TimeoutException.class, () -> client.writeNext(50L, TimeUnit.MILLISECONDS));

            assertEquals("Timed out after 50 ms while invoking GreptimeDB bulk writeNext()", error.getMessage());
            assertEquals(1, sdkWriter.closeCalls);
        } finally {
            client.shutdownClient();
        }
    }

    @Test
    void closeStreamMarksStreamClosedWhenCloseThrows() throws Exception {
        ThrowingCloseBulkStreamWriter sdkWriter = new ThrowingCloseBulkStreamWriter();
        GreptimeDbBulkWriteClient client = new GreptimeDbBulkWriteClient(sdkWriter);

        try {
            client.startNewStream(50L, TimeUnit.MILLISECONDS);

            IllegalStateException error = assertThrows(IllegalStateException.class, client::closeStream);
            client.closeStream();

            assertEquals("close failed", error.getMessage());
            assertEquals(1, sdkWriter.closeCalls);
        } finally {
            client.shutdownClient();
        }
    }

    @Test
    void closesStreamWhenWriteNextInvocationIsInterrupted() throws Exception {
        BlockingWriteNextBulkStreamWriter sdkWriter = new BlockingWriteNextBulkStreamWriter();
        GreptimeDbBulkWriteClient client = new GreptimeDbBulkWriteClient(sdkWriter);

        try {
            client.startNewStream(50L, TimeUnit.MILLISECONDS);

            interruptBlockedCall(
                    () -> client.writeNext(5000L, TimeUnit.MILLISECONDS), sdkWriter::awaitWriteNextStarted);

            assertTrue(sdkWriter.awaitClose(2000L), "stream was not closed after interrupt");
            assertEquals(1, sdkWriter.closeCalls);
        } finally {
            sdkWriter.releaseWriteNext();
            client.shutdownClient();
        }
    }

    @Test
    void closesStreamWhenWriteNextResultWaitIsInterrupted() throws Exception {
        BlockingGetFuture writeNextFuture = new BlockingGetFuture();
        PendingWriteNextBulkStreamWriter sdkWriter = new PendingWriteNextBulkStreamWriter(writeNextFuture);
        GreptimeDbBulkWriteClient client = new GreptimeDbBulkWriteClient(sdkWriter);

        try {
            client.startNewStream(50L, TimeUnit.MILLISECONDS);

            interruptBlockedCall(() -> client.writeNext(5000L, TimeUnit.MILLISECONDS), writeNextFuture::awaitTimedGet);

            assertTrue(writeNextFuture.isCancelled());
            assertTrue(sdkWriter.awaitClose(2000L), "stream was not closed after interrupt");
            assertEquals(1, sdkWriter.closeCalls);
        } finally {
            client.shutdownClient();
        }
    }

    @Test
    void closesStreamWhenCompletedWaitIsInterrupted() throws Exception {
        BlockingCompletedBulkStreamWriter sdkWriter = new BlockingCompletedBulkStreamWriter();
        GreptimeDbBulkWriteClient client = new GreptimeDbBulkWriteClient(sdkWriter);

        try {
            client.startNewStream(50L, TimeUnit.MILLISECONDS);

            interruptBlockedCall(
                    () -> client.completed(5000L, TimeUnit.MILLISECONDS), sdkWriter::awaitCompletedStarted);

            assertTrue(sdkWriter.awaitClose(2000L), "stream was not closed after interrupt");
            assertEquals(1, sdkWriter.closeCalls);
        } finally {
            sdkWriter.releaseCompleted();
            client.shutdownClient();
        }
    }

    private static void interruptBlockedCall(InterruptibleCall call, BlockedWait blockedWait) throws Exception {
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
                "greptimedb-bulk-client-test-caller");

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

    private static Thread startStreamThread(
            GreptimeDbBulkWriteClient client,
            CountDownLatch callersReady,
            CountDownLatch startCalls,
            AtomicReference<Throwable> failure) {
        return new Thread(
                () -> {
                    try {
                        callersReady.countDown();
                        startCalls.await();
                        client.startNewStream(5000L, TimeUnit.MILLISECONDS);
                    } catch (Throwable t) {
                        failure.set(t);
                    }
                },
                "greptimedb-bulk-client-start-test-caller");
    }

    private static void awaitLatchUnchecked(CountDownLatch latch) {
        try {
            if (!latch.await(2000L, TimeUnit.MILLISECONDS)) {
                throw new IllegalStateException("timed out waiting for latch");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    private static final class BlockingBulkStreamWriterFactory {
        private static final long MAX_BLOCK_MS = 5000L;

        private final CountDownLatch releaseCreate = new CountDownLatch(1);
        private final BulkStreamWriter writer;

        private BlockingBulkStreamWriterFactory() {
            this(new PendingWriteNextBulkStreamWriter());
        }

        private BlockingBulkStreamWriterFactory(BulkStreamWriter writer) {
            this.writer = writer;
        }

        private BulkStreamWriter create() {
            long deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(MAX_BLOCK_MS);
            boolean interrupted = false;
            while (System.nanoTime() < deadlineNanos) {
                try {
                    if (releaseCreate.await(10L, TimeUnit.MILLISECONDS)) {
                        break;
                    }
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
            return writer;
        }

        private void release() {
            releaseCreate.countDown();
        }
    }

    private static final class PendingWriteNextBulkStreamWriter implements BulkStreamWriter {
        private final CompletableFuture<Integer> writeNextFuture;
        private final CountDownLatch closeCalled = new CountDownLatch(1);
        private RecordingTable currentTable;
        private int closeCalls;

        private PendingWriteNextBulkStreamWriter() {
            this(new CompletableFuture<>());
        }

        private PendingWriteNextBulkStreamWriter(CompletableFuture<Integer> writeNextFuture) {
            this.writeNextFuture = writeNextFuture;
        }

        @Override
        public Table.TableBufferRoot tableBufferRoot(int columnBufferSize) {
            currentTable = new RecordingTable("metrics");
            return currentTable;
        }

        @Override
        public CompletableFuture<Integer> writeNext() {
            return writeNextFuture;
        }

        @Override
        public void completed() {}

        @Override
        public void close() {
            closeCalls++;
            writeNextFuture.cancel(true);
            closeCalled.countDown();
        }

        private boolean awaitClose(long timeoutMs) throws InterruptedException {
            return closeCalled.await(timeoutMs, TimeUnit.MILLISECONDS);
        }
    }

    private static final class BlockingWriteNextBulkStreamWriter implements BulkStreamWriter {
        private final CountDownLatch writeNextStarted = new CountDownLatch(1);
        private final CountDownLatch releaseWriteNext = new CountDownLatch(1);
        private final CountDownLatch closeCalled = new CountDownLatch(1);
        private int closeCalls;

        @Override
        public Table.TableBufferRoot tableBufferRoot(int columnBufferSize) {
            return new RecordingTable("metrics");
        }

        @Override
        public CompletableFuture<Integer> writeNext() throws Exception {
            writeNextStarted.countDown();
            releaseWriteNext.await();
            return new CompletableFuture<>();
        }

        @Override
        public void completed() {}

        @Override
        public void close() {
            closeCalls++;
            releaseWriteNext();
            closeCalled.countDown();
        }

        private boolean awaitWriteNextStarted(long timeoutMs) throws InterruptedException {
            return writeNextStarted.await(timeoutMs, TimeUnit.MILLISECONDS);
        }

        private void releaseWriteNext() {
            releaseWriteNext.countDown();
        }

        private boolean awaitClose(long timeoutMs) throws InterruptedException {
            return closeCalled.await(timeoutMs, TimeUnit.MILLISECONDS);
        }
    }

    private static final class BlockingCompletedBulkStreamWriter implements BulkStreamWriter {
        private final CountDownLatch completedStarted = new CountDownLatch(1);
        private final CountDownLatch releaseCompleted = new CountDownLatch(1);
        private final CountDownLatch closeCalled = new CountDownLatch(1);
        private int closeCalls;

        @Override
        public Table.TableBufferRoot tableBufferRoot(int columnBufferSize) {
            return new RecordingTable("metrics");
        }

        @Override
        public CompletableFuture<Integer> writeNext() {
            return CompletableFuture.completedFuture(0);
        }

        @Override
        public void completed() throws Exception {
            completedStarted.countDown();
            releaseCompleted.await();
        }

        @Override
        public void close() {
            closeCalls++;
            releaseCompleted();
            closeCalled.countDown();
        }

        private boolean awaitCompletedStarted(long timeoutMs) throws InterruptedException {
            return completedStarted.await(timeoutMs, TimeUnit.MILLISECONDS);
        }

        private void releaseCompleted() {
            releaseCompleted.countDown();
        }

        private boolean awaitClose(long timeoutMs) throws InterruptedException {
            return closeCalled.await(timeoutMs, TimeUnit.MILLISECONDS);
        }
    }

    private static final class ThrowingCloseBulkStreamWriter implements BulkStreamWriter {
        private int closeCalls;

        @Override
        public Table.TableBufferRoot tableBufferRoot(int columnBufferSize) {
            return new RecordingTable("metrics");
        }

        @Override
        public CompletableFuture<Integer> writeNext() {
            return CompletableFuture.completedFuture(0);
        }

        @Override
        public void completed() {}

        @Override
        public void close() {
            closeCalls++;
            throw new IllegalStateException("close failed");
        }
    }

    private static final class BlockingGetFuture extends CompletableFuture<Integer> {
        private final CountDownLatch timedGetCalled = new CountDownLatch(1);

        @Override
        public Integer get(long timeout, TimeUnit unit)
                throws InterruptedException, java.util.concurrent.ExecutionException, TimeoutException {
            timedGetCalled.countDown();
            return super.get(timeout, unit);
        }

        private boolean awaitTimedGet(long timeoutMs) throws InterruptedException {
            return timedGetCalled.await(timeoutMs, TimeUnit.MILLISECONDS);
        }
    }

    private interface InterruptibleCall {
        void run() throws Exception;
    }

    private interface BlockedWait {
        boolean await(long timeoutMs) throws Exception;
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
            throw new UnsupportedOperationException("subRange is not used by bulk write client tests");
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
            throw new UnsupportedOperationException("Row insert request is not used by bulk write client tests");
        }

        @Override
        public Database.RowDeleteRequest intoRowDeleteRequest() {
            throw new UnsupportedOperationException("Row delete request is not used by bulk write client tests");
        }
    }
}
