package io.greptime.flink.sink;

import io.greptime.BulkStreamWriter;
import io.greptime.GreptimeDB;
import io.greptime.flink.cfg.GreptimeBulkWriteConfig;
import io.greptime.flink.sink.schema.GreptimeTableSchema;
import io.greptime.models.Table;
import io.greptime.rpc.Context;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

final class GreptimeDbBulkWriteClient implements GreptimeBulkWriteClient {
    private final BulkStreamWriterFactory writerFactory;
    private final ExecutorService streamOperationExecutor;
    private final Runnable shutdownClient;
    private BulkStreamWriter writer;
    private boolean streamClosed = true;

    GreptimeDbBulkWriteClient(
            GreptimeDB client,
            GreptimeTableSchema tableSchema,
            GreptimeBulkWriteConfig bulkWriteConfig,
            Context context) {
        this(
                () -> newBulkStreamWriter(client, tableSchema, bulkWriteConfig, context),
                Objects.requireNonNull(client, "client")::shutdownGracefully);
    }

    GreptimeDbBulkWriteClient(BulkStreamWriter writer) {
        this(() -> writer, () -> {});
    }

    GreptimeDbBulkWriteClient(BulkStreamWriterFactory writerFactory, Runnable shutdownClient) {
        this.shutdownClient = Objects.requireNonNull(shutdownClient, "shutdownClient");
        this.writerFactory = Objects.requireNonNull(writerFactory, "writerFactory");
        this.streamOperationExecutor = newStreamOperationExecutor();
    }

    private static BulkStreamWriter newBulkStreamWriter(
            GreptimeDB client,
            GreptimeTableSchema tableSchema,
            GreptimeBulkWriteConfig bulkWriteConfig,
            Context context) {
        return client.bulkStreamWriter(
                Objects.requireNonNull(tableSchema, "tableSchema").toGreptimeTableSchema(),
                Objects.requireNonNull(bulkWriteConfig, "bulkWriteConfig").toSdkConfig(),
                Objects.requireNonNull(context, "context"));
    }

    @Override
    public Table.TableBufferRoot newTableBuffer(int columnBufferSize) {
        return currentWriter().tableBufferRoot(columnBufferSize);
    }

    @Override
    public int writeNext(long timeout, TimeUnit unit) throws Exception {
        validateTimeout(timeout, unit);

        long timeoutNanos = unit.toNanos(timeout);
        long deadlineNanos = System.nanoTime() + timeoutNanos;
        CompletableFuture<Integer> writeFuture = invokeWriteNext(timeout, unit);
        long remainingNanos = deadlineNanos - System.nanoTime();
        if (remainingNanos <= 0L) {
            throw newWriteNextTimeoutExceptionAfterClose(timeout, unit);
        }
        try {
            return writeFuture.get(remainingNanos, TimeUnit.NANOSECONDS);
        } catch (TimeoutException e) {
            writeFuture.cancel(true);
            throw newWriteNextTimeoutExceptionAfterClose(timeout, unit);
        } catch (InterruptedException e) {
            cancelAndCloseAfterInterrupt(writeFuture, e);
            throw e;
        }
    }

    private CompletableFuture<Integer> invokeWriteNext(long timeout, TimeUnit unit) throws Exception {
        BulkStreamWriter currentWriter = currentWriter();
        Future<CompletableFuture<Integer>> invocation = streamOperationExecutor.submit(currentWriter::writeNext);
        try {
            return invocation.get(timeout, unit);
        } catch (TimeoutException e) {
            invocation.cancel(true);
            throw newWriteNextTimeoutExceptionAfterClose(timeout, unit);
        } catch (InterruptedException e) {
            cancelAndCloseAfterInterrupt(invocation, e);
            throw e;
        } catch (ExecutionException e) {
            throw unwrapExecutionException(e);
        }
    }

    @Override
    public void completed(long timeout, TimeUnit unit) throws Exception {
        validateTimeout(timeout, unit);

        BulkStreamWriter currentWriter = currentWriter();
        Future<Void> completion = streamOperationExecutor.submit(() -> {
            currentWriter.completed();
            return null;
        });
        try {
            completion.get(timeout, unit);
            markStreamClosed();
        } catch (TimeoutException e) {
            completion.cancel(true);
            TimeoutException timeoutFailure = newCompletedTimeoutException(timeout, unit);
            closeStreamSuppressing(timeoutFailure);
            throw timeoutFailure;
        } catch (InterruptedException e) {
            cancelAndCloseAfterInterrupt(completion, e);
            throw e;
        } catch (ExecutionException e) {
            throw unwrapExecutionException(e);
        }
    }

    @Override
    public void startNewStream(long timeout, TimeUnit unit) throws Exception {
        validateTimeout(timeout, unit);
        if (hasOpenStream()) {
            return;
        }

        StreamOpenAttempt attempt = new StreamOpenAttempt();
        Future<?> opening = streamOperationExecutor.submit(() -> {
            try {
                BulkStreamWriter openedWriter = createWriter();
                if (!attempt.complete(openedWriter)) {
                    closeLateOpenedWriter(openedWriter);
                }
            } catch (RuntimeException | Error e) {
                attempt.completeExceptionally(e);
                throw e;
            }
        });
        try {
            BulkStreamWriter openedWriter = attempt.await(timeout, unit);
            setCurrentWriter(openedWriter);
        } catch (TimeoutException e) {
            opening.cancel(true);
            throw newStartStreamTimeoutException(timeout, unit);
        } catch (InterruptedException e) {
            BulkStreamWriter openedWriter = attempt.abandon();
            if (openedWriter != null) {
                closeLateOpenedWriter(openedWriter);
            }
            opening.cancel(true);
            Thread.currentThread().interrupt();
            throw e;
        } catch (ExecutionException e) {
            throw unwrapExecutionException(e);
        }
    }

    private static final class StreamOpenAttempt {
        private BulkStreamWriter writer;
        private Throwable failure;
        private boolean abandoned;

        synchronized boolean complete(BulkStreamWriter openedWriter) {
            if (abandoned) {
                return false;
            }
            writer = Objects.requireNonNull(openedWriter, "writer");
            notifyAll();
            return true;
        }

        synchronized void completeExceptionally(Throwable e) {
            if (!abandoned) {
                failure = e;
                notifyAll();
            }
        }

        synchronized BulkStreamWriter await(long timeout, TimeUnit unit)
                throws InterruptedException, ExecutionException, TimeoutException {
            long remainingNanos = unit.toNanos(timeout);
            long deadlineNanos = System.nanoTime() + remainingNanos;
            while (writer == null && failure == null) {
                if (remainingNanos <= 0L) {
                    abandon();
                    throw new TimeoutException();
                }
                TimeUnit.NANOSECONDS.timedWait(this, remainingNanos);
                remainingNanos = deadlineNanos - System.nanoTime();
            }

            if (failure != null) {
                if (failure instanceof Error) {
                    throw (Error) failure;
                }
                throw new ExecutionException(failure);
            }
            return writer;
        }

        synchronized BulkStreamWriter abandon() {
            abandoned = true;
            BulkStreamWriter openedWriter = writer;
            writer = null;
            return openedWriter;
        }
    }

    @Override
    public boolean isStreamReady() {
        return currentWriter().isStreamReady();
    }

    @Override
    public synchronized void closeStream() throws Exception {
        if (writer == null || streamClosed) {
            return;
        }
        writer.close();
        streamClosed = true;
    }

    @Override
    public void shutdownClient() {
        streamOperationExecutor.shutdownNow();
        shutdownClient.run();
    }

    private static ExecutorService newStreamOperationExecutor() {
        return Executors.newSingleThreadExecutor(command -> {
            Thread thread = new Thread(command, "greptimedb-bulk-stream-operation");
            thread.setDaemon(true);
            return thread;
        });
    }

    private static void validateTimeout(long timeout, TimeUnit unit) {
        if (timeout <= 0L) {
            throw new IllegalArgumentException("timeout must be greater than 0");
        }
        Objects.requireNonNull(unit, "unit");
    }

    private synchronized void markStreamClosed() {
        streamClosed = true;
    }

    private BulkStreamWriter createWriter() {
        return Objects.requireNonNull(writerFactory.create(), "writer");
    }

    private synchronized BulkStreamWriter currentWriter() {
        if (writer == null || streamClosed) {
            throw new IllegalStateException("GreptimeDB bulk write stream is not open");
        }
        return writer;
    }

    private synchronized boolean hasOpenStream() {
        return writer != null && !streamClosed;
    }

    private synchronized void setCurrentWriter(BulkStreamWriter openedWriter) {
        writer = Objects.requireNonNull(openedWriter, "writer");
        streamClosed = false;
    }

    private static void closeLateOpenedWriter(BulkStreamWriter openedWriter) {
        try {
            openedWriter.close();
        } catch (Exception ignored) {
            // The caller has already timed out; this is best-effort cleanup for a late SDK writer.
        }
    }

    private TimeoutException newWriteNextTimeoutExceptionAfterClose(long timeout, TimeUnit unit) {
        TimeoutException timeoutFailure = newWriteNextTimeoutException(timeout, unit);
        closeStreamSuppressing(timeoutFailure);
        return timeoutFailure;
    }

    private void cancelAndCloseAfterInterrupt(Future<?> operation, InterruptedException interrupted) {
        operation.cancel(true);
        closeStreamSuppressing(interrupted);
        Thread.currentThread().interrupt();
    }

    private void closeStreamSuppressing(Throwable failure) {
        try {
            closeStream();
        } catch (Exception closeFailure) {
            failure.addSuppressed(closeFailure);
        }
    }

    private static TimeoutException newStartStreamTimeoutException(long timeout, TimeUnit unit) {
        return new TimeoutException(
                "Timed out after " + unit.toMillis(timeout) + " ms while opening GreptimeDB bulk stream");
    }

    private static TimeoutException newWriteNextTimeoutException(long timeout, TimeUnit unit) {
        return new TimeoutException(
                "Timed out after " + unit.toMillis(timeout) + " ms while invoking GreptimeDB bulk writeNext()");
    }

    private static TimeoutException newCompletedTimeoutException(long timeout, TimeUnit unit) {
        return new TimeoutException(
                "Timed out after " + unit.toMillis(timeout) + " ms while invoking GreptimeDB bulk completed()");
    }

    private static Exception unwrapExecutionException(ExecutionException e) {
        Throwable cause = e.getCause();
        if (cause instanceof Exception) {
            return (Exception) cause;
        }
        if (cause instanceof Error) {
            throw (Error) cause;
        }
        return e;
    }

    interface BulkStreamWriterFactory {
        BulkStreamWriter create();
    }
}
