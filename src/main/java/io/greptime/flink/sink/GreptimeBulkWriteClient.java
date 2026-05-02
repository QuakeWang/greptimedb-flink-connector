package io.greptime.flink.sink;

import io.greptime.models.Table;
import java.util.concurrent.TimeUnit;

interface GreptimeBulkWriteClient {
    Table.TableBufferRoot newTableBuffer(int columnBufferSize);

    int writeNext(long timeout, TimeUnit unit) throws Exception;

    void completed(long timeout, TimeUnit unit) throws Exception;

    void startNewStream(long timeout, TimeUnit unit) throws Exception;

    boolean isStreamReady();

    void closeStream() throws Exception;

    void shutdownClient();
}
