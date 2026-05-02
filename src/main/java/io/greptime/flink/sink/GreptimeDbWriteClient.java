package io.greptime.flink.sink;

import io.greptime.GreptimeDB;
import io.greptime.WriteOp;
import io.greptime.models.Err;
import io.greptime.models.Result;
import io.greptime.models.Table;
import io.greptime.models.WriteOk;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

final class GreptimeDbWriteClient implements GreptimeWriteClient {
    private final GreptimeDB client;

    GreptimeDbWriteClient(GreptimeDB client) {
        this.client = Objects.requireNonNull(client, "client");
    }

    @Override
    public CompletableFuture<Result<WriteOk, Err>> write(
            Table table, WriteOp writeOp, io.greptime.rpc.Context context) {
        return client.write(List.of(table), writeOp, context);
    }

    @Override
    public void shutdownGracefully() {
        client.shutdownGracefully();
    }
}
