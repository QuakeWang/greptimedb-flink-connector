package io.greptime.flink.sink;

import io.greptime.WriteOp;
import io.greptime.models.Err;
import io.greptime.models.Result;
import io.greptime.models.Table;
import io.greptime.models.WriteOk;
import java.util.concurrent.CompletableFuture;

interface GreptimeWriteClient {
    CompletableFuture<Result<WriteOk, Err>> write(Table table, WriteOp writeOp, io.greptime.rpc.Context context);

    void shutdownGracefully();
}
