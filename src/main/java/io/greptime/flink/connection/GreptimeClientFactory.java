package io.greptime.flink.connection;

import io.greptime.GreptimeDB;
import io.greptime.flink.cfg.GreptimeSinkConfig;
import io.greptime.limit.LimitedPolicy;
import io.greptime.models.AuthInfo;
import io.greptime.options.GreptimeOptions;
import io.greptime.rpc.RpcOptions;
import java.util.concurrent.TimeUnit;

public final class GreptimeClientFactory {
    public GreptimeDB create(GreptimeSinkConfig sinkConfig) {
        return GreptimeDB.create(buildGreptimeOptions(sinkConfig));
    }

    GreptimeOptions buildGreptimeOptions(GreptimeSinkConfig sinkConfig) {
        GreptimeOptions.Builder builder =
                GreptimeOptions.newBuilder(sinkConfig.getEndpoints().toArray(new String[0]), sinkConfig.getDatabase());

        if (sinkConfig.getUsername() != null && sinkConfig.getPassword() != null) {
            builder.authInfo(new AuthInfo(sinkConfig.getUsername(), sinkConfig.getPassword()));
        }

        return builder.writeMaxRetries(sinkConfig.getWriteMaxRetries())
                .maxInFlightWritePoints(sinkConfig.getWriteMaxInFlightPoints())
                .writeLimitedPolicy(createWriteLimitedPolicy(sinkConfig))
                .rpcOptions(createRpcOptions(sinkConfig))
                .routeTableRefreshPeriodSeconds(sinkConfig.getRouteRefreshPeriodSeconds())
                .checkHealthTimeoutMs(sinkConfig.getRouteHealthTimeoutMs())
                .build();
    }

    private static RpcOptions createRpcOptions(GreptimeSinkConfig sinkConfig) {
        RpcOptions rpcOptions = RpcOptions.newDefault();
        rpcOptions.setDefaultRpcTimeout(sinkConfig.getRpcTimeoutMs());
        return rpcOptions;
    }

    private static LimitedPolicy createWriteLimitedPolicy(GreptimeSinkConfig sinkConfig) {
        switch (sinkConfig.getWriteLimitPolicy()) {
            case ABORT:
                return new LimitedPolicy.AbortPolicy();
            case BLOCKING:
                return new LimitedPolicy.BlockingPolicy();
            case BLOCKING_TIMEOUT:
                return new LimitedPolicy.BlockingTimeoutPolicy(
                        sinkConfig.getWriteLimitTimeoutMs(), TimeUnit.MILLISECONDS);
            case ABORT_ON_BLOCKING_TIMEOUT:
                return new LimitedPolicy.AbortOnBlockingTimeoutPolicy(
                        sinkConfig.getWriteLimitTimeoutMs(), TimeUnit.MILLISECONDS);
            default:
                throw new IllegalArgumentException(
                        "Unsupported write limit policy: " + sinkConfig.getWriteLimitPolicy());
        }
    }
}
