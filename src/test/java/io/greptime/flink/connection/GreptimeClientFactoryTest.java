package io.greptime.flink.connection;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import io.greptime.flink.cfg.GreptimeSinkConfig;
import io.greptime.limit.LimitedPolicy;
import io.greptime.options.GreptimeOptions;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class GreptimeClientFactoryTest {
    @Test
    void buildsGreptimeOptionsFromSinkConfig() {
        GreptimeSinkConfig sinkConfig = GreptimeSinkConfig.builder()
                .endpoints(List.of("127.0.0.1:4001"))
                .database("public")
                .credentials("greptime", "secret")
                .writeMaxRetries(3)
                .writeMaxInFlightPoints(1024)
                .writeLimitPolicy(GreptimeSinkConfig.WriteLimitPolicy.ABORT_ON_BLOCKING_TIMEOUT)
                .writeLimitTimeoutMs(2500L)
                .rpcTimeoutMs(30000)
                .routeRefreshPeriodSeconds(60L)
                .routeHealthTimeoutMs(500L)
                .build();

        GreptimeOptions options = new GreptimeClientFactory().buildGreptimeOptions(sinkConfig);

        assertEquals("public", options.getDatabase());
        assertEquals(3, options.getWriteOptions().getMaxRetries());
        assertEquals(1024, options.getWriteOptions().getMaxInFlightWritePoints());
        assertEquals(30000, options.getRpcOptions().getDefaultRpcTimeout());
        assertEquals(60L, options.getRouterOptions().getRefreshPeriodSeconds());
        assertEquals(500L, options.getRouterOptions().getCheckHealthTimeoutMs());
        assertTrue(options.getWriteOptions().getLimitedPolicy() instanceof LimitedPolicy.AbortOnBlockingTimeoutPolicy);

        LimitedPolicy.BlockingTimeoutPolicy policy =
                (LimitedPolicy.BlockingTimeoutPolicy) options.getWriteOptions().getLimitedPolicy();
        assertEquals(2500L, policy.timeout());
        assertEquals(TimeUnit.MILLISECONDS, policy.unit());
    }

    @Test
    void mapsNonTimeoutWriteLimitPolicies() {
        assertTrue(writeLimitedPolicy(GreptimeSinkConfig.WriteLimitPolicy.ABORT) instanceof LimitedPolicy.AbortPolicy);
        assertTrue(
                writeLimitedPolicy(GreptimeSinkConfig.WriteLimitPolicy.BLOCKING)
                        instanceof LimitedPolicy.BlockingPolicy);
    }

    @Test
    void mapsBlockingTimeoutWriteLimitPolicy() {
        LimitedPolicy policy = writeLimitedPolicy(GreptimeSinkConfig.WriteLimitPolicy.BLOCKING_TIMEOUT);

        assertTrue(policy instanceof LimitedPolicy.BlockingTimeoutPolicy);
        assertEquals(2500L, ((LimitedPolicy.BlockingTimeoutPolicy) policy).timeout());
    }

    @Test
    void treatsBlankCredentialsAsNoAuthorization() {
        GreptimeSinkConfig sinkConfig = GreptimeSinkConfig.builder()
                .endpoints(List.of("127.0.0.1:4001"))
                .credentials(" ", "")
                .build();

        GreptimeOptions options = new GreptimeClientFactory().buildGreptimeOptions(sinkConfig);

        assertNull(options.getWriteOptions().getAuthInfo());
    }

    private static LimitedPolicy writeLimitedPolicy(GreptimeSinkConfig.WriteLimitPolicy writeLimitPolicy) {
        GreptimeSinkConfig sinkConfig = GreptimeSinkConfig.builder()
                .endpoints(List.of("127.0.0.1:4001"))
                .writeLimitPolicy(writeLimitPolicy)
                .writeLimitTimeoutMs(2500L)
                .build();

        return new GreptimeClientFactory()
                .buildGreptimeOptions(sinkConfig)
                .getWriteOptions()
                .getLimitedPolicy();
    }
}
