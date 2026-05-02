package io.greptime.flink.cfg;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import io.greptime.BulkWrite;
import io.greptime.rpc.Compression;
import io.greptime.rpc.Context;
import java.util.List;
import org.junit.jupiter.api.Test;

class GreptimeSinkConfigTest {
    @Test
    void createsWriteContextWithConfiguredHints() {
        GreptimeSinkConfig sinkConfig = baseConfigBuilder()
                .batchMaxRows(100)
                .autoCreateTable(false)
                .ttl("7d")
                .appendMode(false)
                .mergeMode("last_non_null")
                .build();

        Context context = sinkConfig.createWriteContext();

        assertEquals("auto_create_table=false,ttl=7d,append_mode=false,merge_mode=last_non_null", context.getHints());
        assertEquals(
                "auto_create_table=false,ttl=7d,append_mode=false,merge_mode=last_non_null",
                sinkConfig.describeWriteHints());
    }

    @Test
    void omitsOptionalHintsWhenTheyAreNotConfigured() {
        GreptimeSinkConfig sinkConfig = baseConfigBuilder().batchMaxRows(100).build();

        Context context = sinkConfig.createWriteContext();

        assertEquals("auto_create_table=true", context.getHints());
        assertEquals("auto_create_table=true", sinkConfig.describeWriteHints());
    }

    @Test
    void describesWriteHintsWithoutCredentials() {
        GreptimeSinkConfig sinkConfig = baseConfigBuilder()
                .batchMaxRows(100)
                .credentials("greptime", "secret")
                .build();

        assertEquals("auto_create_table=true", sinkConfig.describeWriteHints());
    }

    @Test
    void createsWriteContextWithCompression() {
        GreptimeSinkConfig sinkConfig = GreptimeSinkConfig.builder()
                .endpoints(List.of("127.0.0.1:4001"))
                .writeCompression(GreptimeSinkConfig.WriteCompression.ZSTD)
                .build();

        Context context = sinkConfig.createWriteContext();

        assertEquals(Compression.Zstd, context.getCompression());
        assertEquals("auto_create_table=true", context.getHints());
    }

    @Test
    void createsBulkWriteContextWithoutRegularWriteHintsOrCompression() {
        GreptimeSinkConfig sinkConfig = GreptimeSinkConfig.builder()
                .endpoints(List.of("127.0.0.1:4001"))
                .autoCreateTable(false)
                .ttl("7d")
                .appendMode(false)
                .mergeMode("last_row")
                .writeCompression(GreptimeSinkConfig.WriteCompression.ZSTD)
                .build();

        Context context = sinkConfig.createBulkWriteContext();

        assertNull(context.getHints());
        assertEquals(Compression.None, context.getCompression());
    }

    @Test
    void describesWriteSettingsWithoutCredentials() {
        GreptimeSinkConfig sinkConfig = GreptimeSinkConfig.builder()
                .endpoints(List.of("127.0.0.1:4001"))
                .credentials("greptime", "secret")
                .writeMaxRetries(3)
                .writeMaxInFlightPoints(1024)
                .writeLimitPolicy(GreptimeSinkConfig.WriteLimitPolicy.BLOCKING_TIMEOUT)
                .writeLimitTimeoutMs(2500L)
                .writeCompression(GreptimeSinkConfig.WriteCompression.GZIP)
                .rpcTimeoutMs(30000)
                .routeRefreshPeriodSeconds(60L)
                .routeHealthTimeoutMs(500L)
                .build();

        assertEquals(
                "writeMaxRetries=3,writeMaxInFlightPoints=1024,writeLimitPolicy=blocking-timeout,writeLimitTimeoutMs=2500,writeCompression=gzip,rpcTimeoutMs=30000,routeRefreshPeriodSeconds=60,routeHealthTimeoutMs=500",
                sinkConfig.describeWriteSettings());
    }

    @Test
    void defaultsToRegularWriteModeAndBulkDefaults() {
        GreptimeSinkConfig sinkConfig = GreptimeSinkConfig.builder()
                .endpoints(List.of("127.0.0.1:4001"))
                .build();

        GreptimeBulkWriteConfig bulkWriteConfig = sinkConfig.getBulkWriteConfig();

        assertEquals(GreptimeWriteMode.REGULAR, sinkConfig.getWriteMode());
        assertEquals(GreptimeChangelogMode.INSERT_ONLY, sinkConfig.getChangelogMode());
        assertEquals(GreptimeBulkWriteConfig.DEFAULT_COLUMN_BUFFER_SIZE, bulkWriteConfig.getColumnBufferSize());
        assertEquals(GreptimeBulkWriteConfig.DEFAULT_TIMEOUT_MS_PER_MESSAGE, bulkWriteConfig.getTimeoutMsPerMessage());
        assertEquals(GreptimeBulkWriteConfig.DEFAULT_MAX_REQUESTS_IN_FLIGHT, bulkWriteConfig.getMaxRequestsInFlight());
        assertEquals(
                GreptimeBulkWriteConfig.DEFAULT_ALLOCATOR_INIT_RESERVATION_BYTES,
                bulkWriteConfig.getAllocatorInitReservationBytes());
        assertEquals(
                GreptimeBulkWriteConfig.DEFAULT_ALLOCATOR_MAX_ALLOCATION_BYTES,
                bulkWriteConfig.getAllocatorMaxAllocationBytes());
    }

    @Test
    void describesWriteModeSettingsWithoutCredentials() {
        GreptimeBulkWriteConfig bulkWriteConfig = GreptimeBulkWriteConfig.builder()
                .columnBufferSize(2048)
                .timeoutMsPerMessage(30000L)
                .maxRequestsInFlight(4)
                .allocatorInitReservationBytes(1024L)
                .allocatorMaxAllocationBytes(65536L)
                .build();
        GreptimeSinkConfig sinkConfig = GreptimeSinkConfig.builder()
                .endpoints(List.of("127.0.0.1:4001"))
                .credentials("greptime", "secret")
                .autoCreateTable(false)
                .writeMode(GreptimeWriteMode.BULK)
                .bulkWriteConfig(bulkWriteConfig)
                .build();

        assertEquals(
                "writeMode=bulk,changelogMode=insert-only,bulkColumnBufferSize=2048,bulkTimeoutMsPerMessage=30000,bulkMaxRequestsInFlight=4,bulkAllocatorInitReservationBytes=1024,bulkAllocatorMaxAllocationBytes=65536",
                sinkConfig.describeWriteModeSettings());
    }

    @Test
    void convertsBulkWriteConfigToSdkConfig() {
        GreptimeBulkWriteConfig bulkWriteConfig = GreptimeBulkWriteConfig.builder()
                .timeoutMsPerMessage(30000L)
                .maxRequestsInFlight(4)
                .allocatorInitReservationBytes(1024L)
                .allocatorMaxAllocationBytes(65536L)
                .build();

        BulkWrite.Config sdkConfig = bulkWriteConfig.toSdkConfig();

        assertEquals(30000L, sdkConfig.getTimeoutMsPerMessage());
        assertEquals(4, sdkConfig.getMaxRequestsInFlight());
        assertEquals(1024L, sdkConfig.getAllocatorInitReservation());
        assertEquals(65536L, sdkConfig.getAllocatorMaxAllocation());
    }

    @Test
    void rejectsInvalidBulkWriteConfig() {
        assertEquals(
                "bulkColumnBufferSize must be greater than 0",
                assertThrows(IllegalArgumentException.class, () -> GreptimeBulkWriteConfig.builder()
                                .columnBufferSize(0)
                                .build())
                        .getMessage());
        assertEquals(
                "bulkTimeoutMsPerMessage must be greater than 0",
                assertThrows(IllegalArgumentException.class, () -> GreptimeBulkWriteConfig.builder()
                                .timeoutMsPerMessage(0L)
                                .build())
                        .getMessage());
        assertEquals(
                "bulkMaxRequestsInFlight must be greater than 0",
                assertThrows(IllegalArgumentException.class, () -> GreptimeBulkWriteConfig.builder()
                                .maxRequestsInFlight(0)
                                .build())
                        .getMessage());
        assertEquals(
                "bulkAllocatorInitReservationBytes must be greater than or equal to 0",
                assertThrows(IllegalArgumentException.class, () -> GreptimeBulkWriteConfig.builder()
                                .allocatorInitReservationBytes(-1L)
                                .build())
                        .getMessage());
        assertEquals(
                "bulkAllocatorMaxAllocationBytes must be greater than 0",
                assertThrows(IllegalArgumentException.class, () -> GreptimeBulkWriteConfig.builder()
                                .allocatorMaxAllocationBytes(0L)
                                .build())
                        .getMessage());
        assertEquals(
                "bulkAllocatorMaxAllocationBytes must be greater than or equal to bulkAllocatorInitReservationBytes",
                assertThrows(IllegalArgumentException.class, () -> GreptimeBulkWriteConfig.builder()
                                .allocatorInitReservationBytes(2048L)
                                .allocatorMaxAllocationBytes(1024L)
                                .build())
                        .getMessage());
    }

    @Test
    void rejectsInvalidBulkWriteModeContract() {
        assertEquals(
                "sink.writeMode=bulk requires autoCreateTable=false because Bulk Write does not create tables",
                assertThrows(IllegalArgumentException.class, () -> GreptimeSinkConfig.builder()
                                .endpoints(List.of("127.0.0.1:4001"))
                                .writeMode(GreptimeWriteMode.BULK)
                                .build())
                        .getMessage());
        assertEquals(
                "ttl is not supported when sink.writeMode=bulk",
                assertThrows(IllegalArgumentException.class, () -> GreptimeSinkConfig.builder()
                                .endpoints(List.of("127.0.0.1:4001"))
                                .autoCreateTable(false)
                                .ttl("7d")
                                .writeMode(GreptimeWriteMode.BULK)
                                .build())
                        .getMessage());
        assertEquals(
                "appendMode=true is not supported when sink.writeMode=bulk",
                assertThrows(IllegalArgumentException.class, () -> GreptimeSinkConfig.builder()
                                .endpoints(List.of("127.0.0.1:4001"))
                                .autoCreateTable(false)
                                .appendMode(true)
                                .writeMode(GreptimeWriteMode.BULK)
                                .build())
                        .getMessage());
        assertEquals(
                "mergeMode is not supported when sink.writeMode=bulk",
                assertThrows(IllegalArgumentException.class, () -> GreptimeSinkConfig.builder()
                                .endpoints(List.of("127.0.0.1:4001"))
                                .autoCreateTable(false)
                                .mergeMode("last_row")
                                .writeMode(GreptimeWriteMode.BULK)
                                .build())
                        .getMessage());
    }

    @Test
    void rejectsInvalidChangelogModeContract() {
        assertEquals(
                "sink.changelogMode=retract is not supported when sink.writeMode=bulk",
                assertThrows(IllegalArgumentException.class, () -> GreptimeSinkConfig.builder()
                                .endpoints(List.of("127.0.0.1:4001"))
                                .autoCreateTable(false)
                                .writeMode(GreptimeWriteMode.BULK)
                                .changelogMode(GreptimeChangelogMode.RETRACT)
                                .build())
                        .getMessage());
        assertEquals(
                "appendMode=true cannot be used with sink.changelogMode=retract",
                assertThrows(IllegalArgumentException.class, () -> GreptimeSinkConfig.builder()
                                .endpoints(List.of("127.0.0.1:4001"))
                                .appendMode(true)
                                .changelogMode(GreptimeChangelogMode.RETRACT)
                                .build())
                        .getMessage());
    }

    @Test
    void rejectsNegativeFlushIntervalMs() {
        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> baseConfigBuilder().batchMaxRows(100).flushIntervalMs(-1L).build());

        assertEquals("flushIntervalMs must be greater than or equal to 0", error.getMessage());
    }

    @Test
    void rejectsInvalidProductionSettings() {
        assertEquals(
                "writeMaxRetries must be greater than or equal to 0",
                assertThrows(IllegalArgumentException.class, () -> GreptimeSinkConfig.builder()
                                .endpoints(List.of("127.0.0.1:4001"))
                                .writeMaxRetries(-1)
                                .build())
                        .getMessage());
        assertEquals(
                "writeMaxInFlightPoints must be greater than 0",
                assertThrows(IllegalArgumentException.class, () -> GreptimeSinkConfig.builder()
                                .endpoints(List.of("127.0.0.1:4001"))
                                .writeMaxInFlightPoints(0)
                                .build())
                        .getMessage());
        assertEquals(
                "rpcTimeoutMs must be greater than 0",
                assertThrows(IllegalArgumentException.class, () -> GreptimeSinkConfig.builder()
                                .endpoints(List.of("127.0.0.1:4001"))
                                .rpcTimeoutMs(0)
                                .build())
                        .getMessage());
        assertEquals(
                "routeRefreshPeriodSeconds must be greater than or equal to 0",
                assertThrows(IllegalArgumentException.class, () -> GreptimeSinkConfig.builder()
                                .endpoints(List.of("127.0.0.1:4001"))
                                .routeRefreshPeriodSeconds(-1L)
                                .build())
                        .getMessage());
    }

    @Test
    void rejectsBlankDatabase() {
        assertEquals(
                "database must not be blank",
                assertThrows(IllegalArgumentException.class, () -> GreptimeSinkConfig.builder()
                                .endpoints(List.of("127.0.0.1:4001"))
                                .database(" ")
                                .build())
                        .getMessage());
    }

    @Test
    void rejectsDatabaseWithLeadingOrTrailingWhitespace() {
        assertEquals(
                "database must not have leading or trailing whitespace",
                assertThrows(IllegalArgumentException.class, () -> GreptimeSinkConfig.builder()
                                .endpoints(List.of("127.0.0.1:4001"))
                                .database(" public ")
                                .build())
                        .getMessage());
    }

    @Test
    void rejectsInvalidHintValues() {
        assertEquals(
                "ttl must not be blank",
                assertThrows(IllegalArgumentException.class, () -> GreptimeSinkConfig.builder()
                                .endpoints(List.of("127.0.0.1:4001"))
                                .ttl(" ")
                                .build())
                        .getMessage());
        assertEquals(
                "ttl must not contain ','",
                assertThrows(IllegalArgumentException.class, () -> GreptimeSinkConfig.builder()
                                .endpoints(List.of("127.0.0.1:4001"))
                                .ttl("7d,forever")
                                .build())
                        .getMessage());
        assertEquals(
                "mergeMode must not be blank",
                assertThrows(IllegalArgumentException.class, () -> GreptimeSinkConfig.builder()
                                .endpoints(List.of("127.0.0.1:4001"))
                                .mergeMode("")
                                .build())
                        .getMessage());
        assertEquals(
                "mergeMode must not contain ','",
                assertThrows(IllegalArgumentException.class, () -> GreptimeSinkConfig.builder()
                                .endpoints(List.of("127.0.0.1:4001"))
                                .mergeMode("last_row,last_non_null")
                                .build())
                        .getMessage());
        assertEquals(
                "mergeMode must be one of [last_row, last_non_null], but was: unknown",
                assertThrows(IllegalArgumentException.class, () -> GreptimeSinkConfig.builder()
                                .endpoints(List.of("127.0.0.1:4001"))
                                .mergeMode("unknown")
                                .build())
                        .getMessage());
    }

    @Test
    void normalizesMergeModeHintValue() {
        GreptimeSinkConfig sinkConfig = GreptimeSinkConfig.builder()
                .endpoints(List.of("127.0.0.1:4001"))
                .mergeMode("LAST_ROW")
                .build();

        assertEquals("auto_create_table=true,merge_mode=last_row", sinkConfig.describeWriteHints());
    }

    @Test
    void rejectsInvalidEndpoints() {
        assertEquals(
                "Invalid endpoint, expected host:port: ",
                assertThrows(IllegalArgumentException.class, () -> GreptimeSinkConfig.builder()
                                .endpoints(List.of(""))
                                .build())
                        .getMessage());
        assertEquals(
                "Invalid endpoint, expected host:port: localhost",
                assertThrows(IllegalArgumentException.class, () -> GreptimeSinkConfig.builder()
                                .endpoints(List.of("localhost"))
                                .build())
                        .getMessage());
        assertEquals(
                "Invalid endpoint, expected host:port: localhost:abc",
                assertThrows(IllegalArgumentException.class, () -> GreptimeSinkConfig.builder()
                                .endpoints(List.of("localhost:abc"))
                                .build())
                        .getMessage());
    }

    @Test
    void normalizesEndpointWhitespace() {
        GreptimeSinkConfig sinkConfig = GreptimeSinkConfig.builder()
                .endpoints(List.of(" localhost : 4001 "))
                .build();

        assertEquals(List.of("localhost:4001"), sinkConfig.getEndpoints());
    }

    @Test
    void rejectsIncompleteCredentials() {
        assertEquals(
                "username and password must be configured together",
                assertThrows(IllegalArgumentException.class, () -> GreptimeSinkConfig.builder()
                                .endpoints(List.of("127.0.0.1:4001"))
                                .credentials("greptime", null)
                                .build())
                        .getMessage());
        assertEquals(
                "username and password must be configured together",
                assertThrows(IllegalArgumentException.class, () -> GreptimeSinkConfig.builder()
                                .endpoints(List.of("127.0.0.1:4001"))
                                .credentials(null, "secret")
                                .build())
                        .getMessage());
    }

    @Test
    void normalizesBlankCredentialsToAnonymousAccess() {
        GreptimeSinkConfig sinkConfig = GreptimeSinkConfig.builder()
                .endpoints(List.of("127.0.0.1:4001"))
                .credentials(" ", "")
                .build();

        assertEquals(null, sinkConfig.getUsername());
        assertEquals(null, sinkConfig.getPassword());
    }

    @Test
    void preservesNonBlankCredentialWhitespace() {
        GreptimeSinkConfig sinkConfig = GreptimeSinkConfig.builder()
                .endpoints(List.of("127.0.0.1:4001"))
                .credentials(" greptime ", " secret ")
                .build();

        assertEquals(" greptime ", sinkConfig.getUsername());
        assertEquals(" secret ", sinkConfig.getPassword());
    }

    @Test
    void rejectsAppendModeWithMergeMode() {
        IllegalArgumentException error = assertThrows(IllegalArgumentException.class, () -> GreptimeSinkConfig.builder()
                .endpoints(List.of("127.0.0.1:4001"))
                .appendMode(true)
                .mergeMode("last_non_null")
                .build());

        assertEquals("appendMode=true cannot be used with mergeMode", error.getMessage());
    }

    @Test
    void exposesFlushIntervalSettings() {
        GreptimeSinkConfig disabled =
                baseConfigBuilder().batchMaxRows(100).flushIntervalMs(0L).build();
        GreptimeSinkConfig enabled =
                baseConfigBuilder().batchMaxRows(100).flushIntervalMs(500L).build();

        assertEquals(0L, disabled.getFlushIntervalMs());
        assertEquals(false, disabled.isFlushIntervalEnabled());
        assertEquals(500L, enabled.getFlushIntervalMs());
        assertEquals(true, enabled.isFlushIntervalEnabled());
    }

    @Test
    void acceptsZeroRouteRefreshPeriodSeconds() {
        GreptimeSinkConfig sinkConfig =
                baseConfigBuilder().routeRefreshPeriodSeconds(0L).build();

        assertEquals(0L, sinkConfig.getRouteRefreshPeriodSeconds());
    }

    private static GreptimeSinkConfig.Builder baseConfigBuilder() {
        return GreptimeSinkConfig.builder().endpoints(List.of("127.0.0.1:4001"));
    }
}
