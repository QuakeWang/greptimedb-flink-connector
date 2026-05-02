package io.greptime.flink.sink;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import io.greptime.BulkWrite;
import io.greptime.GreptimeDB;
import io.greptime.flink.cfg.GreptimeSinkConfig;
import io.greptime.flink.connection.GreptimeClientFactory;
import io.greptime.flink.sink.schema.GreptimeTableSchema;
import io.greptime.rpc.Context;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
class GreptimeBulkWriteProbeIT {
    private static final String DEFAULT_GREPTIMEDB_TEST_IMAGE = "greptime/greptimedb:v1.0.1";
    private static final DockerImageName GREPTIMEDB_IMAGE =
            DockerImageName.parse(System.getProperty("greptimedb.test.image", DEFAULT_GREPTIMEDB_TEST_IMAGE));
    private static final int COLUMN_BUFFER_SIZE = 2;
    private static final BulkWrite.Config BULK_CONFIG = BulkWrite.Config.newBuilder()
            .allocatorInitReservation(0)
            .allocatorMaxAllocation(64 * 1024 * 1024L)
            .timeoutMsPerMessage(30000)
            .maxRequestsInFlight(4)
            .build();

    @Container
    private static final GenericContainer<?> GREPTIMEDB = new GenericContainer<>(GREPTIMEDB_IMAGE)
            .withExposedPorts(4000, 4001, 4002, 4003)
            .withCommand(
                    "standalone",
                    "start",
                    "--http-addr",
                    "0.0.0.0:4000",
                    "--rpc-bind-addr",
                    "0.0.0.0:4001",
                    "--mysql-addr",
                    "0.0.0.0:4002",
                    "--postgres-addr",
                    "0.0.0.0:4003")
            .waitingFor(Wait.forHttp("/health").forPort(4000).forStatusCode(200))
            .withStartupTimeout(Duration.ofMinutes(2));

    @Test
    void writesRowsToPreCreatedTableWithCurrentSchemaAndConverter() throws Exception {
        String tableName = uniqueTableName("success");
        dropTableIfExists(tableName);
        createMetricsTable(tableName);

        try {
            GreptimeBulkWriteProbe.Result result = writeRows(metricsSchema(tableName), metricRows());

            assertEquals(3, result.getInputRows());
            assertEquals(3, result.getAffectedRows());
            assertTrue(result.getBytesUsed() > 0, "Bulk probe should expose off-heap bytes used");
            assertEquals(
                    List.of(
                            "host-1|eu-west|0.25|2026-04-28T15:00",
                            "host-2|ap-south|0.5|2026-04-28T15:01",
                            "host-3|us-east|0.75|2026-04-28T15:02"),
                    queryRows(tableName));
        } finally {
            dropTableIfExists(tableName);
        }
    }

    @Test
    void surfacesDiagnosticErrorWhenBulkTableDoesNotExist() throws Exception {
        String tableName = uniqueTableName("missing");
        dropTableIfExists(tableName);

        IOException error = assertThrows(IOException.class, () -> writeRows(metricsSchema(tableName), metricRows()));

        String messages = collectMessages(error);
        assertTrue(messages.contains("Bulk write probe failed"), messages);
        assertTrue(messages.contains("table=" + tableName), messages);
        assertTrue(messages.contains("inputRows=3"), messages);
        assertTrue(messages.contains("columnBufferSize=" + COLUMN_BUFFER_SIZE), messages);
        assertTrue(messages.contains("maxRequestsInFlight=4"), messages);
        assertCauseContainsAny(error, "not found", "missing", "execute grpc request error");
        assertFalse(tableExists(tableName));
    }

    @Test
    void surfacesDiagnosticErrorWhenBulkSchemaDoesNotMatchExistingTable() throws Exception {
        String tableName = uniqueTableName("schema_mismatch");
        dropTableIfExists(tableName);
        createMetricsTableWithDifferentTimeIndex(tableName);

        try {
            IOException error =
                    assertThrows(IOException.class, () -> writeRows(metricsSchema(tableName), metricRows()));

            String messages = collectMessages(error);
            assertTrue(messages.contains("Bulk write probe failed"), messages);
            assertTrue(messages.contains("table=" + tableName), messages);
            assertTrue(messages.contains("inputRows=3"), messages);
            assertTrue(messages.contains("columnBufferSize=" + COLUMN_BUFFER_SIZE), messages);
            assertCauseContainsAny(
                    error,
                    "timestamp column",
                    "not found",
                    "missing",
                    "schema",
                    "mismatch",
                    "execute grpc request error");
        } finally {
            dropTableIfExists(tableName);
        }
    }

    @Test
    void surfacesDiagnosticErrorWhenBulkColumnTypeDoesNotMatchExistingTable() throws Exception {
        String tableName = uniqueTableName("type_mismatch");
        dropTableIfExists(tableName);
        createMetricsTable(tableName);

        try {
            IOException error = assertThrows(
                    IOException.class,
                    () -> writeRows(metricsSchemaWithStringCpu(tableName), metricRowsWithStringCpu()));

            String messages = collectMessages(error);
            assertTrue(messages.contains("Bulk write probe failed"), messages);
            assertTrue(messages.contains("table=" + tableName), messages);
            assertTrue(messages.contains("inputRows=1"), messages);
            assertTrue(messages.contains("columnBufferSize=" + COLUMN_BUFFER_SIZE), messages);
            assertCauseContainsAny(error, "type", "convert", "cast", "mismatch", "execute grpc request error");
        } finally {
            dropTableIfExists(tableName);
        }
    }

    private GreptimeBulkWriteProbe.Result writeRows(GreptimeTableSchema tableSchema, List<RowData> rows)
            throws IOException {
        GreptimeDB client = new GreptimeClientFactory().create(sinkConfig());
        try {
            return new GreptimeBulkWriteProbe(
                            client, tableSchema, BULK_CONFIG, Context.newDefault(), COLUMN_BUFFER_SIZE)
                    .writeRows(rows);
        } finally {
            client.shutdownGracefully();
        }
    }

    private GreptimeSinkConfig sinkConfig() {
        return GreptimeSinkConfig.builder()
                .endpoints(List.of(GREPTIMEDB.getHost() + ":" + GREPTIMEDB.getMappedPort(4001)))
                .database("public")
                .build();
    }

    private GreptimeTableSchema metricsSchema(String tableName) {
        return GreptimeTableSchema.from(
                tableName,
                DataTypes.ROW(
                        DataTypes.FIELD("host", DataTypes.STRING()),
                        DataTypes.FIELD("region", DataTypes.STRING()),
                        DataTypes.FIELD("cpu", DataTypes.DOUBLE()),
                        DataTypes.FIELD("ts", DataTypes.TIMESTAMP(3).notNull())),
                "ts",
                List.of("host", "region"));
    }

    private GreptimeTableSchema metricsSchemaWithStringCpu(String tableName) {
        return GreptimeTableSchema.from(
                tableName,
                DataTypes.ROW(
                        DataTypes.FIELD("host", DataTypes.STRING()),
                        DataTypes.FIELD("region", DataTypes.STRING()),
                        DataTypes.FIELD("cpu", DataTypes.STRING()),
                        DataTypes.FIELD("ts", DataTypes.TIMESTAMP(3).notNull())),
                "ts",
                List.of("host", "region"));
    }

    private List<RowData> metricRows() {
        return List.of(
                metricRow("host-1", "eu-west", 0.25d, LocalDateTime.of(2026, 4, 28, 15, 0)),
                metricRow("host-2", "ap-south", 0.50d, LocalDateTime.of(2026, 4, 28, 15, 1)),
                metricRow("host-3", "us-east", 0.75d, LocalDateTime.of(2026, 4, 28, 15, 2)));
    }

    private RowData metricRow(String host, String region, double cpu, LocalDateTime timestamp) {
        return GenericRowData.of(
                StringData.fromString(host),
                StringData.fromString(region),
                cpu,
                TimestampData.fromLocalDateTime(timestamp));
    }

    private List<RowData> metricRowsWithStringCpu() {
        return List.of(GenericRowData.of(
                StringData.fromString("host-1"),
                StringData.fromString("eu-west"),
                StringData.fromString("0.25"),
                TimestampData.fromLocalDateTime(LocalDateTime.of(2026, 4, 28, 15, 0))));
    }

    private void createMetricsTable(String tableName) throws SQLException {
        executeUpdate("CREATE TABLE "
                + tableName
                + " ("
                + "host STRING,"
                + "`region` STRING,"
                + "cpu DOUBLE,"
                + "ts TIMESTAMP(3) TIME INDEX,"
                + "PRIMARY KEY (host, `region`)"
                + ")");
    }

    private void createMetricsTableWithDifferentTimeIndex(String tableName) throws SQLException {
        executeUpdate("CREATE TABLE "
                + tableName
                + " ("
                + "host STRING,"
                + "`region` STRING,"
                + "cpu DOUBLE,"
                + "event_ts TIMESTAMP(3) TIME INDEX,"
                + "PRIMARY KEY (host, `region`)"
                + ")");
    }

    private List<String> queryRows(String tableName) throws Exception {
        return queryRows(
                "SELECT host, region, cpu, ts FROM " + tableName + " ORDER BY host",
                resultSet -> String.format(
                        "%s|%s|%s|%s",
                        resultSet.getString("host"),
                        resultSet.getString("region"),
                        resultSet.getObject("cpu"),
                        resultSet.getTimestamp("ts").toLocalDateTime()));
    }

    private List<String> queryRows(String sql, ResultSetFormatter formatter) throws Exception {
        SQLException lastError = null;
        for (int i = 0; i < 20; i++) {
            try (Connection connection = openConnection();
                    Statement statement = connection.createStatement();
                    ResultSet resultSet = statement.executeQuery(sql)) {
                List<String> rows = new ArrayList<>();
                while (resultSet.next()) {
                    rows.add(formatter.format(resultSet));
                }
                return rows;
            } catch (SQLException e) {
                lastError = e;
                Thread.sleep(500L);
            }
        }
        throw lastError;
    }

    private boolean tableExists(String tableName) throws Exception {
        return queryRows("SHOW TABLES", resultSet -> resultSet.getString(1)).contains(tableName);
    }

    private void dropTableIfExists(String tableName) throws SQLException {
        executeUpdate("DROP TABLE IF EXISTS " + tableName);
    }

    private void executeUpdate(String sql) throws SQLException {
        try (Connection connection = openConnection();
                Statement statement = connection.createStatement()) {
            statement.executeUpdate(sql);
        }
    }

    private Connection openConnection() throws SQLException {
        String jdbcUrl = String.format(
                "jdbc:mysql://%s:%d/public?useSSL=false&allowPublicKeyRetrieval=true",
                GREPTIMEDB.getHost(), GREPTIMEDB.getMappedPort(4002));
        return DriverManager.getConnection(jdbcUrl);
    }

    private String collectMessages(Throwable error) {
        StringBuilder builder = new StringBuilder();
        Throwable current = error;
        while (current != null) {
            if (builder.length() > 0) {
                builder.append(" | ");
            }
            builder.append(current.getClass().getSimpleName());
            builder.append(": ");
            builder.append(current.getMessage());
            current = current.getCause();
        }
        return builder.toString();
    }

    private void assertCauseContainsAny(Throwable error, String... keywords) {
        String causeMessages = collectMessages(error.getCause()).toLowerCase(Locale.ROOT);
        for (String keyword : keywords) {
            if (causeMessages.contains(keyword)) {
                return;
            }
        }
        throw new AssertionError("Expected cause chain to contain one of " + List.of(keywords) + ", actual messages: "
                + collectMessages(error));
    }

    private String uniqueTableName(String suffix) {
        return "m5_bulk_probe_" + suffix + "_" + System.nanoTime();
    }

    @FunctionalInterface
    private interface ResultSetFormatter {
        String format(ResultSet resultSet) throws SQLException;
    }
}
