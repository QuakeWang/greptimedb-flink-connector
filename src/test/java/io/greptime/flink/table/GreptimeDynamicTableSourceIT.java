package io.greptime.flink.table;

import static org.junit.jupiter.api.Assertions.assertEquals;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
class GreptimeDynamicTableSourceIT {
    private static final String DEFAULT_GREPTIMEDB_TEST_IMAGE = "greptime/greptimedb:v1.0.1";

    private static final DockerImageName GREPTIMEDB_IMAGE =
            DockerImageName.parse(System.getProperty("greptimedb.test.image", DEFAULT_GREPTIMEDB_TEST_IMAGE));

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
    void readsBoundedRowsWithProjectionReorderAndFilter() throws Exception {
        String greptimeTable = "metrics_source_projection_it";
        createMetricsTable(greptimeTable);
        TableEnvironment tableEnv = newBatchTableEnvironment();

        tableEnv.executeSql(createSourceTableSql("metrics_src", greptimeTable, ""))
                .await();

        assertEquals(
                List.of("0.25|host-1|2026-04-28T15:00", "0.75|host-3|2026-04-28T15:02"),
                collectRows(
                        tableEnv.executeSql(
                                "SELECT cpu, host, ts FROM metrics_src WHERE region = 'eu-west' ORDER BY host"),
                        row -> row.getField(0) + "|" + row.getField(1) + "|" + row.getField(2)));
    }

    @Test
    void readsRowsWrittenThroughGreptimeSink() throws Exception {
        String greptimeTable = "metrics_source_roundtrip_it";
        dropTableIfExists(greptimeTable);
        TableEnvironment tableEnv = newBatchTableEnvironment();

        tableEnv.executeSql(createQuickstartSinkTableSql("metrics_roundtrip_sink", greptimeTable))
                .await();
        tableEnv.executeSql(insertQuickstartRowsSql("metrics_roundtrip_sink")).await();
        waitUntilRowsVisible(greptimeTable, 3L);
        tableEnv.executeSql(createSourceTableSql("metrics_roundtrip_source", greptimeTable, ""))
                .await();

        assertEquals(
                List.of(
                        "host-1|eu-west|0.25|2026-04-28T15:00",
                        "host-2|ap-south|0.5|2026-04-28T15:01",
                        "host-3|us-east|0.75|2026-04-28T15:02"),
                collectRows(
                        tableEnv.executeSql("SELECT host, region, cpu, ts FROM metrics_roundtrip_source ORDER BY host"),
                        row -> row.getField(0)
                                + "|"
                                + row.getField(1)
                                + "|"
                                + row.getField(2)
                                + "|"
                                + row.getField(3)));
    }

    @Test
    void supportsEmptyProjectionForCount() throws Exception {
        String greptimeTable = "metrics_source_count_it";
        createMetricsTable(greptimeTable);
        TableEnvironment tableEnv = newBatchTableEnvironment();

        tableEnv.executeSql(createSourceTableSql("metrics_count_src", greptimeTable, ""))
                .await();

        assertEquals(
                List.of("3"),
                collectRows(tableEnv.executeSql("SELECT COUNT(*) FROM metrics_count_src"), row -> row.getField(0)
                        .toString()));
    }

    @Test
    void sourceDdlMayCarrySinkOptionsWithoutSinkValidation() throws Exception {
        String greptimeTable = "metrics_source_scope_it";
        createMetricsTable(greptimeTable);
        TableEnvironment tableEnv = newBatchTableEnvironment();

        tableEnv.executeSql(createSourceTableSql(
                        "metrics_scope_src",
                        greptimeTable,
                        ", 'endpoints' = '127.0.0.1:1'"
                                + ", 'time-index' = 'missing_ts'"
                                + ", 'tags' = 'missing_tag'"
                                + ", 'sink.write-mode' = 'invalid-write-mode'"))
                .await();

        assertEquals(
                2,
                collectRows(tableEnv.executeSql("SELECT host FROM metrics_scope_src LIMIT 2"), row -> row.getField(0)
                                .toString())
                        .size());
    }

    @Test
    void readsJdbcBackedTypesAndNulls() throws Exception {
        String greptimeTable = "metrics_source_types_it";
        createTypeTable(greptimeTable);
        TableEnvironment tableEnv = newBatchTableEnvironment();

        tableEnv.executeSql(createTypeSourceTableSql("metrics_types_src", greptimeTable))
                .await();

        assertEquals(
                List.of(
                        "host-1|[1, 2, 3]|12.34|2026-05-01|2026-05-01T10:30:15.123|filled|45.67",
                        "host-2|null|null|null|2026-05-01T10:31|null|null"),
                collectRows(
                        tableEnv.executeSql("SELECT host, payload, amount, event_date, ts, nullable_text,"
                                + " nullable_amount FROM metrics_types_src ORDER BY host"),
                        this::formatTypeRow));
    }

    private TableEnvironment newBatchTableEnvironment() {
        return TableEnvironment.create(
                EnvironmentSettings.newInstance().inBatchMode().build());
    }

    private String createSourceTableSql(String flinkTableName, String greptimeTableName, String extraOptions) {
        return String.format(
                "CREATE TEMPORARY TABLE %s ("
                        + " host STRING,"
                        + " region STRING,"
                        + " cpu DOUBLE,"
                        + " ts TIMESTAMP(3)"
                        + ") WITH ("
                        + " 'connector' = 'greptimedb',"
                        + " 'query.jdbc-url' = '%s',"
                        + " 'database' = 'public',"
                        + " 'table' = '%s',"
                        + " 'query.fetch-size' = '2'"
                        + "%s"
                        + ")",
                flinkTableName, jdbcUrl(), greptimeTableName, extraOptions);
    }

    private String createTypeSourceTableSql(String flinkTableName, String greptimeTableName) {
        return String.format(
                "CREATE TEMPORARY TABLE %s ("
                        + " host STRING,"
                        + " payload VARBINARY(16),"
                        + " amount DECIMAL(10, 2),"
                        + " event_date DATE,"
                        + " ts TIMESTAMP(3),"
                        + " nullable_text STRING,"
                        + " nullable_amount DECIMAL(10, 2)"
                        + ") WITH ("
                        + " 'connector' = 'greptimedb',"
                        + " 'query.jdbc-url' = '%s',"
                        + " 'database' = 'public',"
                        + " 'table' = '%s'"
                        + ")",
                flinkTableName, jdbcUrl(), greptimeTableName);
    }

    private String createQuickstartSinkTableSql(String flinkTableName, String greptimeTableName) {
        return String.format(
                "CREATE TEMPORARY TABLE %s ("
                        + " host STRING,"
                        + " region STRING,"
                        + " cpu DOUBLE,"
                        + " ts TIMESTAMP(3) NOT NULL,"
                        + " PRIMARY KEY (host, region) NOT ENFORCED"
                        + ") WITH ("
                        + " 'connector' = 'greptimedb',"
                        + " 'endpoints' = '%s:%d',"
                        + " 'database' = 'public',"
                        + " 'table' = '%s',"
                        + " 'time-index' = 'ts',"
                        + " 'tags' = 'host;region',"
                        + " 'batch.max-rows' = '2'"
                        + ")",
                flinkTableName, GREPTIMEDB.getHost(), GREPTIMEDB.getMappedPort(4001), greptimeTableName);
    }

    private String insertQuickstartRowsSql(String flinkTableName) {
        return "INSERT INTO "
                + flinkTableName
                + " VALUES "
                + "('host-1', 'eu-west', 0.25, TIMESTAMP '2026-04-28 15:00:00.000'),"
                + "('host-2', 'ap-south', 0.50, TIMESTAMP '2026-04-28 15:01:00.000'),"
                + "('host-3', 'us-east', 0.75, TIMESTAMP '2026-04-28 15:02:00.000')";
    }

    private void createMetricsTable(String tableName) throws Exception {
        try (Connection connection = openConnection();
                Statement statement = connection.createStatement()) {
            statement.execute("DROP TABLE IF EXISTS " + tableName);
            statement.execute("CREATE TABLE " + tableName + " ("
                    + "host STRING,"
                    + "`region` STRING,"
                    + "cpu DOUBLE,"
                    + "ts TIMESTAMP(3) TIME INDEX,"
                    + "PRIMARY KEY(host, `region`)"
                    + ")");
            statement.execute("INSERT INTO " + tableName + " VALUES "
                    + "('host-1', 'eu-west', 0.25, '2026-04-28 15:00:00.000'),"
                    + "('host-2', 'ap-south', 0.50, '2026-04-28 15:01:00.000'),"
                    + "('host-3', 'eu-west', 0.75, '2026-04-28 15:02:00.000')");
            waitUntilRowsVisible(tableName, 3L);
        }
    }

    private void dropTableIfExists(String tableName) throws SQLException {
        try (Connection connection = openConnection();
                Statement statement = connection.createStatement()) {
            statement.execute("DROP TABLE IF EXISTS " + tableName);
        }
    }

    private void createTypeTable(String tableName) throws Exception {
        try (Connection connection = openConnection();
                Statement statement = connection.createStatement()) {
            statement.execute("DROP TABLE IF EXISTS " + tableName);
            statement.execute("CREATE TABLE " + tableName + " ("
                    + "host STRING,"
                    + "payload VARBINARY,"
                    + "amount DECIMAL(10, 2),"
                    + "event_date DATE,"
                    + "ts TIMESTAMP(3) TIME INDEX,"
                    + "nullable_text STRING,"
                    + "nullable_amount DECIMAL(10, 2),"
                    + "PRIMARY KEY(host)"
                    + ")");
        }

        try (Connection connection = openConnection();
                PreparedStatement statement = connection.prepareStatement("INSERT INTO " + tableName
                        + " (host, payload, amount, event_date, ts, nullable_text, nullable_amount)"
                        + " VALUES (?, ?, ?, ?, ?, ?, ?)")) {
            statement.setString(1, "host-1");
            statement.setBytes(2, new byte[] {1, 2, 3});
            statement.setBigDecimal(3, new BigDecimal("12.34"));
            statement.setDate(4, Date.valueOf("2026-05-01"));
            statement.setTimestamp(5, Timestamp.valueOf("2026-05-01 10:30:15.123"));
            statement.setString(6, "filled");
            statement.setBigDecimal(7, new BigDecimal("45.67"));
            statement.executeUpdate();

            statement.setString(1, "host-2");
            statement.setNull(2, Types.VARBINARY);
            statement.setNull(3, Types.DECIMAL);
            statement.setNull(4, Types.DATE);
            statement.setTimestamp(5, Timestamp.valueOf("2026-05-01 10:31:00.000"));
            statement.setNull(6, Types.VARCHAR);
            statement.setNull(7, Types.DECIMAL);
            statement.executeUpdate();
        }

        waitUntilRowsVisible(tableName, 2L);
    }

    private void waitUntilRowsVisible(String tableName, long expectedRows) throws Exception {
        SQLException lastError = null;
        for (int i = 0; i < 20; i++) {
            try {
                if (queryRowCount(tableName) == expectedRows) {
                    return;
                }
            } catch (SQLException e) {
                lastError = e;
            }
            Thread.sleep(500L);
        }
        if (lastError != null) {
            throw lastError;
        }
        throw new AssertionError("Expected rows were not visible for table: " + tableName);
    }

    private long queryRowCount(String tableName) throws SQLException {
        try (Connection connection = openConnection();
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery("SELECT COUNT(*) FROM " + tableName)) {
            if (!resultSet.next()) {
                throw new SQLException("Expected a count row");
            }
            return resultSet.getLong(1);
        }
    }

    private List<String> collectRows(TableResult result, RowFormatter formatter) throws Exception {
        List<String> rows = new ArrayList<>();
        try (CloseableIterator<Row> iterator = result.collect()) {
            while (iterator.hasNext()) {
                rows.add(formatter.format(iterator.next()));
            }
        }
        return rows;
    }

    private Connection openConnection() throws SQLException {
        return DriverManager.getConnection(jdbcUrl());
    }

    private String formatTypeRow(Row row) {
        byte[] payload = (byte[]) row.getField(1);
        return row.getField(0)
                + "|"
                + (payload == null ? "null" : Arrays.toString(payload))
                + "|"
                + row.getField(2)
                + "|"
                + row.getField(3)
                + "|"
                + row.getField(4)
                + "|"
                + row.getField(5)
                + "|"
                + row.getField(6);
    }

    private String jdbcUrl() {
        return String.format(
                "jdbc:mysql://%s:%d/public?useSSL=false&allowPublicKeyRetrieval=true",
                GREPTIMEDB.getHost(), GREPTIMEDB.getMappedPort(4002));
    }

    @FunctionalInterface
    private interface RowFormatter {
        String format(Row row);
    }
}
