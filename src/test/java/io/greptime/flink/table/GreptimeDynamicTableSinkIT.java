package io.greptime.flink.table;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
class GreptimeDynamicTableSinkIT {
    private static final String METRICS_COLUMNS =
            "host STRING," + " region STRING," + " cpu DOUBLE," + " ts TIMESTAMP(3) NOT NULL";
    private static final String MERGE_COLUMNS =
            "host STRING," + " region STRING," + " cpu DOUBLE," + " mem DOUBLE," + " ts TIMESTAMP(3) NOT NULL";
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
    void insertsRowsIntoAutoCreatedTable() throws Exception {
        TableEnvironment tableEnv = newBatchTableEnvironment();

        tableEnv.executeSql(createSinkTableSql("metrics_sink", "metrics_sink", METRICS_COLUMNS, "host;region", ""))
                .await();
        tableEnv.executeSql(insertRowsSql("metrics_sink")).await();

        assertEquals(
                List.of(
                        "host-1|eu-west|0.25|2026-04-28T15:00",
                        "host-2|ap-south|0.5|2026-04-28T15:01",
                        "host-3|us-east|0.75|2026-04-28T15:02"),
                queryRows("metrics_sink"));
    }

    @Test
    void bulkModeWritesRowsIntoPrecreatedTable() throws Exception {
        TableEnvironment tableEnv = newBatchTableEnvironment();
        String tableName = "metrics_bulk_sink";

        createMetricsTable(tableName);
        tableEnv.executeSql(createSinkTableSql(
                        "metrics_bulk_sink",
                        tableName,
                        METRICS_COLUMNS,
                        "host;region",
                        ", 'sink.write-mode' = 'bulk'"
                                + ", 'auto-create-table' = 'false'"
                                + ", 'bulk.column-buffer-size' = '1024'"
                                + ", 'bulk.timeout-ms-per-message' = '60000'"
                                + ", 'bulk.max-requests-in-flight' = '8'"))
                .await();

        tableEnv.executeSql(insertRowsSql("metrics_bulk_sink")).await();

        assertEquals(
                List.of(
                        "host-1|eu-west|0.25|2026-04-28T15:00",
                        "host-2|ap-south|0.5|2026-04-28T15:01",
                        "host-3|us-east|0.75|2026-04-28T15:02"),
                queryRows(tableName));
    }

    @Test
    void bulkPreflightWritesRowsIntoPrecreatedTable() throws Exception {
        TableEnvironment tableEnv = newBatchTableEnvironment();
        String tableName = "metrics_bulk_preflight_sink";

        dropTableIfExists(tableName);
        createMetricsTable(tableName);
        tableEnv.executeSql(createSinkTableSql(
                        "metrics_bulk_preflight_sink",
                        tableName,
                        METRICS_COLUMNS,
                        "host;region",
                        bulkOptions() + preflightOptions()))
                .await();

        tableEnv.executeSql(insertRowsSql("metrics_bulk_preflight_sink")).await();

        assertEquals(
                List.of(
                        "host-1|eu-west|0.25|2026-04-28T15:00",
                        "host-2|ap-south|0.5|2026-04-28T15:01",
                        "host-3|us-east|0.75|2026-04-28T15:02"),
                queryRows(tableName));
    }

    @Test
    void bulkPreflightFailsBeforeGrpcClientWhenTableIsMissing() throws Exception {
        TableEnvironment tableEnv = newBatchTableEnvironment();

        tableEnv.executeSql(createSinkTableSql(
                        "metrics_bulk_preflight_missing",
                        "missing_metrics_bulk_preflight",
                        METRICS_COLUMNS,
                        "host;region",
                        bulkOptions() + preflightOptions()))
                .await();

        Exception error =
                assertThrows(Exception.class, () -> tableEnv.executeSql(insertRowsSql("metrics_bulk_preflight_missing"))
                        .await());

        String messages = collectMessages(error).toLowerCase();
        assertTrue(messages.contains("greptimedb preflight failed"), messages);
        assertTrue(messages.contains("reason=missing-table"), messages);
        assertFalse(messages.contains("failed to bulk write rows to greptimedb"), messages);
        assertFalse(tableExists("missing_metrics_bulk_preflight"));
    }

    @Test
    void bulkPreflightFailsBeforeGrpcClientWhenColumnTypeMismatches() throws Exception {
        TableEnvironment tableEnv = newBatchTableEnvironment();
        String tableName = "metrics_bulk_preflight_type_mismatch";
        dropTableIfExists(tableName);
        createMetricsTableWithStringCpu(tableName);

        tableEnv.executeSql(createSinkTableSql(
                        "metrics_bulk_preflight_type_mismatch",
                        tableName,
                        METRICS_COLUMNS,
                        "host;region",
                        bulkOptions() + preflightOptions()))
                .await();

        Exception error = assertThrows(
                Exception.class, () -> tableEnv.executeSql(insertRowsSql("metrics_bulk_preflight_type_mismatch"))
                        .await());

        String messages = collectMessages(error).toLowerCase();
        assertTrue(messages.contains("greptimedb preflight failed"), messages);
        assertTrue(messages.contains("reason=type-mismatch"), messages);
        assertTrue(messages.contains("column cpu"), messages);
        assertFalse(messages.contains("failed to bulk write rows to greptimedb"), messages);
    }

    @Test
    void bulkPreflightFailsBeforeGrpcClientWhenPrimaryKeySetMismatches() throws Exception {
        TableEnvironment tableEnv = newBatchTableEnvironment();
        String tableName = "metrics_bulk_preflight_key_mismatch";
        dropTableIfExists(tableName);
        createMetricsTableWithHostPrimaryKey(tableName);

        tableEnv.executeSql(createSinkTableSql(
                        "metrics_bulk_preflight_key_mismatch",
                        tableName,
                        METRICS_COLUMNS,
                        "host;region",
                        bulkOptions() + preflightOptions()))
                .await();

        Exception error = assertThrows(
                Exception.class, () -> tableEnv.executeSql(insertRowsSql("metrics_bulk_preflight_key_mismatch"))
                        .await());

        String messages = collectMessages(error).toLowerCase();
        assertTrue(messages.contains("greptimedb preflight failed"), messages);
        assertTrue(messages.contains("reason=row-key-set-mismatch"), messages);
        assertFalse(messages.contains("failed to bulk write rows to greptimedb"), messages);
    }

    @Test
    void bulkModeSurfacesMissingTableDiagnostic() throws Exception {
        TableEnvironment tableEnv = newBatchTableEnvironment();

        tableEnv.executeSql(createSinkTableSql(
                        "metrics_bulk_missing_table",
                        "missing_metrics_bulk_table",
                        METRICS_COLUMNS,
                        "host;region",
                        ", 'sink.write-mode' = 'bulk', 'auto-create-table' = 'false'"))
                .await();

        Exception error =
                assertThrows(Exception.class, () -> tableEnv.executeSql(insertRowsSql("metrics_bulk_missing_table"))
                        .await());

        String messages = collectMessages(error).toLowerCase();
        assertTrue(messages.contains("failed to bulk write rows to greptimedb"));
        assertTrue(messages.contains("writemode=bulk"));
        assertTrue(messages.contains("table=missing_metrics_bulk_table"));
        assertTrue(messages.contains("rows=2"));
        assertTrue(messages.contains("not found") || messages.contains("missing") || messages.contains("table"));
    }

    @Test
    void flushesLowVolumeStreamingRowsBeforeJobFinishes() throws Exception {
        TableEnvironment tableEnv = newStreamingTableEnvironment();

        tableEnv.executeSql("CREATE TEMPORARY TABLE metrics_low_rate_source ("
                        + " host STRING,"
                        + " region STRING,"
                        + " cpu DOUBLE"
                        + ") WITH ("
                        + " 'connector' = 'datagen',"
                        + " 'rows-per-second' = '1',"
                        + " 'fields.host.length' = '8',"
                        + " 'fields.region.length' = '8'"
                        + ")")
                .await();
        tableEnv.executeSql(String.format(
                        "CREATE TEMPORARY TABLE metrics_low_rate_sink ("
                                + " host STRING,"
                                + " region STRING,"
                                + " cpu DOUBLE,"
                                + " ts TIMESTAMP(3) NOT NULL"
                                + ") WITH ("
                                + " 'connector' = 'greptimedb',"
                                + " 'endpoints' = '%s:%d',"
                                + " 'database' = 'public',"
                                + " 'table' = 'metrics_low_rate_sink',"
                                + " 'time-index' = 'ts',"
                                + " 'tags' = 'host;region',"
                                + " 'batch.max-rows' = '100',"
                                + " 'flush.interval-ms' = '500'"
                                + ")",
                        GREPTIMEDB.getHost(), GREPTIMEDB.getMappedPort(4001)))
                .await();

        TableResult insertResult = tableEnv.executeSql("INSERT INTO metrics_low_rate_sink "
                + "SELECT host, region, cpu, CAST(CURRENT_TIMESTAMP AS TIMESTAMP(3)) "
                + "FROM metrics_low_rate_source");
        JobClient jobClient = insertResult
                .getJobClient()
                .orElseThrow(() -> new IllegalStateException("Expected streaming INSERT to expose a job client"));

        try {
            assertTrue(waitUntilRowsVisibleBeforeJobFinishes(jobClient, "metrics_low_rate_sink"));
        } finally {
            if (!jobClient.getJobStatus().get().isTerminalState()) {
                jobClient.cancel().get();
            }
        }
    }

    @Test
    void failsWhenAutoCreateTableIsDisabled() throws Exception {
        TableEnvironment tableEnv = newBatchTableEnvironment();

        tableEnv.executeSql(createSinkTableSql(
                        "metrics_sink_no_auto_create",
                        "missing_metrics_table",
                        METRICS_COLUMNS,
                        "host;region",
                        ", 'auto-create-table' = 'false'"))
                .await();

        Exception error =
                assertThrows(Exception.class, () -> tableEnv.executeSql(insertRowsSql("metrics_sink_no_auto_create"))
                        .await());

        String messages = collectMessages(error);
        assertTrue(messages.contains("Failed to write rows to GreptimeDB"));
        assertTrue(messages.contains("table=missing_metrics_table"));
        assertTrue(messages.contains("database=public"));
        assertTrue(messages.contains("writeHints=auto_create_table=false"));
        assertFalse(tableExists("missing_metrics_table"));
    }

    @Test
    void createsInstantTtlTableAndDoesNotExposeRows() throws Exception {
        TableEnvironment tableEnv = newBatchTableEnvironment();

        tableEnv.executeSql(createSinkTableSql(
                        "metrics_sink_ttl_instant",
                        "metrics_ttl_instant",
                        METRICS_COLUMNS,
                        "host;region",
                        ", 'ttl' = 'instant'"))
                .await();
        tableEnv.executeSql(insertRowsSql("metrics_sink_ttl_instant")).await();

        assertEquals(0L, queryRowCount("SELECT count(*) FROM metrics_ttl_instant"));
        assertTrue(showCreateTable("metrics_ttl_instant").contains("ttl = 'instant'"));
    }

    @Test
    void preservesDuplicatesWhenAppendModeIsEnabled() throws Exception {
        TableEnvironment tableEnv = newBatchTableEnvironment();

        tableEnv.executeSql(createSinkTableSql(
                        "metrics_sink_append_mode",
                        "metrics_append_mode",
                        METRICS_COLUMNS,
                        "host;region",
                        ", 'append-mode' = 'true'"))
                .await();
        tableEnv.executeSql(insertValuesSql(
                        "metrics_sink_append_mode",
                        "('host-1', 'eu-west', 1.0, TIMESTAMP '2026-04-28 15:00:00.000'),"
                                + "('host-2', 'ap-south', 2.0, TIMESTAMP '2026-04-28 15:01:00.000')"))
                .await();
        tableEnv.executeSql(insertValuesSql(
                        "metrics_sink_append_mode",
                        "('host-1', 'eu-west', 10.0, TIMESTAMP '2026-04-28 15:00:00.000'),"
                                + "('host-1', 'eu-west', 11.0, TIMESTAMP '2026-04-28 15:00:00.000')"))
                .await();

        assertEquals(
                List.of(
                        "host-1|eu-west|1.0|2026-04-28T15:00",
                        "host-1|eu-west|10.0|2026-04-28T15:00",
                        "host-1|eu-west|11.0|2026-04-28T15:00",
                        "host-2|ap-south|2.0|2026-04-28T15:01"),
                queryRows(
                        "SELECT host, region, cpu, ts FROM metrics_append_mode ORDER BY host, region, ts, cpu",
                        resultSet -> String.format(
                                "%s|%s|%s|%s",
                                resultSet.getString("host"),
                                resultSet.getString("region"),
                                resultSet.getObject("cpu"),
                                resultSet.getTimestamp("ts").toLocalDateTime())));
        assertTrue(showCreateTable("metrics_append_mode").contains("append_mode = 'true'"));
    }

    @Test
    void mergesRowsWhenMergeModeIsLastNonNull() throws Exception {
        TableEnvironment tableEnv = newBatchTableEnvironment();

        tableEnv.executeSql(createSinkTableSql(
                        "metrics_sink_merge_mode",
                        "metrics_merge_mode",
                        MERGE_COLUMNS,
                        "host;region",
                        ", 'merge-mode' = 'last_non_null'"))
                .await();
        tableEnv.executeSql(
                        insertValuesSql(
                                "metrics_sink_merge_mode",
                                "('host-1', 'eu-west', 1.0, CAST(NULL AS DOUBLE), TIMESTAMP '2026-04-28 15:00:00.000'),"
                                        + "('host-1', 'eu-west', CAST(NULL AS DOUBLE), CAST(NULL AS DOUBLE), TIMESTAMP '2026-04-28 15:01:00.000'),"
                                        + "('host-1', 'eu-west', CAST(NULL AS DOUBLE), 3.0, TIMESTAMP '2026-04-28 15:02:00.000')"))
                .await();
        tableEnv.executeSql(
                        insertValuesSql(
                                "metrics_sink_merge_mode",
                                "('host-1', 'eu-west', 12.0, CAST(NULL AS DOUBLE), TIMESTAMP '2026-04-28 15:01:00.000'),"
                                        + "('host-1', 'eu-west', 13.0, CAST(NULL AS DOUBLE), TIMESTAMP '2026-04-28 15:02:00.000')"))
                .await();
        tableEnv.executeSql(insertValuesSql(
                        "metrics_sink_merge_mode",
                        "('host-1', 'eu-west', 11.0, CAST(NULL AS DOUBLE), TIMESTAMP '2026-04-28 15:00:00.000'),"
                                + "('host-1', 'eu-west', 22.0, 222.0, TIMESTAMP '2026-04-28 15:01:00.000')"))
                .await();

        assertEquals(
                List.of(
                        "host-1|eu-west|11.0|null|2026-04-28T15:00",
                        "host-1|eu-west|22.0|222.0|2026-04-28T15:01",
                        "host-1|eu-west|13.0|3.0|2026-04-28T15:02"),
                queryRows(
                        "SELECT host, region, cpu, mem, ts FROM metrics_merge_mode ORDER BY host, region, ts",
                        resultSet -> String.format(
                                "%s|%s|%s|%s|%s",
                                resultSet.getString("host"),
                                resultSet.getString("region"),
                                nullableValue(resultSet, "cpu"),
                                nullableValue(resultSet, "mem"),
                                resultSet.getTimestamp("ts").toLocalDateTime())));
        assertTrue(showCreateTable("metrics_merge_mode").contains("merge_mode = 'last_non_null'"));
    }

    @Test
    void tableEnvironmentWritesRetractChangelogRows() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(
                env, EnvironmentSettings.newInstance().inStreamingMode().build());
        String tableName = "metrics_retract_table_env";

        createMetricsTable(tableName);
        DataStream<Row> source = env.fromElements(
                        Row.ofKind(RowKind.INSERT, "host-1", "eu-west", 0.25d, LocalDateTime.of(2026, 4, 28, 15, 0)),
                        Row.ofKind(RowKind.DELETE, "host-1", "eu-west", 0.25d, LocalDateTime.of(2026, 4, 28, 15, 0)),
                        Row.ofKind(RowKind.INSERT, "host-2", "eu-west", 0.50d, LocalDateTime.of(2026, 4, 28, 15, 1)),
                        Row.ofKind(
                                RowKind.UPDATE_BEFORE,
                                "host-2",
                                "eu-west",
                                0.50d,
                                LocalDateTime.of(2026, 4, 28, 15, 1)),
                        Row.ofKind(
                                RowKind.UPDATE_AFTER, "host-3", "eu-west", 0.75d, LocalDateTime.of(2026, 4, 28, 15, 2)))
                .returns(Types.ROW_NAMED(
                        new String[] {"host", "region", "cpu", "ts"},
                        Types.STRING,
                        Types.STRING,
                        Types.DOUBLE,
                        Types.LOCAL_DATE_TIME));
        tableEnv.createTemporaryView(
                "metrics_retract_source",
                tableEnv.fromChangelogStream(
                        source,
                        Schema.newBuilder()
                                .column("host", "STRING")
                                .column("region", "STRING")
                                .column("cpu", "DOUBLE")
                                .column("ts", "TIMESTAMP(3)")
                                .build()));
        tableEnv.executeSql(createSinkTableSql(
                        "metrics_retract_sink",
                        tableName,
                        METRICS_COLUMNS + ", PRIMARY KEY (host, region) NOT ENFORCED",
                        "host;region",
                        ", 'sink.changelog-mode' = 'retract'"
                                + ", 'sink.parallelism' = '1'"
                                + ", 'auto-create-table' = 'false'"))
                .await();

        tableEnv.executeSql("INSERT INTO metrics_retract_sink SELECT * FROM metrics_retract_source")
                .await();

        assertEquals(List.of("host-3|eu-west|0.75|2026-04-28T15:02"), queryRows(tableName));
    }

    @Test
    void shadedJarSupportsFactoryDiscoveryAndMinimalWrite() throws Exception {
        String tableName = "metrics_shaded_smoke";
        ProcessResult result = runShadedJarProbe(tableName);

        assertEquals(0, result.exitCode, result.output);
        assertEquals(
                List.of("host-shaded|0.42|2026-04-28T15:00"),
                queryRows(
                        "SELECT host, cpu, ts FROM " + tableName + " ORDER BY host",
                        resultSet -> String.format(
                                "%s|%s|%s",
                                resultSet.getString("host"),
                                resultSet.getObject("cpu"),
                                resultSet.getTimestamp("ts").toLocalDateTime())));
    }

    private TableEnvironment newBatchTableEnvironment() {
        return TableEnvironment.create(
                EnvironmentSettings.newInstance().inBatchMode().build());
    }

    private TableEnvironment newStreamingTableEnvironment() {
        return TableEnvironment.create(
                EnvironmentSettings.newInstance().inStreamingMode().build());
    }

    private String createSinkTableSql(
            String flinkTableName,
            String greptimeTableName,
            String columnDefinitions,
            String tags,
            String extraOptions) {
        return String.format(
                "CREATE TEMPORARY TABLE %s ("
                        + " %s"
                        + ") WITH ("
                        + " 'connector' = 'greptimedb',"
                        + " 'endpoints' = '%s:%d',"
                        + " 'database' = 'public',"
                        + " 'table' = '%s',"
                        + " 'time-index' = 'ts',"
                        + " 'tags' = '%s',"
                        + " 'batch.max-rows' = '2'"
                        + "%s"
                        + ")",
                flinkTableName,
                columnDefinitions,
                GREPTIMEDB.getHost(),
                GREPTIMEDB.getMappedPort(4001),
                greptimeTableName,
                tags,
                extraOptions);
    }

    private String bulkOptions() {
        return ", 'sink.write-mode' = 'bulk'"
                + ", 'auto-create-table' = 'false'"
                + ", 'bulk.column-buffer-size' = '1024'"
                + ", 'bulk.timeout-ms-per-message' = '60000'"
                + ", 'bulk.max-requests-in-flight' = '8'";
    }

    private String preflightOptions() {
        return ", 'preflight.enabled' = 'true'" + ", 'query.jdbc-url' = '" + jdbcUrl() + "'";
    }

    private String insertRowsSql(String flinkTableName) {
        return insertValuesSql(
                flinkTableName,
                "('host-1', 'eu-west', 0.25, TIMESTAMP '2026-04-28 15:00:00.000'),"
                        + "('host-2', 'ap-south', 0.50, TIMESTAMP '2026-04-28 15:01:00.000'),"
                        + "('host-3', 'us-east', 0.75, TIMESTAMP '2026-04-28 15:02:00.000')");
    }

    private List<String> queryRows(String tableName) throws Exception {
        return queryRows(
                String.format("SELECT host, region, cpu, ts FROM %s ORDER BY host", tableName),
                resultSet -> String.format(
                        "%s|%s|%s|%s",
                        resultSet.getString("host"),
                        resultSet.getString("region"),
                        resultSet.getObject("cpu"),
                        resultSet.getTimestamp("ts").toLocalDateTime()));
    }

    private String insertValuesSql(String flinkTableName, String valuesClause) {
        return "INSERT INTO " + flinkTableName + " VALUES " + valuesClause;
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

    private long queryRowCount(String sql) throws Exception {
        return queryRows(sql, resultSet -> Long.toString(resultSet.getLong(1))).stream()
                .findFirst()
                .map(Long::parseLong)
                .orElseThrow(() -> new IllegalStateException("Expected a count row"));
    }

    private long queryRowCountOnce(String sql) throws SQLException {
        try (Connection connection = openConnection();
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(sql)) {
            if (!resultSet.next()) {
                throw new SQLException("Expected a count row");
            }
            return resultSet.getLong(1);
        }
    }

    private boolean waitUntilRowsVisibleBeforeJobFinishes(JobClient jobClient, String tableName) throws Exception {
        long deadlineNanos = System.nanoTime() + Duration.ofSeconds(20).toNanos();
        while (System.nanoTime() < deadlineNanos) {
            JobStatus jobStatus = jobClient.getJobStatus().get();
            if (jobStatus.isTerminalState()) {
                return false;
            }
            try {
                if (queryRowCountOnce("SELECT count(*) FROM " + tableName) > 0) {
                    return true;
                }
            } catch (SQLException ignored) {
                // The sink table may not exist until the first write triggers auto creation.
            }
            Thread.sleep(250L);
        }
        return false;
    }

    private String showCreateTable(String tableName) throws Exception {
        return queryRows("SHOW CREATE TABLE " + tableName, resultSet -> resultSet.getString(2)).stream()
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("Expected SHOW CREATE TABLE row"));
    }

    private boolean tableExists(String tableName) throws Exception {
        return queryRows("SHOW TABLES", resultSet -> resultSet.getString(1)).contains(tableName);
    }

    private void createMetricsTable(String tableName) throws Exception {
        try (Connection connection = openConnection();
                Statement statement = connection.createStatement()) {
            statement.execute("CREATE TABLE " + tableName + " ("
                    + "host STRING,"
                    + "`region` STRING,"
                    + "cpu DOUBLE,"
                    + "ts TIMESTAMP(3) TIME INDEX,"
                    + "PRIMARY KEY(host, `region`)"
                    + ")");
        }
    }

    private void createMetricsTableWithStringCpu(String tableName) throws SQLException {
        try (Connection connection = openConnection();
                Statement statement = connection.createStatement()) {
            statement.execute("CREATE TABLE " + tableName + " ("
                    + "host STRING,"
                    + "`region` STRING,"
                    + "cpu STRING,"
                    + "ts TIMESTAMP(3) TIME INDEX,"
                    + "PRIMARY KEY(host, `region`)"
                    + ")");
        }
    }

    private void createMetricsTableWithHostPrimaryKey(String tableName) throws SQLException {
        try (Connection connection = openConnection();
                Statement statement = connection.createStatement()) {
            statement.execute("CREATE TABLE " + tableName + " ("
                    + "host STRING,"
                    + "`region` STRING,"
                    + "cpu DOUBLE,"
                    + "ts TIMESTAMP(3) TIME INDEX,"
                    + "PRIMARY KEY(host)"
                    + ")");
        }
    }

    private void dropTableIfExists(String tableName) throws SQLException {
        try (Connection connection = openConnection();
                Statement statement = connection.createStatement()) {
            statement.executeUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    private String nullableValue(ResultSet resultSet, String columnName) throws SQLException {
        Object value = resultSet.getObject(columnName);
        return value == null ? "null" : value.toString();
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

    private Connection openConnection() throws SQLException {
        return DriverManager.getConnection(jdbcUrl());
    }

    private String jdbcUrl() {
        return String.format(
                "jdbc:mysql://%s:%d/public?useSSL=false&allowPublicKeyRetrieval=true",
                GREPTIMEDB.getHost(), GREPTIMEDB.getMappedPort(4002));
    }

    private ProcessResult runShadedJarProbe(String tableName) throws Exception {
        Path outputFile = Files.createTempFile("greptime-shaded-jar-probe-", ".log");
        ProcessBuilder processBuilder = new ProcessBuilder(
                javaExecutable(),
                "-cp",
                GreptimeShadedProbeClasspath.shadedRuntimeClasspath(),
                GreptimeShadedJarProbe.class.getName(),
                GREPTIMEDB.getHost(),
                Integer.toString(GREPTIMEDB.getMappedPort(4001)),
                tableName);
        processBuilder.redirectErrorStream(true);
        processBuilder.redirectOutput(outputFile.toFile());

        Process process = processBuilder.start();
        try {
            boolean finished = process.waitFor(60, TimeUnit.SECONDS);
            String output = Files.readString(outputFile, StandardCharsets.UTF_8);
            if (!finished) {
                process.destroyForcibly();
                return new ProcessResult(-1, "Timed out waiting for shaded jar probe\n" + output);
            }
            return new ProcessResult(process.exitValue(), output);
        } finally {
            Files.deleteIfExists(outputFile);
        }
    }

    private String javaExecutable() {
        return Paths.get(System.getProperty("java.home"), "bin", "java").toString();
    }

    @FunctionalInterface
    private interface ResultSetFormatter {
        String format(ResultSet resultSet) throws SQLException;
    }

    private static final class ProcessResult {
        private final int exitCode;
        private final String output;

        private ProcessResult(int exitCode, String output) {
            this.exitCode = exitCode;
            this.output = output;
        }
    }
}
