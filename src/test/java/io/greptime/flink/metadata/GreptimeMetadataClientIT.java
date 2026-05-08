package io.greptime.flink.metadata;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import io.greptime.flink.query.GreptimeQueryConfig;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
class GreptimeMetadataClientIT {
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
    void loadsTableMetadataFromGreptimeInformationSchema() throws Exception {
        String tableName = "metadata_client_it";
        dropTableIfExists(tableName);
        executeUpdate("CREATE TABLE " + tableName + " ("
                + "host STRING,"
                + "`region` STRING,"
                + "active BOOLEAN,"
                + "cpu DOUBLE,"
                + "amount DECIMAL(12, 2),"
                + "ts TIMESTAMP(6) TIME INDEX,"
                + "PRIMARY KEY(host, `region`)"
                + ")");

        GreptimeTableMetadata metadata = new GreptimeMetadataClient(queryConfig(tableName))
                .loadTable("public", tableName)
                .orElseThrow();

        assertEquals("public", metadata.getDatabase());
        assertEquals(tableName, metadata.getTable());
        assertEquals(6, metadata.getColumns().size());
        assertEquals("ts", metadata.getTimeIndexColumn().orElseThrow());
        assertEquals(2, metadata.getPrimaryKeyColumnSet().size());
        assertTrue(metadata.getPrimaryKeyColumnSet().contains("host"));
        assertTrue(metadata.getPrimaryKeyColumnSet().contains("region"));

        GreptimeColumnMetadata host = metadata.column("host").orElseThrow();
        assertEquals(1, host.getOrdinalPosition());
        assertEquals("TAG", host.getSemanticType());
        assertEquals("PRI", host.getColumnKey());
        assertEquals("String", host.getGreptimeDataType());

        GreptimeColumnMetadata cpu = metadata.column("cpu").orElseThrow();
        assertEquals("FIELD", cpu.getSemanticType());
        assertEquals("Float64", cpu.getGreptimeDataType());

        GreptimeColumnMetadata active = metadata.column("active").orElseThrow();
        assertEquals("FIELD", active.getSemanticType());
        assertEquals("Boolean", active.getGreptimeDataType());

        GreptimeColumnMetadata amount = metadata.column("amount").orElseThrow();
        assertEquals("FIELD", amount.getSemanticType());
        assertEquals("Decimal(12, 2)", amount.getGreptimeDataType());
        assertEquals(12, amount.getNumericPrecision());
        assertEquals(2, amount.getNumericScale());

        GreptimeColumnMetadata ts = metadata.column("ts").orElseThrow();
        assertEquals("TIMESTAMP", ts.getSemanticType());
        assertEquals("TIME INDEX", ts.getColumnKey());
        assertEquals("TimestampMicrosecond", ts.getGreptimeDataType());
        assertEquals(6, ts.getDatetimePrecision());
        assertFalse(ts.isNullable());
    }

    private GreptimeQueryConfig queryConfig(String tableName) {
        return GreptimeQueryConfig.builder()
                .owner("GreptimeDB preflight")
                .jdbcUrl(jdbcUrl())
                .database("public")
                .table(tableName)
                .build();
    }

    private void dropTableIfExists(String tableName) throws SQLException {
        executeUpdate("DROP TABLE IF EXISTS " + tableName);
    }

    private void executeUpdate(String sql) throws SQLException {
        try (Connection connection = DriverManager.getConnection(jdbcUrl());
                Statement statement = connection.createStatement()) {
            statement.executeUpdate(sql);
        }
    }

    private String jdbcUrl() {
        return String.format(
                "jdbc:mysql://%s:%d/public?useSSL=false&allowPublicKeyRetrieval=true",
                GREPTIMEDB.getHost(), GREPTIMEDB.getMappedPort(4002));
    }
}
