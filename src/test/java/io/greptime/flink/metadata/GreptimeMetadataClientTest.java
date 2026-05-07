package io.greptime.flink.metadata;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import io.greptime.flink.query.GreptimeQueryConfig;
import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.logging.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class GreptimeMetadataClientTest {
    private static final String JDBC_URL = "jdbc:mysql:metadata-client-test";

    private FakeDriver driver;

    @AfterEach
    void deregisterDriver() throws SQLException {
        if (driver != null) {
            DriverManager.deregisterDriver(driver);
        }
    }

    @Test
    void returnsEmptyWhenTableHasNoMetadataRows() throws Exception {
        FakeDriver driver = registerDriver(FakeDriver.withRows(List.of()));

        Optional<GreptimeTableMetadata> metadata = new GreptimeMetadataClient(config()).loadTable("public", "metrics");

        assertTrue(metadata.isEmpty());
        assertEquals(List.of("public", "metrics"), driver.statementParameters);
        assertTrue(driver.connectionClosed);
        assertTrue(driver.statementClosed);
        assertTrue(driver.resultSetClosed);
    }

    @Test
    void parsesColumnsWithNullAwarePrecisionFields() throws Exception {
        FakeDriver driver = registerDriver(FakeDriver.withRows(List.of(
                row("host", 1, "string", "String", "TAG", "PRI", "YES", null, null, null),
                row("cpu", 2, "double", "Float64", "FIELD", "", "Yes", null, null, null),
                row("ts", 3, "timestamp", "TimestampMillisecond", "TIMESTAMP", "TIME INDEX", "NO", 3, null, null))));

        GreptimeTableMetadata metadata = new GreptimeMetadataClient(config())
                .loadTable("public", "metrics")
                .orElseThrow();

        assertEquals("ts", metadata.getTimeIndexColumn().orElseThrow());
        assertEquals("host", metadata.getPrimaryKeyColumnSet().iterator().next());
        GreptimeColumnMetadata cpu = metadata.column("cpu").orElseThrow();
        assertTrue(cpu.isNullable());
        assertEquals(null, cpu.getDatetimePrecision());
        GreptimeColumnMetadata ts = metadata.column("ts").orElseThrow();
        assertFalse(ts.isNullable());
        assertEquals(3, ts.getDatetimePrecision());
        assertTrue(driver.connectionClosed);
        assertTrue(driver.statementClosed);
        assertTrue(driver.resultSetClosed);
    }

    @Test
    void malformedMetadataFailsClosed() throws Exception {
        registerDriver(FakeDriver.withRows(
                List.of(row("host", 1, "string", "String", "TAG", "PRI", "MAYBE", null, null, null))));

        IOException error = org.junit.jupiter.api.Assertions.assertThrows(
                IOException.class, () -> new GreptimeMetadataClient(config()).loadTable("public", "metrics"));

        assertTrue(error.getMessage().contains("reason=unsupported-remote-metadata"));
        assertTrue(error.getMessage().contains("is_nullable must be YES or NO"));
    }

    @Test
    void metadataReadFailureHidesDriverMessage() throws Exception {
        registerDriver(FakeDriver.withConnectionFailure(
                new SQLException("Access denied for password=driver-secret", "28000", 1045)));

        IOException error = org.junit.jupiter.api.Assertions.assertThrows(
                IOException.class, () -> new GreptimeMetadataClient(config()).loadTable("public", "metrics"));

        assertTrue(error.getMessage().contains("reason=metadata-read-failure"));
        assertTrue(error.getMessage().contains("phase=preflight"));
        assertFalse(error.getMessage().contains("driver-secret"));
        assertFalse(error.getCause().getMessage().contains("driver-secret"));
        assertEquals("28000", ((SQLException) error.getCause()).getSQLState());
        assertEquals(1045, ((SQLException) error.getCause()).getErrorCode());
    }

    private FakeDriver registerDriver(FakeDriver driver) throws SQLException {
        this.driver = driver;
        DriverManager.registerDriver(driver);
        return driver;
    }

    private static GreptimeQueryConfig config() {
        return GreptimeQueryConfig.builder()
                .owner("GreptimeDB preflight")
                .jdbcUrl(JDBC_URL)
                .database("public")
                .table("metrics")
                .build();
    }

    private static Map<String, Object> row(
            String name,
            int ordinalPosition,
            String dataType,
            String greptimeDataType,
            String semanticType,
            String columnKey,
            String isNullable,
            Integer datetimePrecision,
            Integer numericPrecision,
            Integer numericScale) {
        Map<String, Object> values = new HashMap<>();
        values.put("column_name", name);
        values.put("ordinal_position", ordinalPosition);
        values.put("data_type", dataType);
        values.put("greptime_data_type", greptimeDataType);
        values.put("semantic_type", semanticType);
        values.put("column_key", columnKey);
        values.put("column_default", null);
        values.put("is_nullable", isNullable);
        values.put("datetime_precision", datetimePrecision);
        values.put("numeric_precision", numericPrecision);
        values.put("numeric_scale", numericScale);
        return values;
    }

    private static final class FakeDriver implements Driver {
        private final List<Map<String, Object>> rows;
        private final SQLException connectionFailure;
        private final List<String> statementParameters = new ArrayList<>();
        private boolean connectionClosed;
        private boolean statementClosed;
        private boolean resultSetClosed;

        private FakeDriver(List<Map<String, Object>> rows, SQLException connectionFailure) {
            this.rows = rows;
            this.connectionFailure = connectionFailure;
        }

        private static FakeDriver withRows(List<Map<String, Object>> rows) {
            return new FakeDriver(rows, null);
        }

        private static FakeDriver withConnectionFailure(SQLException connectionFailure) {
            return new FakeDriver(List.of(), connectionFailure);
        }

        @Override
        public Connection connect(String url, Properties info) throws SQLException {
            if (!acceptsURL(url)) {
                return null;
            }
            if (connectionFailure != null) {
                throw connectionFailure;
            }
            return proxy(Connection.class, new ConnectionHandler(this));
        }

        @Override
        public boolean acceptsURL(String url) {
            return JDBC_URL.equals(url);
        }

        @Override
        public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) {
            return new DriverPropertyInfo[0];
        }

        @Override
        public int getMajorVersion() {
            return 1;
        }

        @Override
        public int getMinorVersion() {
            return 0;
        }

        @Override
        public boolean jdbcCompliant() {
            return false;
        }

        @Override
        public Logger getParentLogger() {
            return Logger.getGlobal();
        }
    }

    private static final class ConnectionHandler implements InvocationHandler {
        private final FakeDriver driver;

        private ConnectionHandler(FakeDriver driver) {
            this.driver = driver;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) {
            switch (method.getName()) {
                case "prepareStatement":
                    return proxy(PreparedStatement.class, new PreparedStatementHandler(driver));
                case "close":
                    driver.connectionClosed = true;
                    return null;
                case "isClosed":
                    return driver.connectionClosed;
                default:
                    return defaultValue(method.getReturnType());
            }
        }
    }

    private static final class PreparedStatementHandler implements InvocationHandler {
        private final FakeDriver driver;

        private PreparedStatementHandler(FakeDriver driver) {
            this.driver = driver;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) {
            switch (method.getName()) {
                case "setString":
                    driver.statementParameters.add((String) args[1]);
                    return null;
                case "executeQuery":
                    return proxy(ResultSet.class, new ResultSetHandler(driver));
                case "close":
                    driver.statementClosed = true;
                    return null;
                default:
                    return defaultValue(method.getReturnType());
            }
        }
    }

    private static final class ResultSetHandler implements InvocationHandler {
        private final FakeDriver driver;
        private int index = -1;
        private boolean lastWasNull;

        private ResultSetHandler(FakeDriver driver) {
            this.driver = driver;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) {
            switch (method.getName()) {
                case "next":
                    index++;
                    return index < driver.rows.size();
                case "getString":
                    return stringValue((String) args[0]);
                case "getInt":
                    return intValue((String) args[0]);
                case "wasNull":
                    return lastWasNull;
                case "close":
                    driver.resultSetClosed = true;
                    return null;
                default:
                    return defaultValue(method.getReturnType());
            }
        }

        private String stringValue(String columnName) {
            Object value = driver.rows.get(index).get(columnName);
            lastWasNull = value == null;
            return value == null ? null : value.toString();
        }

        private int intValue(String columnName) {
            Object value = driver.rows.get(index).get(columnName);
            lastWasNull = value == null;
            return value == null ? 0 : ((Number) value).intValue();
        }
    }

    @SuppressWarnings("unchecked")
    private static <T> T proxy(Class<T> type, InvocationHandler handler) {
        return (T) Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[] {type}, handler);
    }

    private static Object defaultValue(Class<?> type) {
        if (type == Boolean.TYPE) {
            return false;
        }
        if (type == Integer.TYPE) {
            return 0;
        }
        if (type == Long.TYPE) {
            return 0L;
        }
        return null;
    }
}
