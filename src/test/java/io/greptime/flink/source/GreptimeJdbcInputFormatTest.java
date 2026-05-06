package io.greptime.flink.source;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import io.greptime.flink.query.GreptimeQueryConfig;
import java.io.IOException;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;

class GreptimeJdbcInputFormatTest {
    private static final String FAILING_NEXT_URL = "jdbc:mysql:greptime-input-format-open-next-failure";
    private static final String CLOSE_FAILURE_URL = "jdbc:mysql:greptime-input-format-close-failure";
    private static final String NON_SQL_CLOSE_FAILURE_URL = "jdbc:mysql:greptime-input-format-non-sql-close-failure";

    @Test
    void closesJdbcResourcesWhenInitialNextFailsDuringOpen() throws Exception {
        ResourceTracker tracker = new ResourceTracker();
        Driver driver = new FailingNextDriver(tracker);
        DriverManager.registerDriver(driver);
        try {
            GreptimeJdbcInputFormat inputFormat = new GreptimeJdbcInputFormat(
                    queryConfig(),
                    (RowType) DataTypes.ROW(DataTypes.FIELD("host", DataTypes.STRING()))
                            .getLogicalType(),
                    List.of("host"),
                    null);

            IOException error = assertThrows(IOException.class, () -> inputFormat.open(new GenericInputSplit(0, 1)));

            assertTrue(error.getMessage().contains("operation=read-next"));
            assertSame(SQLException.class, error.getCause().getClass());
            SQLException cause = (SQLException) error.getCause();
            assertTrue(cause.getMessage().contains("original driver message is hidden"));
            assertFalse(cause.getMessage().contains("source-secret"));
            assertEquals("HY000", cause.getSQLState());
            assertEquals(1234, cause.getErrorCode());
            assertEquals(1, error.getSuppressed().length);
            assertTrue(error.getSuppressed()[0].getMessage().contains("Failed to close GreptimeDB query resources"));
            assertTrue(error.getSuppressed()[0].getCause().getMessage().contains("original driver message is hidden"));
            assertFalse(error.getSuppressed()[0].getCause().getMessage().contains("source-secret"));
            assertTrue(tracker.resultSetClosed.get());
            assertTrue(tracker.statementClosed.get());
            assertTrue(tracker.connectionClosed.get());
            assertTrue(inputFormat.reachedEnd());
        } finally {
            DriverManager.deregisterDriver(driver);
        }
    }

    @Test
    void sanitizesCloseFailuresWhenClosingInputFormat() throws Exception {
        ResourceTracker tracker = new ResourceTracker();
        Driver driver = new CloseFailureDriver(tracker, true);
        DriverManager.registerDriver(driver);
        try {
            GreptimeJdbcInputFormat inputFormat = new GreptimeJdbcInputFormat(
                    queryConfig(CLOSE_FAILURE_URL),
                    (RowType) DataTypes.ROW(DataTypes.FIELD("host", DataTypes.STRING()))
                            .getLogicalType(),
                    List.of("host"),
                    null);

            inputFormat.open(new GenericInputSplit(0, 1));

            IOException error = assertThrows(IOException.class, inputFormat::close);

            assertTrue(error.getMessage().contains("Failed to close GreptimeDB query resources"));
            assertFalse(error.getMessage().contains("close-secret"));
            assertSame(SQLException.class, error.getCause().getClass());
            assertTrue(error.getCause().getMessage().contains("original driver message is hidden"));
            assertFalse(error.getCause().getMessage().contains("close-secret"));
            assertEquals(2, error.getCause().getSuppressed().length);
            for (Throwable suppressed : error.getCause().getSuppressed()) {
                assertTrue(suppressed.getMessage().contains("original driver message is hidden"));
                assertFalse(suppressed.getMessage().contains("close-secret"));
            }
            assertTrue(tracker.resultSetClosed.get());
            assertTrue(tracker.statementClosed.get());
            assertTrue(tracker.connectionClosed.get());
            assertTrue(inputFormat.reachedEnd());
        } finally {
            DriverManager.deregisterDriver(driver);
        }
    }

    @Test
    void reportsNonSqlCloseFailureTypeWithoutExposingMessage() throws Exception {
        ResourceTracker tracker = new ResourceTracker();
        Driver driver = new CloseFailureDriver(tracker, false);
        DriverManager.registerDriver(driver);
        try {
            GreptimeJdbcInputFormat inputFormat = new GreptimeJdbcInputFormat(
                    queryConfig(NON_SQL_CLOSE_FAILURE_URL),
                    (RowType) DataTypes.ROW(DataTypes.FIELD("host", DataTypes.STRING()))
                            .getLogicalType(),
                    List.of("host"),
                    null);

            inputFormat.open(new GenericInputSplit(0, 1));

            IOException error = assertThrows(IOException.class, inputFormat::close);

            assertTrue(error.getMessage().contains("Failed to close GreptimeDB query resources"));
            assertFalse(error.getMessage().contains("non-sql-close-secret"));
            assertSame(SQLException.class, error.getCause().getClass());
            assertTrue(error.getCause().getMessage().contains(IllegalStateException.class.getName()));
            assertTrue(error.getCause().getMessage().contains("original driver message is hidden"));
            assertFalse(error.getCause().getMessage().contains("non-sql-close-secret"));
            assertTrue(tracker.resultSetClosed.get());
            assertTrue(tracker.statementClosed.get());
            assertTrue(tracker.connectionClosed.get());
        } finally {
            DriverManager.deregisterDriver(driver);
        }
    }

    private static GreptimeQueryConfig queryConfig() {
        return queryConfig(FAILING_NEXT_URL);
    }

    private static GreptimeQueryConfig queryConfig(String jdbcUrl) {
        return GreptimeQueryConfig.builder()
                .jdbcUrl(jdbcUrl)
                .database("public")
                .table("metrics")
                .build();
    }

    private static Connection connection(ResourceTracker tracker) {
        return (Connection) Proxy.newProxyInstance(
                GreptimeJdbcInputFormatTest.class.getClassLoader(),
                new Class<?>[] {Connection.class},
                (proxy, method, args) -> {
                    switch (method.getName()) {
                        case "createStatement":
                            return statement(tracker);
                        case "close":
                            tracker.connectionClosed.set(true);
                            return null;
                        case "isClosed":
                            return tracker.connectionClosed.get();
                        case "toString":
                            return "FailingNextConnection";
                        default:
                            return defaultValue(method.getReturnType());
                    }
                });
    }

    private static Statement statement(ResourceTracker tracker) {
        return (Statement) Proxy.newProxyInstance(
                GreptimeJdbcInputFormatTest.class.getClassLoader(),
                new Class<?>[] {Statement.class},
                (proxy, method, args) -> {
                    switch (method.getName()) {
                        case "executeQuery":
                            return resultSet(tracker);
                        case "setFetchSize":
                            return null;
                        case "close":
                            tracker.statementClosed.set(true);
                            return null;
                        case "toString":
                            return "FailingNextStatement";
                        default:
                            return defaultValue(method.getReturnType());
                    }
                });
    }

    private static ResultSet resultSet(ResourceTracker tracker) {
        return (ResultSet) Proxy.newProxyInstance(
                GreptimeJdbcInputFormatTest.class.getClassLoader(),
                new Class<?>[] {ResultSet.class},
                (proxy, method, args) -> {
                    switch (method.getName()) {
                        case "next":
                            throw new SQLException("next failed with source-secret", "HY000", 1234);
                        case "close":
                            tracker.resultSetClosed.set(true);
                            throw new SQLException("result close failed with source-secret");
                        case "toString":
                            return "FailingNextResultSet";
                        default:
                            return defaultValue(method.getReturnType());
                    }
                });
    }

    private static Connection closeFailingConnection(ResourceTracker tracker, boolean sqlFailure) {
        return (Connection) Proxy.newProxyInstance(
                GreptimeJdbcInputFormatTest.class.getClassLoader(),
                new Class<?>[] {Connection.class},
                (proxy, method, args) -> {
                    switch (method.getName()) {
                        case "createStatement":
                            return closeFailingStatement(tracker, sqlFailure);
                        case "close":
                            tracker.connectionClosed.set(true);
                            throwCloseFailure("connection", sqlFailure);
                            return null;
                        case "isClosed":
                            return tracker.connectionClosed.get();
                        case "toString":
                            return "CloseFailingConnection";
                        default:
                            return defaultValue(method.getReturnType());
                    }
                });
    }

    private static Statement closeFailingStatement(ResourceTracker tracker, boolean sqlFailure) {
        return (Statement) Proxy.newProxyInstance(
                GreptimeJdbcInputFormatTest.class.getClassLoader(),
                new Class<?>[] {Statement.class},
                (proxy, method, args) -> {
                    switch (method.getName()) {
                        case "executeQuery":
                            return closeFailingResultSet(tracker, sqlFailure);
                        case "setFetchSize":
                            return null;
                        case "close":
                            tracker.statementClosed.set(true);
                            throwCloseFailure("statement", sqlFailure);
                            return null;
                        case "toString":
                            return "CloseFailingStatement";
                        default:
                            return defaultValue(method.getReturnType());
                    }
                });
    }

    private static ResultSet closeFailingResultSet(ResourceTracker tracker, boolean sqlFailure) {
        return (ResultSet) Proxy.newProxyInstance(
                GreptimeJdbcInputFormatTest.class.getClassLoader(),
                new Class<?>[] {ResultSet.class},
                (proxy, method, args) -> {
                    switch (method.getName()) {
                        case "next":
                            return false;
                        case "close":
                            tracker.resultSetClosed.set(true);
                            throwCloseFailure("result", sqlFailure);
                            return null;
                        case "toString":
                            return "CloseFailingResultSet";
                        default:
                            return defaultValue(method.getReturnType());
                    }
                });
    }

    private static void throwCloseFailure(String resource, boolean sqlFailure) throws SQLException {
        if (sqlFailure) {
            throw new SQLException(resource + " close failed with close-secret");
        }
        throw new IllegalStateException(resource + " close failed with non-sql-close-secret");
    }

    private static Object defaultValue(Class<?> returnType) {
        if (!returnType.isPrimitive() || returnType == void.class) {
            return null;
        }
        if (returnType == boolean.class) {
            return false;
        }
        if (returnType == byte.class) {
            return (byte) 0;
        }
        if (returnType == short.class) {
            return (short) 0;
        }
        if (returnType == int.class) {
            return 0;
        }
        if (returnType == long.class) {
            return 0L;
        }
        if (returnType == float.class) {
            return 0f;
        }
        if (returnType == double.class) {
            return 0d;
        }
        if (returnType == char.class) {
            return '\0';
        }
        return null;
    }

    private static final class ResourceTracker {
        private final AtomicBoolean resultSetClosed = new AtomicBoolean();
        private final AtomicBoolean statementClosed = new AtomicBoolean();
        private final AtomicBoolean connectionClosed = new AtomicBoolean();
    }

    private static final class FailingNextDriver implements Driver {
        private final ResourceTracker tracker;

        private FailingNextDriver(ResourceTracker tracker) {
            this.tracker = tracker;
        }

        @Override
        public Connection connect(String url, Properties info) throws SQLException {
            return acceptsURL(url) ? connection(tracker) : null;
        }

        @Override
        public boolean acceptsURL(String url) {
            return FAILING_NEXT_URL.equals(url);
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
        public Logger getParentLogger() throws SQLFeatureNotSupportedException {
            throw new SQLFeatureNotSupportedException();
        }
    }

    private static final class CloseFailureDriver implements Driver {
        private final ResourceTracker tracker;
        private final boolean sqlFailure;

        private CloseFailureDriver(ResourceTracker tracker, boolean sqlFailure) {
            this.tracker = tracker;
            this.sqlFailure = sqlFailure;
        }

        @Override
        public Connection connect(String url, Properties info) throws SQLException {
            return acceptsURL(url) ? closeFailingConnection(tracker, sqlFailure) : null;
        }

        @Override
        public boolean acceptsURL(String url) {
            return CLOSE_FAILURE_URL.equals(url) || NON_SQL_CLOSE_FAILURE_URL.equals(url);
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
        public Logger getParentLogger() throws SQLFeatureNotSupportedException {
            throw new SQLFeatureNotSupportedException();
        }
    }
}
