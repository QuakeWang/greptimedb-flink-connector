package io.greptime.flink.query;

import java.io.IOException;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public final class GreptimeQueryClient implements Serializable {
    private static final long serialVersionUID = 1L;

    private final GreptimeQueryConfig config;

    public GreptimeQueryClient(GreptimeQueryConfig config) {
        this.config = Objects.requireNonNull(config, "config");
    }

    public QueryResult executeQuery(String sql, List<String> projectedColumns, Long limit) throws IOException {
        Connection connection = null;
        Statement statement = null;
        ResultSet resultSet = null;
        try {
            config.getDialect().ensureJdbcDriverAvailable();
            connection = DriverManager.getConnection(config.getJdbcUrl(), config.createJdbcProperties());
            statement = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            if (config.getFetchSize() > 0) {
                statement.setFetchSize(config.getFetchSize());
            }
            resultSet = statement.executeQuery(sql);
            return new QueryResult(config, projectedColumns, limit, connection, statement, resultSet);
        } catch (SQLException e) {
            closeQuietly(resultSet);
            closeQuietly(statement);
            closeQuietly(connection);
            throw queryException("execute-query", projectedColumns, limit, e);
        }
    }

    public IOException queryException(String operation, List<String> projectedColumns, Long limit, SQLException cause) {
        String message = "GreptimeDB query failed: " + describeQueryContext(config, projectedColumns, limit)
                + ", operation=" + operation;
        boolean missingDriver = isMissingDriver(cause);
        if (missingDriver) {
            message += ", hint=MySQL-compatible JDBC driver must be available on the Flink classpath";
        }
        return new IOException(message, missingDriver ? missingDriverCause() : sanitizedCause(cause));
    }

    private static boolean isMissingDriver(SQLException error) {
        String message = error.getMessage();
        return message != null && message.toLowerCase().contains("no suitable driver");
    }

    private SQLException missingDriverCause() {
        return new SQLException("No suitable MySQL-compatible JDBC driver found for "
                + config.getDialect().redactJdbcUrl(config.getJdbcUrl()));
    }

    private static SQLException sanitizedCause(SQLException cause) {
        SQLException sanitizedCause = new SQLException(
                "JDBC driver error while accessing GreptimeDB; original driver message is hidden because it may contain credentials",
                cause.getSQLState(),
                cause.getErrorCode());
        sanitizedCause.setStackTrace(cause.getStackTrace());
        return sanitizedCause;
    }

    private static SQLException sanitizedCloseCause(Exception cause) {
        if (cause instanceof SQLException) {
            return sanitizedCause((SQLException) cause);
        }
        SQLException sanitizedCause = new SQLException("JDBC driver error while accessing GreptimeDB; original "
                + "driver message is hidden ("
                + cause.getClass().getName()
                + ") because it may contain credentials");
        sanitizedCause.setStackTrace(cause.getStackTrace());
        return sanitizedCause;
    }

    private static void closeQuietly(AutoCloseable resource) {
        if (resource == null) {
            return;
        }
        try {
            resource.close();
        } catch (Exception ignored) {
            // Preserve the original query error.
        }
    }

    private static String describeQueryContext(GreptimeQueryConfig config, List<String> projectedColumns, Long limit) {
        String columns = projectedColumns.stream().collect(Collectors.joining(",", "[", "]"));
        return config.describeConnectionContext()
                + ", columns="
                + columns
                + ", limit="
                + (limit == null ? "none" : limit);
    }

    public static final class QueryResult implements AutoCloseable {
        private final GreptimeQueryConfig config;
        private final List<String> projectedColumns;
        private final Long limit;
        private final Connection connection;
        private final Statement statement;
        private final ResultSet resultSet;
        private boolean closed;

        private QueryResult(
                GreptimeQueryConfig config,
                List<String> projectedColumns,
                Long limit,
                Connection connection,
                Statement statement,
                ResultSet resultSet) {
            this.config = config;
            this.projectedColumns = List.copyOf(projectedColumns);
            this.limit = limit;
            this.connection = connection;
            this.statement = statement;
            this.resultSet = resultSet;
        }

        public boolean next() throws SQLException {
            return resultSet.next();
        }

        public ResultSet getResultSet() {
            return resultSet;
        }

        @Override
        public void close() throws IOException {
            if (closed) {
                return;
            }
            closed = true;

            SQLException firstError = null;
            firstError = close(resultSet, firstError);
            firstError = close(statement, firstError);
            firstError = close(connection, firstError);
            if (firstError != null) {
                throw new IOException(
                        "Failed to close GreptimeDB query resources: "
                                + describeQueryContext(config, projectedColumns, limit),
                        firstError);
            }
        }

        private static SQLException close(AutoCloseable resource, SQLException firstError) {
            if (resource == null) {
                return firstError;
            }
            try {
                resource.close();
                return firstError;
            } catch (SQLException e) {
                SQLException sanitized = sanitizedCause(e);
                if (firstError == null) {
                    return sanitized;
                }
                firstError.addSuppressed(sanitized);
                return firstError;
            } catch (Exception e) {
                SQLException wrapped = sanitizedCloseCause(e);
                if (firstError == null) {
                    return wrapped;
                }
                firstError.addSuppressed(wrapped);
                return firstError;
            }
        }
    }
}
