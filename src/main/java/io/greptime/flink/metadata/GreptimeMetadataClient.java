package io.greptime.flink.metadata;

import io.greptime.flink.query.GreptimeQueryConfig;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;

public final class GreptimeMetadataClient {
    private static final String COLUMNS_SQL = "SELECT "
            + "column_name, "
            + "ordinal_position, "
            + "data_type, "
            + "greptime_data_type, "
            + "semantic_type, "
            + "column_key, "
            + "column_default, "
            + "is_nullable, "
            + "datetime_precision, "
            + "numeric_precision, "
            + "numeric_scale "
            + "FROM information_schema.columns "
            + "WHERE table_schema = ? "
            + "AND table_name = ? "
            + "ORDER BY ordinal_position";

    private final GreptimeQueryConfig config;

    public GreptimeMetadataClient(GreptimeQueryConfig config) {
        this.config = Objects.requireNonNull(config, "config");
    }

    public Optional<GreptimeTableMetadata> loadTable(String database, String table) throws IOException {
        Objects.requireNonNull(database, "database");
        Objects.requireNonNull(table, "table");
        try {
            config.getDialect().ensureJdbcDriverAvailable();
            try (Connection connection =
                            DriverManager.getConnection(config.getJdbcUrl(), config.createJdbcProperties());
                    PreparedStatement statement = connection.prepareStatement(COLUMNS_SQL)) {
                statement.setString(1, database);
                statement.setString(2, table);
                try (ResultSet resultSet = statement.executeQuery()) {
                    List<GreptimeColumnMetadata> columns = readColumns(resultSet);
                    if (columns.isEmpty()) {
                        return Optional.empty();
                    }
                    return Optional.of(new GreptimeTableMetadata(database, table, columns));
                }
            }
        } catch (SQLException e) {
            throw metadataReadException("load-table-metadata", e);
        } catch (IllegalArgumentException e) {
            throw unsupportedRemoteMetadataException(e);
        }
    }

    private static List<GreptimeColumnMetadata> readColumns(ResultSet resultSet) throws SQLException {
        List<GreptimeColumnMetadata> columns = new ArrayList<>();
        while (resultSet.next()) {
            columns.add(new GreptimeColumnMetadata(
                    requiredString(resultSet, "column_name"),
                    requiredInt(resultSet, "ordinal_position"),
                    requiredString(resultSet, "data_type"),
                    requiredString(resultSet, "greptime_data_type"),
                    requiredString(resultSet, "semantic_type"),
                    nullableString(resultSet, "column_key"),
                    nullableString(resultSet, "column_default"),
                    parseNullable(requiredString(resultSet, "is_nullable")),
                    nullableInt(resultSet, "datetime_precision"),
                    nullableInt(resultSet, "numeric_precision"),
                    nullableInt(resultSet, "numeric_scale")));
        }
        return columns;
    }

    private IOException metadataReadException(String operation, SQLException cause) {
        String message = "GreptimeDB metadata read failed: phase=preflight, operation="
                + operation
                + ", reason=metadata-read-failure, "
                + config.describeMetadataConnectionContext();
        boolean missingDriver = isMissingDriver(cause);
        if (missingDriver) {
            message += ", hint=MySQL-compatible JDBC driver must be available on the Flink classpath";
        }
        return new IOException(message, missingDriver ? missingDriverCause() : sanitizedCause(cause));
    }

    private IOException unsupportedRemoteMetadataException(IllegalArgumentException cause) {
        return new IOException(
                "GreptimeDB metadata read failed: phase=preflight, operation=parse-table-metadata, reason=unsupported-remote-metadata, "
                        + config.describeMetadataConnectionContext()
                        + ", detail="
                        + cause.getMessage(),
                cause);
    }

    private static boolean isMissingDriver(SQLException error) {
        String message = error.getMessage();
        return message != null && message.toLowerCase(Locale.ROOT).contains("no suitable driver");
    }

    private SQLException missingDriverCause() {
        return new SQLException("No suitable MySQL-compatible JDBC driver found for "
                + config.getDialect().redactJdbcUrl(config.getJdbcUrl()));
    }

    private static SQLException sanitizedCause(SQLException cause) {
        SQLException sanitizedCause = new SQLException(
                "JDBC driver error while reading GreptimeDB metadata; original driver message is hidden because it may contain credentials",
                cause.getSQLState(),
                cause.getErrorCode());
        sanitizedCause.setStackTrace(cause.getStackTrace());
        return sanitizedCause;
    }

    private static String requiredString(ResultSet resultSet, String columnName) throws SQLException {
        String value = resultSet.getString(columnName);
        if (value == null) {
            throw new IllegalArgumentException(columnName + " must not be null");
        }
        return value;
    }

    private static String nullableString(ResultSet resultSet, String columnName) throws SQLException {
        return resultSet.getString(columnName);
    }

    private static Integer nullableInt(ResultSet resultSet, String columnName) throws SQLException {
        int value = resultSet.getInt(columnName);
        return resultSet.wasNull() ? null : value;
    }

    private static int requiredInt(ResultSet resultSet, String columnName) throws SQLException {
        int value = resultSet.getInt(columnName);
        if (resultSet.wasNull()) {
            throw new IllegalArgumentException(columnName + " must not be null");
        }
        return value;
    }

    private static boolean parseNullable(String value) {
        String normalized = value.trim().toUpperCase(Locale.ROOT);
        if ("YES".equals(normalized)) {
            return true;
        }
        if ("NO".equals(normalized)) {
            return false;
        }
        throw new IllegalArgumentException("is_nullable must be YES or NO, but was: " + value);
    }
}
