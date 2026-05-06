package io.greptime.flink.query;

import io.greptime.flink.cfg.GreptimeConfigValidator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public final class GreptimeQuerySqlBuilder {
    private final GreptimeQueryDialect dialect;

    public GreptimeQuerySqlBuilder(GreptimeQueryDialect dialect) {
        this.dialect = Objects.requireNonNull(dialect, "dialect");
    }

    public String buildSelect(String database, String table, List<String> columns, Long limit) {
        String validatedDatabase = GreptimeConfigValidator.validateRequiredText("database", database);
        String validatedTable = GreptimeConfigValidator.validateRequiredText("table", table);
        Objects.requireNonNull(columns, "columns");
        if (limit != null) {
            GreptimeConfigValidator.validateNonNegative("limit", limit);
        }

        String selectList = columns.isEmpty()
                ? "1"
                : columns.stream()
                        .map(column -> GreptimeConfigValidator.validateRequiredText("column", column))
                        .map(dialect::quoteIdentifier)
                        .collect(Collectors.joining(", "));

        String sql = "SELECT "
                + selectList
                + " FROM "
                + dialect.quoteIdentifier(validatedDatabase)
                + "."
                + dialect.quoteIdentifier(validatedTable);
        if (limit != null) {
            sql += " LIMIT " + limit;
        }
        return sql;
    }
}
