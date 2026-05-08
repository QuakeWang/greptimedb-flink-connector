package io.greptime.flink.metadata;

import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

public final class GreptimeTableMetadata implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String database;
    private final String table;
    private final List<GreptimeColumnMetadata> columns;
    private final Optional<String> timeIndexColumn;
    private final Set<String> primaryKeyColumnSet;
    private final Map<String, GreptimeColumnMetadata> columnsByName;

    public GreptimeTableMetadata(String database, String table, List<GreptimeColumnMetadata> columns) {
        this.database = validateRequiredText("database", database);
        this.table = validateRequiredText("table", table);
        if (Objects.requireNonNull(columns, "columns").isEmpty()) {
            throw new IllegalArgumentException("columns must not be empty");
        }
        this.columns = validateColumns(columns);
        MetadataIndex index = buildIndex(this.columns);
        this.timeIndexColumn = Optional.ofNullable(index.timeIndexColumn);
        this.primaryKeyColumnSet = index.primaryKeyColumnSet;
        this.columnsByName = index.columnsByName;
    }

    public String getDatabase() {
        return database;
    }

    public String getTable() {
        return table;
    }

    public List<GreptimeColumnMetadata> getColumns() {
        return columns;
    }

    public Optional<String> getTimeIndexColumn() {
        return timeIndexColumn;
    }

    public Set<String> getPrimaryKeyColumnSet() {
        return primaryKeyColumnSet;
    }

    public Optional<GreptimeColumnMetadata> column(String name) {
        return Optional.ofNullable(columnsByName.get(name));
    }

    private static List<GreptimeColumnMetadata> validateColumns(List<GreptimeColumnMetadata> columns) {
        List<GreptimeColumnMetadata> copy = List.copyOf(columns);
        for (int i = 0; i < copy.size(); i++) {
            int expectedOrdinal = i + 1;
            int actualOrdinal = copy.get(i).getOrdinalPosition();
            if (actualOrdinal != expectedOrdinal) {
                throw new IllegalArgumentException("ordinal_position must be strictly increasing from 1, expected "
                        + expectedOrdinal
                        + " but was "
                        + actualOrdinal);
            }
        }
        return copy;
    }

    private static MetadataIndex buildIndex(List<GreptimeColumnMetadata> columns) {
        Map<String, GreptimeColumnMetadata> byName = new HashMap<>();
        Map<String, String> lowerCaseNames = new HashMap<>();
        Set<String> primaryKeyColumns = new LinkedHashSet<>();
        String timeIndex = null;

        for (GreptimeColumnMetadata column : columns) {
            if (byName.put(column.getName(), column) != null) {
                throw new IllegalArgumentException("duplicate column name: " + column.getName());
            }
            String lowerCaseName = column.getName().toLowerCase(Locale.ROOT);
            String previousName = lowerCaseNames.putIfAbsent(lowerCaseName, column.getName());
            if (previousName != null && !previousName.equals(column.getName())) {
                throw new IllegalArgumentException(
                        "case-ambiguous column names are not supported: " + previousName + ", " + column.getName());
            }
            if (column.isTimeIndex()) {
                if (timeIndex != null) {
                    throw new IllegalArgumentException("multiple time index columns are not supported");
                }
                timeIndex = column.getName();
            }
            if (column.isPrimaryKeyColumn()) {
                primaryKeyColumns.add(column.getName());
            }
        }

        return new MetadataIndex(timeIndex, Set.copyOf(primaryKeyColumns), Map.copyOf(byName));
    }

    private static String validateRequiredText(String name, String value) {
        Objects.requireNonNull(value, name);
        String trimmed = value.trim();
        if (trimmed.isEmpty()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return trimmed;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof GreptimeTableMetadata)) {
            return false;
        }
        GreptimeTableMetadata that = (GreptimeTableMetadata) other;
        return Objects.equals(database, that.database)
                && Objects.equals(table, that.table)
                && Objects.equals(columns, that.columns);
    }

    @Override
    public int hashCode() {
        return Objects.hash(database, table, columns);
    }

    private static final class MetadataIndex {
        private final String timeIndexColumn;
        private final Set<String> primaryKeyColumnSet;
        private final Map<String, GreptimeColumnMetadata> columnsByName;

        private MetadataIndex(
                String timeIndexColumn,
                Set<String> primaryKeyColumnSet,
                Map<String, GreptimeColumnMetadata> columnsByName) {
            this.timeIndexColumn = timeIndexColumn;
            this.primaryKeyColumnSet = primaryKeyColumnSet;
            this.columnsByName = columnsByName;
        }
    }
}
