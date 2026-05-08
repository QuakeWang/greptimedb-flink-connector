package io.greptime.flink.metadata;

import java.io.Serializable;
import java.util.Locale;
import java.util.Objects;

public final class GreptimeColumnMetadata implements Serializable {
    private static final long serialVersionUID = 1L;

    private static final String SEMANTIC_TIME_INDEX = "TIMESTAMP";
    private static final String SEMANTIC_TAG = "TAG";
    private static final String COLUMN_KEY_TIME_INDEX = "TIME INDEX";
    private static final String COLUMN_KEY_PRIMARY = "PRI";

    private final String name;
    private final int ordinalPosition;
    private final String dataType;
    private final String greptimeDataType;
    private final String semanticType;
    private final String columnKey;
    private final String columnDefault;
    private final boolean nullable;
    private final Integer datetimePrecision;
    private final Integer numericPrecision;
    private final Integer numericScale;

    public GreptimeColumnMetadata(
            String name,
            int ordinalPosition,
            String dataType,
            String greptimeDataType,
            String semanticType,
            String columnKey,
            String columnDefault,
            boolean nullable,
            Integer datetimePrecision,
            Integer numericPrecision,
            Integer numericScale) {
        this.name = validateRequiredText("column_name", name);
        if (ordinalPosition <= 0) {
            throw new IllegalArgumentException("ordinal_position must start at 1");
        }
        this.ordinalPosition = ordinalPosition;
        this.dataType = validateRequiredText("data_type", dataType);
        this.greptimeDataType = validateRequiredText("greptime_data_type", greptimeDataType);
        this.semanticType = validateRequiredText("semantic_type", semanticType);
        this.columnKey = columnKey == null ? "" : columnKey.trim();
        this.columnDefault = columnDefault;
        this.nullable = nullable;
        this.datetimePrecision = datetimePrecision;
        this.numericPrecision = numericPrecision;
        this.numericScale = numericScale;
    }

    public String getName() {
        return name;
    }

    public int getOrdinalPosition() {
        return ordinalPosition;
    }

    public String getDataType() {
        return dataType;
    }

    public String getGreptimeDataType() {
        return greptimeDataType;
    }

    public String getSemanticType() {
        return semanticType;
    }

    public String getColumnKey() {
        return columnKey;
    }

    public String getColumnDefault() {
        return columnDefault;
    }

    public boolean isNullable() {
        return nullable;
    }

    public Integer getDatetimePrecision() {
        return datetimePrecision;
    }

    public Integer getNumericPrecision() {
        return numericPrecision;
    }

    public Integer getNumericScale() {
        return numericScale;
    }

    public boolean isTimeIndex() {
        return normalized(semanticType).equals(SEMANTIC_TIME_INDEX)
                || normalized(columnKey).equals(COLUMN_KEY_TIME_INDEX);
    }

    public boolean isPrimaryKeyColumn() {
        return normalized(semanticType).equals(SEMANTIC_TAG)
                || normalized(columnKey).equals(COLUMN_KEY_PRIMARY);
    }

    private static String validateRequiredText(String name, String value) {
        Objects.requireNonNull(value, name);
        String trimmed = value.trim();
        if (trimmed.isEmpty()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return trimmed;
    }

    private static String normalized(String value) {
        return value.trim().toUpperCase(Locale.ROOT);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof GreptimeColumnMetadata)) {
            return false;
        }
        GreptimeColumnMetadata that = (GreptimeColumnMetadata) other;
        return ordinalPosition == that.ordinalPosition
                && nullable == that.nullable
                && Objects.equals(name, that.name)
                && Objects.equals(dataType, that.dataType)
                && Objects.equals(greptimeDataType, that.greptimeDataType)
                && Objects.equals(semanticType, that.semanticType)
                && Objects.equals(columnKey, that.columnKey)
                && Objects.equals(columnDefault, that.columnDefault)
                && Objects.equals(datetimePrecision, that.datetimePrecision)
                && Objects.equals(numericPrecision, that.numericPrecision)
                && Objects.equals(numericScale, that.numericScale);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                name,
                ordinalPosition,
                dataType,
                greptimeDataType,
                semanticType,
                columnKey,
                columnDefault,
                nullable,
                datetimePrecision,
                numericPrecision,
                numericScale);
    }
}
