package io.greptime.flink.sink.schema;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;

public final class GreptimeRowDataConverter {
    private final String tableName;
    private final String timeIndexColumn;
    private final List<FieldConverter> fields;
    private final boolean validateRequiredColumnsAsRowKey;

    private GreptimeRowDataConverter(
            String tableName,
            String timeIndexColumn,
            List<FieldConverter> fields,
            boolean validateRequiredColumnsAsRowKey) {
        this.tableName = tableName;
        this.timeIndexColumn = timeIndexColumn;
        this.fields = fields;
        this.validateRequiredColumnsAsRowKey = validateRequiredColumnsAsRowKey;
    }

    public static GreptimeRowDataConverter forSchema(GreptimeTableSchema tableSchema) {
        RowType rowType = (RowType) tableSchema.getPhysicalRowDataType().getLogicalType();
        List<Integer> columnPositions = new ArrayList<>(rowType.getFieldCount());

        for (int i = 0; i < rowType.getFieldCount(); i++) {
            columnPositions.add(i);
        }

        return forColumns(tableSchema, columnPositions, false);
    }

    public static GreptimeRowDataConverter forKeyColumns(GreptimeTableSchema tableSchema) {
        return forColumns(tableSchema, tableSchema.rowKeyColumnPositions(), true);
    }

    private static GreptimeRowDataConverter forColumns(
            GreptimeTableSchema tableSchema, List<Integer> columnPositions, boolean validateRequiredColumnsAsRowKey) {
        RowType rowType = (RowType) tableSchema.getPhysicalRowDataType().getLogicalType();
        List<FieldConverter> fields = new ArrayList<>(columnPositions.size());
        int timeIndexPosition = rowType.getFieldNames().indexOf(tableSchema.getTimeIndex());

        for (int columnPosition : columnPositions) {
            LogicalType logicalType = rowType.getTypeAt(columnPosition);
            fields.add(new FieldConverter(
                    rowType.getFieldNames().get(columnPosition),
                    logicalType,
                    RowData.createFieldGetter(logicalType, columnPosition),
                    validateRequiredColumnsAsRowKey || columnPosition == timeIndexPosition));
        }

        return new GreptimeRowDataConverter(
                tableSchema.getTableName(),
                tableSchema.getTimeIndex(),
                List.copyOf(fields),
                validateRequiredColumnsAsRowKey);
    }

    public Object[] convert(RowData row) {
        Object[] values = new Object[fields.size()];
        for (int i = 0; i < fields.size(); i++) {
            FieldConverter field = fields.get(i);
            Object value = field.getter.getFieldOrNull(row);
            if (value == null && field.required) {
                throw nullRequiredColumn(field.columnName);
            }
            values[i] = convertValue(field.logicalType, value);
        }
        return values;
    }

    private IllegalArgumentException nullRequiredColumn(String columnName) {
        if (validateRequiredColumnsAsRowKey) {
            return new IllegalArgumentException(
                    "row key column must not be null: table=" + tableName + ", column=" + columnName);
        }
        return new IllegalArgumentException(
                "time-index column must not be null: table=" + tableName + ", column=" + timeIndexColumn);
    }

    private static Object convertValue(LogicalType logicalType, Object value) {
        if (value == null) {
            return null;
        }

        switch (logicalType.getTypeRoot()) {
            case TINYINT:
            case SMALLINT:
                return ((Number) value).intValue();
            case CHAR:
            case VARCHAR:
                return ((StringData) value).toString();
            case DATE:
                return LocalDate.ofEpochDay((Integer) value);
            case DECIMAL:
                return ((DecimalData) value).toBigDecimal();
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return convertTimestamp((TimestampType) logicalType, (TimestampData) value);
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return convertTimestamp((LocalZonedTimestampType) logicalType, (TimestampData) value);
            default:
                return value;
        }
    }

    private static Object convertTimestamp(TimestampType logicalType, TimestampData value) {
        return convertTimestampValue(logicalType.getPrecision(), value);
    }

    private static Object convertTimestamp(LocalZonedTimestampType logicalType, TimestampData value) {
        return convertTimestampValue(logicalType.getPrecision(), value);
    }

    private static Object convertTimestampValue(int precision, TimestampData value) {
        return GreptimeTimestampPrecision.convertValue(precision, value);
    }

    private static final class FieldConverter {
        private final String columnName;
        private final LogicalType logicalType;
        private final RowData.FieldGetter getter;
        private final boolean required;

        private FieldConverter(
                String columnName, LogicalType logicalType, RowData.FieldGetter getter, boolean required) {
            this.columnName = columnName;
            this.logicalType = logicalType;
            this.getter = getter;
            this.required = required;
        }
    }
}
