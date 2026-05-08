package io.greptime.flink.sink.schema;

import io.greptime.models.SemanticType;
import io.greptime.models.TableSchema;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;

public final class GreptimeTableSchema implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final int GREPTIME_DECIMAL128_MAX_PRECISION = 38;

    private final String tableName;
    private final DataType physicalRowDataType;
    private final String timeIndex;
    private final List<String> tagColumns;

    private GreptimeTableSchema(
            String tableName, DataType physicalRowDataType, String timeIndex, List<String> tagColumns) {
        this.tableName = validateTableName(tableName);
        this.physicalRowDataType = Objects.requireNonNull(physicalRowDataType, "physicalRowDataType");
        this.timeIndex = Objects.requireNonNull(timeIndex, "timeIndex");
        this.tagColumns = List.copyOf(tagColumns);
    }

    public static GreptimeTableSchema from(
            String tableName, DataType physicalRowDataType, String timeIndex, List<String> tags) {
        RowType rowType = (RowType) physicalRowDataType.getLogicalType();

        for (RowType.RowField field : rowType.getFields()) {
            toGreptimeColumnType(field.getName(), field.getType());
        }

        GreptimeSchemaValidator.validate(rowType, timeIndex, tags);

        return new GreptimeTableSchema(tableName, physicalRowDataType, timeIndex, tags);
    }

    public String getTableName() {
        return tableName;
    }

    public DataType getPhysicalRowDataType() {
        return physicalRowDataType;
    }

    public String getTimeIndex() {
        return timeIndex;
    }

    public List<String> getTagColumns() {
        return tagColumns;
    }

    public TableSchema toGreptimeTableSchema() {
        RowType rowType = (RowType) physicalRowDataType.getLogicalType();
        List<Integer> columnPositions = new ArrayList<>(rowType.getFieldCount());
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            columnPositions.add(i);
        }
        return toGreptimeTableSchema(columnPositions);
    }

    public TableSchema toGreptimeDeleteTableSchema() {
        return toGreptimeTableSchema(rowKeyColumnPositions());
    }

    List<Integer> rowKeyColumnPositions() {
        RowType rowType = (RowType) physicalRowDataType.getLogicalType();
        Set<String> rowKeyColumns = new HashSet<>(tagColumns);
        rowKeyColumns.add(timeIndex);

        List<Integer> positions = new ArrayList<>(rowKeyColumns.size());
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            if (rowKeyColumns.contains(rowType.getFieldNames().get(i))) {
                positions.add(i);
            }
        }
        return List.copyOf(positions);
    }

    private TableSchema toGreptimeTableSchema(List<Integer> columnPositions) {
        RowType rowType = (RowType) physicalRowDataType.getLogicalType();
        TableSchema.Builder builder = TableSchema.newBuilder(tableName);

        for (int position : columnPositions) {
            RowType.RowField field = rowType.getFields().get(position);
            String fieldName = field.getName();
            GreptimeColumnType greptimeColumnType = toGreptimeColumnType(fieldName, field.getType());
            builder.addColumn(
                    fieldName,
                    toSemanticType(fieldName),
                    greptimeColumnType.dataType,
                    greptimeColumnType.decimalTypeExtension);
        }

        return builder.build();
    }

    private SemanticType toSemanticType(String fieldName) {
        if (fieldName.equals(timeIndex)) {
            return SemanticType.Timestamp;
        }
        if (tagColumns.contains(fieldName)) {
            return SemanticType.Tag;
        }
        return SemanticType.Field;
    }

    private static GreptimeColumnType toGreptimeColumnType(String fieldName, LogicalType logicalType) {
        LogicalTypeRoot root = logicalType.getTypeRoot();
        switch (root) {
            case BOOLEAN:
                return GreptimeColumnType.of(io.greptime.models.DataType.Bool);
            case TINYINT:
                return GreptimeColumnType.of(io.greptime.models.DataType.Int8);
            case SMALLINT:
                return GreptimeColumnType.of(io.greptime.models.DataType.Int16);
            case INTEGER:
                return GreptimeColumnType.of(io.greptime.models.DataType.Int32);
            case BIGINT:
                return GreptimeColumnType.of(io.greptime.models.DataType.Int64);
            case FLOAT:
                return GreptimeColumnType.of(io.greptime.models.DataType.Float32);
            case DOUBLE:
                return GreptimeColumnType.of(io.greptime.models.DataType.Float64);
            case CHAR:
            case VARCHAR:
                return GreptimeColumnType.of(io.greptime.models.DataType.String);
            case BINARY:
            case VARBINARY:
                return GreptimeColumnType.of(io.greptime.models.DataType.Binary);
            case DATE:
                return GreptimeColumnType.of(io.greptime.models.DataType.Date);
            case DECIMAL:
                DecimalType decimalType = (DecimalType) logicalType;
                if (decimalType.getPrecision() > GREPTIME_DECIMAL128_MAX_PRECISION) {
                    throw new IllegalArgumentException(
                            "Decimal precision exceeds Greptime Decimal128 limit: " + decimalType.getPrecision());
                }
                return GreptimeColumnType.decimal(decimalType.getPrecision(), decimalType.getScale());
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return GreptimeColumnType.of(toTimestampDataType(fieldName, logicalType));
            default:
                throw new IllegalArgumentException("Unsupported Flink logical type: " + logicalType);
        }
    }

    private static io.greptime.models.DataType toTimestampDataType(String fieldName, LogicalType logicalType) {
        try {
            if (logicalType instanceof LocalZonedTimestampType) {
                return GreptimeTimestampPrecision.toGreptimeDataType(
                        ((LocalZonedTimestampType) logicalType).getPrecision());
            }
            return GreptimeTimestampPrecision.toGreptimeDataType(
                    ((org.apache.flink.table.types.logical.TimestampType) logicalType).getPrecision());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                    "Unsupported timestamp precision for column `" + fieldName + "`: " + logicalType, e);
        }
    }

    private static String validateTableName(String tableName) {
        Objects.requireNonNull(tableName, "tableName");
        String trimmed = tableName.trim();
        if (trimmed.isEmpty()) {
            throw new IllegalArgumentException("tableName must not be blank");
        }
        if (!tableName.equals(trimmed)) {
            throw new IllegalArgumentException("tableName must not have leading or trailing whitespace");
        }
        return tableName;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof GreptimeTableSchema)) {
            return false;
        }
        GreptimeTableSchema that = (GreptimeTableSchema) other;
        return Objects.equals(tableName, that.tableName)
                && Objects.equals(physicalRowDataType, that.physicalRowDataType)
                && Objects.equals(timeIndex, that.timeIndex)
                && Objects.equals(tagColumns, that.tagColumns);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableName, physicalRowDataType, timeIndex, tagColumns);
    }

    private static final class GreptimeColumnType {
        private final io.greptime.models.DataType dataType;
        private final io.greptime.models.DataType.DecimalTypeExtension decimalTypeExtension;

        private GreptimeColumnType(
                io.greptime.models.DataType dataType,
                io.greptime.models.DataType.DecimalTypeExtension decimalTypeExtension) {
            this.dataType = dataType;
            this.decimalTypeExtension = decimalTypeExtension;
        }

        private static GreptimeColumnType of(io.greptime.models.DataType dataType) {
            return new GreptimeColumnType(dataType, null);
        }

        private static GreptimeColumnType decimal(int precision, int scale) {
            return new GreptimeColumnType(
                    io.greptime.models.DataType.Decimal128,
                    new io.greptime.models.DataType.DecimalTypeExtension(precision, scale));
        }
    }
}
