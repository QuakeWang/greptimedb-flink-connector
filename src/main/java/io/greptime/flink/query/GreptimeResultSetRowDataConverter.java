package io.greptime.flink.query;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Objects;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

public final class GreptimeResultSetRowDataConverter implements java.io.Serializable {
    private static final long serialVersionUID = 1L;

    private final RowType rowType;

    public GreptimeResultSetRowDataConverter(RowType rowType) {
        this.rowType = Objects.requireNonNull(rowType, "rowType");
        validateSupportedTypes(rowType);
    }

    public RowData convert(ResultSet resultSet) throws SQLException {
        GenericRowData row = new GenericRowData(rowType.getFieldCount());
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            row.setField(i, readField(resultSet, i + 1, rowType.getFields().get(i)));
        }
        return row;
    }

    private static Object readField(ResultSet resultSet, int position, RowType.RowField field) throws SQLException {
        LogicalType logicalType = field.getType();
        switch (logicalType.getTypeRoot()) {
            case BOOLEAN:
                boolean booleanValue = resultSet.getBoolean(position);
                return resultSet.wasNull() ? null : booleanValue;
            case TINYINT:
                byte byteValue = resultSet.getByte(position);
                return resultSet.wasNull() ? null : byteValue;
            case SMALLINT:
                short shortValue = resultSet.getShort(position);
                return resultSet.wasNull() ? null : shortValue;
            case INTEGER:
                int intValue = resultSet.getInt(position);
                return resultSet.wasNull() ? null : intValue;
            case BIGINT:
                long longValue = resultSet.getLong(position);
                return resultSet.wasNull() ? null : longValue;
            case FLOAT:
                float floatValue = resultSet.getFloat(position);
                return resultSet.wasNull() ? null : floatValue;
            case DOUBLE:
                double doubleValue = resultSet.getDouble(position);
                return resultSet.wasNull() ? null : doubleValue;
            case CHAR:
            case VARCHAR:
                String stringValue = resultSet.getString(position);
                return stringValue == null ? null : StringData.fromString(stringValue);
            case BINARY:
            case VARBINARY:
                return resultSet.getBytes(position);
            case DATE:
                Date dateValue = resultSet.getDate(position);
                return dateValue == null ? null : (int) dateValue.toLocalDate().toEpochDay();
            case DECIMAL:
                BigDecimal decimalValue = resultSet.getBigDecimal(position);
                DecimalType decimalType = (DecimalType) logicalType;
                if (decimalValue == null) {
                    return null;
                }
                DecimalData decimalData =
                        DecimalData.fromBigDecimal(decimalValue, decimalType.getPrecision(), decimalType.getScale());
                if (decimalData == null) {
                    throw new SQLException("DECIMAL value exceeds declared precision/scale: column="
                            + field.getName()
                            + ", declaredPrecision="
                            + decimalType.getPrecision()
                            + ", declaredScale="
                            + decimalType.getScale()
                            + ", valuePrecision="
                            + decimalValue.precision()
                            + ", valueScale="
                            + decimalValue.scale());
                }
                return decimalData;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                Timestamp timestampValue = resultSet.getTimestamp(position);
                return timestampValue == null ? null : TimestampData.fromTimestamp(timestampValue);
            default:
                throw unsupportedType(logicalType);
        }
    }

    private static void validateSupportedTypes(RowType rowType) {
        for (RowType.RowField field : rowType.getFields()) {
            switch (field.getType().getTypeRoot()) {
                case BOOLEAN:
                case TINYINT:
                case SMALLINT:
                case INTEGER:
                case BIGINT:
                case FLOAT:
                case DOUBLE:
                case CHAR:
                case VARCHAR:
                case BINARY:
                case VARBINARY:
                case DATE:
                case DECIMAL:
                case TIMESTAMP_WITHOUT_TIME_ZONE:
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported GreptimeDB source field type: field="
                            + field.getName()
                            + ", type="
                            + field.getType());
            }
        }
    }

    private static IllegalArgumentException unsupportedType(LogicalType logicalType) {
        return new IllegalArgumentException("Unsupported GreptimeDB source field type: type=" + logicalType);
    }
}
