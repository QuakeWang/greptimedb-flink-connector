package io.greptime.flink.preflight;

import io.greptime.models.TableSchema;
import io.greptime.v1.Common;
import java.util.Locale;

final class GreptimeTypeCompatibility {
    private GreptimeTypeCompatibility() {}

    static boolean supports(Common.ColumnDataType type) {
        return type == Common.ColumnDataType.DECIMAL128 || expectedGreptimeDataType(type) != null;
    }

    static boolean matches(Common.ColumnDataType localType, String remoteGreptimeDataType) {
        if (localType == Common.ColumnDataType.DECIMAL128) {
            return isDecimal(remoteGreptimeDataType);
        }
        String expected = expectedGreptimeDataType(localType);
        return expected != null && normalized(expected).equals(normalized(remoteGreptimeDataType));
    }

    static String expectedType(TableSchema expectedSchema, int position) {
        Common.ColumnDataType localType = expectedSchema.getDataTypes().get(position);
        if (localType == Common.ColumnDataType.DECIMAL128) {
            return expectedDecimalType(expectedSchema, position);
        }
        String expected = expectedGreptimeDataType(localType);
        return expected == null ? localType.name() : expected;
    }

    private static String expectedGreptimeDataType(Common.ColumnDataType type) {
        switch (type) {
            case BOOLEAN:
                return "Boolean";
            case INT8:
                return "Int8";
            case INT16:
                return "Int16";
            case INT32:
                return "Int32";
            case INT64:
                return "Int64";
            case FLOAT32:
                return "Float32";
            case FLOAT64:
                return "Float64";
            case BINARY:
                return "Binary";
            case STRING:
                return "String";
            case DATE:
                return "Date";
            case TIMESTAMP_SECOND:
                return "TimestampSecond";
            case TIMESTAMP_MILLISECOND:
                return "TimestampMillisecond";
            case TIMESTAMP_MICROSECOND:
                return "TimestampMicrosecond";
            case TIMESTAMP_NANOSECOND:
                return "TimestampNanosecond";
            default:
                return null;
        }
    }

    private static String expectedDecimalType(TableSchema expectedSchema, int position) {
        Common.ColumnDataTypeExtension extension =
                expectedSchema.getDataTypeExtensions().get(position);
        if (extension == null || !extension.hasDecimalType()) {
            return "Decimal";
        }
        Common.DecimalTypeExtension decimalType = extension.getDecimalType();
        return "DECIMAL(" + decimalType.getPrecision() + ", " + decimalType.getScale() + ")";
    }

    private static boolean isDecimal(String value) {
        String normalized = normalized(value);
        return normalized.equals("decimal") || normalized.equals("decimal128") || normalized.startsWith("decimal(");
    }

    private static String normalized(String value) {
        return value.trim().toLowerCase(Locale.ROOT);
    }
}
