package io.greptime.flink.sink.schema;

import io.greptime.models.DataType;
import org.apache.flink.table.data.TimestampData;

final class GreptimeTimestampPrecision {
    private GreptimeTimestampPrecision() {}

    static DataType toGreptimeDataType(int precision) {
        switch (precision) {
            case 0:
                return DataType.TimestampSecond;
            case 3:
                return DataType.TimestampMillisecond;
            case 6:
                return DataType.TimestampMicrosecond;
            case 9:
                return DataType.TimestampNanosecond;
            default:
                throw unsupportedPrecision(precision);
        }
    }

    static Object convertValue(int precision, TimestampData value) {
        long millis = value.getMillisecond();
        int nanosOfMilli = value.getNanoOfMillisecond();

        switch (precision) {
            case 0:
                return Math.floorDiv(millis, 1_000L);
            case 3:
                return millis;
            case 6:
                return convertExact(precision, millis, 1_000L, nanosOfMilli / 1_000, nanosOfMilli);
            case 9:
                return convertExact(precision, millis, 1_000_000L, nanosOfMilli, nanosOfMilli);
            default:
                throw unsupportedPrecision(precision);
        }
    }

    private static Long convertExact(int precision, long millis, long scale, long remainder, int nanosOfMilli) {
        try {
            return Math.addExact(Math.multiplyExact(millis, scale), remainder);
        } catch (ArithmeticException e) {
            throw new IllegalArgumentException(
                    "Timestamp value overflows GreptimeDB i64 range for precision "
                            + precision
                            + ": millis="
                            + millis
                            + ", nanosOfMilli="
                            + nanosOfMilli,
                    e);
        }
    }

    private static IllegalArgumentException unsupportedPrecision(int precision) {
        return new IllegalArgumentException(
                "Unsupported timestamp precision: " + precision + ", expected one of [0, 3, 6, 9]");
    }
}
