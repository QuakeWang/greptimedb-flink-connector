package io.greptime.flink.preflight;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import io.greptime.v1.Common;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class GreptimeTypeCompatibilityTest {
    @ParameterizedTest
    @MethodSource("supportedTypes")
    void matchesSupportedGreptimeMetadataTypes(Common.ColumnDataType localType, String remoteType) {
        assertTrue(GreptimeTypeCompatibility.supports(localType));
        assertTrue(GreptimeTypeCompatibility.matches(localType, remoteType));
        assertFalse(GreptimeTypeCompatibility.matches(localType, mismatchType(remoteType)));
    }

    @Test
    void matchesDecimalWithPrecisionAndScaleMetadata() {
        assertTrue(GreptimeTypeCompatibility.supports(Common.ColumnDataType.DECIMAL128));
        assertTrue(GreptimeTypeCompatibility.matches(Common.ColumnDataType.DECIMAL128, "Decimal(12, 2)"));
        assertFalse(GreptimeTypeCompatibility.matches(Common.ColumnDataType.DECIMAL128, "Float64"));
    }

    @ParameterizedTest
    @MethodSource("unsupportedTypes")
    void unsupportedTypesFailClosed(Common.ColumnDataType localType) {
        assertFalse(GreptimeTypeCompatibility.supports(localType));
        assertFalse(GreptimeTypeCompatibility.matches(localType, "String"));
    }

    private static Stream<Arguments> supportedTypes() {
        return Stream.of(
                Arguments.of(Common.ColumnDataType.BOOLEAN, "Boolean"),
                Arguments.of(Common.ColumnDataType.INT8, "Int8"),
                Arguments.of(Common.ColumnDataType.INT16, "Int16"),
                Arguments.of(Common.ColumnDataType.INT32, "Int32"),
                Arguments.of(Common.ColumnDataType.INT64, "Int64"),
                Arguments.of(Common.ColumnDataType.FLOAT32, "Float32"),
                Arguments.of(Common.ColumnDataType.FLOAT64, "Float64"),
                Arguments.of(Common.ColumnDataType.BINARY, "Binary"),
                Arguments.of(Common.ColumnDataType.STRING, "String"),
                Arguments.of(Common.ColumnDataType.DATE, "Date"),
                Arguments.of(Common.ColumnDataType.TIMESTAMP_SECOND, "TimestampSecond"),
                Arguments.of(Common.ColumnDataType.TIMESTAMP_MILLISECOND, "TimestampMillisecond"),
                Arguments.of(Common.ColumnDataType.TIMESTAMP_MICROSECOND, "TimestampMicrosecond"),
                Arguments.of(Common.ColumnDataType.TIMESTAMP_NANOSECOND, "TimestampNanosecond"));
    }

    private static Stream<Common.ColumnDataType> unsupportedTypes() {
        return Stream.of(
                Common.ColumnDataType.UINT8,
                Common.ColumnDataType.UINT16,
                Common.ColumnDataType.UINT32,
                Common.ColumnDataType.UINT64,
                Common.ColumnDataType.DATETIME,
                Common.ColumnDataType.TIME_SECOND,
                Common.ColumnDataType.TIME_MILLISECOND,
                Common.ColumnDataType.TIME_MICROSECOND,
                Common.ColumnDataType.TIME_NANOSECOND,
                Common.ColumnDataType.INTERVAL_YEAR_MONTH,
                Common.ColumnDataType.INTERVAL_DAY_TIME,
                Common.ColumnDataType.INTERVAL_MONTH_DAY_NANO,
                Common.ColumnDataType.JSON,
                Common.ColumnDataType.UNRECOGNIZED);
    }

    private static String mismatchType(String remoteType) {
        return "Boolean".equals(remoteType) ? "String" : "Boolean";
    }
}
