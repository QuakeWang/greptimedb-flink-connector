package io.greptime.flink.sink.schema;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import io.greptime.models.Table;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.List;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.junit.jupiter.api.Test;

class GreptimeRowDataConverterTest {
    @Test
    void convertsTinyIntAndSmallIntToIntegerForSdkRowWriter() {
        GreptimeTableSchema tableSchema = GreptimeTableSchema.from(
                "metrics",
                DataTypes.ROW(
                        DataTypes.FIELD("tiny_value", DataTypes.TINYINT()),
                        DataTypes.FIELD("small_value", DataTypes.SMALLINT()),
                        DataTypes.FIELD("ts", DataTypes.TIMESTAMP(3).notNull())),
                "ts",
                List.of());
        GreptimeRowDataConverter converter = GreptimeRowDataConverter.forSchema(tableSchema);

        Object[] values = converter.convert(
                GenericRowData.of((byte) 7, (short) 1024, TimestampData.fromEpochMillis(1700000000000L)));

        assertEquals(Integer.valueOf(7), values[0]);
        assertEquals(Integer.valueOf(1024), values[1]);

        Table table = Table.from(tableSchema.toGreptimeTableSchema());
        assertDoesNotThrow(() -> table.addRow(values));
        assertEquals(1, table.rowCount());
    }

    @Test
    void convertsDateDecimalBinaryTimestampLtzAndPreservesNullableFields() {
        GreptimeTableSchema tableSchema = GreptimeTableSchema.from(
                "metrics",
                DataTypes.ROW(
                        DataTypes.FIELD("host", DataTypes.STRING()),
                        DataTypes.FIELD("event_date", DataTypes.DATE()),
                        DataTypes.FIELD("amount", DataTypes.DECIMAL(20, 6)),
                        DataTypes.FIELD("payload", DataTypes.BYTES()),
                        DataTypes.FIELD("optional_note", DataTypes.STRING()),
                        DataTypes.FIELD("ts", DataTypes.TIMESTAMP_LTZ(9).notNull())),
                "ts",
                List.of("host"));
        GreptimeRowDataConverter converter = GreptimeRowDataConverter.forSchema(tableSchema);
        byte[] payload = new byte[] {1, 2, 3};

        Object[] values = converter.convert(GenericRowData.of(
                StringData.fromString("host-1"),
                19_479,
                DecimalData.fromBigDecimal(new BigDecimal("12.345678"), 20, 6),
                payload,
                null,
                TimestampData.fromEpochMillis(1_700_000_000_123L, 456_789)));

        assertEquals("host-1", values[0]);
        assertEquals(LocalDate.ofEpochDay(19_479), values[1]);
        assertEquals(new BigDecimal("12.345678"), values[2]);
        assertEquals(true, Arrays.equals(payload, (byte[]) values[3]));
        assertNull(values[4]);
        assertEquals(Long.valueOf(1_700_000_000_123_456_789L), values[5]);

        Table table = Table.from(tableSchema.toGreptimeTableSchema());
        assertDoesNotThrow(() -> table.addRow(values));
        assertEquals(1, table.rowCount());
    }

    @Test
    void convertsTimestampPrecisionsWithoutSilentNormalization() {
        assertEquals(
                Long.valueOf(1_700_000_000L), convertTimestamp(0, TimestampData.fromEpochMillis(1_700_000_000_123L)));
        assertEquals(
                Long.valueOf(1_700_000_000_123L),
                convertTimestamp(3, TimestampData.fromEpochMillis(1_700_000_000_123L)));
        assertEquals(
                Long.valueOf(1_700_000_000_123_456L),
                convertTimestamp(6, TimestampData.fromEpochMillis(1_700_000_000_123L, 456_000)));
        assertEquals(
                Long.valueOf(1_700_000_000_123_456_789L),
                convertTimestamp(9, TimestampData.fromEpochMillis(1_700_000_000_123L, 456_789)));
    }

    @Test
    void rejectsTimestampNanosecondOverflow() {
        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> convertTimestamp(9, TimestampData.fromEpochMillis(32_503_680_000_000L)));

        assertEquals(
                "Timestamp value overflows GreptimeDB i64 range for precision 9: millis=32503680000000, nanosOfMilli=0",
                error.getMessage());
    }

    @Test
    void rejectsNullTimeIndexLocally() {
        GreptimeTableSchema tableSchema = GreptimeTableSchema.from(
                "metrics",
                DataTypes.ROW(
                        DataTypes.FIELD("host", DataTypes.STRING()),
                        DataTypes.FIELD("cpu", DataTypes.DOUBLE()),
                        DataTypes.FIELD("ts", DataTypes.TIMESTAMP(3).notNull())),
                "ts",
                List.of());
        GreptimeRowDataConverter converter = GreptimeRowDataConverter.forSchema(tableSchema);

        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> converter.convert(GenericRowData.of(StringData.fromString("host-1"), 0.5d, null)));

        assertEquals("time-index column must not be null: table=metrics, column=ts", error.getMessage());
    }

    @Test
    void convertsDeleteKeyColumnsInTheSameProjectionAsDeleteSchema() {
        GreptimeTableSchema tableSchema = GreptimeTableSchema.from(
                "metrics",
                DataTypes.ROW(
                        DataTypes.FIELD("region", DataTypes.STRING()),
                        DataTypes.FIELD("cpu", DataTypes.DOUBLE()),
                        DataTypes.FIELD("host", DataTypes.STRING()),
                        DataTypes.FIELD("ts", DataTypes.TIMESTAMP(3).notNull())),
                "ts",
                List.of("region", "host"));
        GreptimeRowDataConverter converter = GreptimeRowDataConverter.forKeyColumns(tableSchema);

        Object[] values = converter.convert(GenericRowData.of(
                StringData.fromString("eu-west"),
                0.5d,
                StringData.fromString("host-1"),
                TimestampData.fromEpochMillis(1_700_000_000_123L)));

        assertEquals(
                List.of("region", "host", "ts"),
                tableSchema.toGreptimeDeleteTableSchema().getColumnNames());
        assertEquals("eu-west", values[0]);
        assertEquals("host-1", values[1]);
        assertEquals(Long.valueOf(1_700_000_000_123L), values[2]);

        Table table = Table.from(tableSchema.toGreptimeDeleteTableSchema());
        assertDoesNotThrow(() -> table.addRow(values));
        assertEquals(1, table.rowCount());
    }

    @Test
    void rejectsNullDeleteKeyColumnLocally() {
        GreptimeTableSchema tableSchema = GreptimeTableSchema.from(
                "metrics",
                DataTypes.ROW(
                        DataTypes.FIELD("host", DataTypes.STRING()),
                        DataTypes.FIELD("cpu", DataTypes.DOUBLE()),
                        DataTypes.FIELD("ts", DataTypes.TIMESTAMP(3).notNull())),
                "ts",
                List.of("host"));
        GreptimeRowDataConverter converter = GreptimeRowDataConverter.forKeyColumns(tableSchema);

        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> converter.convert(GenericRowData.of(null, 0.5d, TimestampData.fromEpochMillis(1L))));

        assertEquals("row key column must not be null: table=metrics, column=host", error.getMessage());
    }

    private static Long convertTimestamp(int precision, TimestampData timestamp) {
        GreptimeTableSchema tableSchema = GreptimeTableSchema.from(
                "metrics",
                DataTypes.ROW(
                        DataTypes.FIELD("host", DataTypes.STRING()),
                        DataTypes.FIELD("ts", DataTypes.TIMESTAMP(precision).notNull())),
                "ts",
                List.of());
        GreptimeRowDataConverter converter = GreptimeRowDataConverter.forSchema(tableSchema);

        Object[] values = converter.convert(GenericRowData.of(StringData.fromString("host-1"), timestamp));
        return (Long) values[1];
    }
}
