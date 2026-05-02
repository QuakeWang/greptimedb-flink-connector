package io.greptime.flink.sink.schema;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import io.greptime.models.TableSchema;
import io.greptime.v1.Common;
import java.util.List;
import org.apache.flink.table.api.DataTypes;
import org.junit.jupiter.api.Test;

class GreptimeTableSchemaTest {
    @Test
    void preservesDecimalPrecisionAndScale() {
        GreptimeTableSchema tableSchema = GreptimeTableSchema.from(
                "metrics",
                DataTypes.ROW(
                        DataTypes.FIELD("host", DataTypes.STRING()),
                        DataTypes.FIELD("amount", DataTypes.DECIMAL(20, 6)),
                        DataTypes.FIELD("ts", DataTypes.TIMESTAMP(3).notNull())),
                "ts",
                List.of("host"));

        TableSchema greptimeSchema = tableSchema.toGreptimeTableSchema();
        Common.ColumnDataTypeExtension decimalExtension =
                greptimeSchema.getDataTypeExtensions().get(1);

        assertEquals(
                Common.ColumnDataType.DECIMAL128, greptimeSchema.getDataTypes().get(1));
        assertNotNull(decimalExtension);
        assertEquals(20, decimalExtension.getDecimalType().getPrecision());
        assertEquals(6, decimalExtension.getDecimalType().getScale());
    }

    @Test
    void keepsTagColumnsInPhysicalColumnOrder() {
        GreptimeTableSchema tableSchema = GreptimeTableSchema.from(
                "metrics",
                DataTypes.ROW(
                        DataTypes.FIELD("region", DataTypes.STRING()),
                        DataTypes.FIELD("host", DataTypes.STRING()),
                        DataTypes.FIELD("cpu", DataTypes.DOUBLE()),
                        DataTypes.FIELD("ts", DataTypes.TIMESTAMP(3).notNull())),
                "ts",
                List.of("region", "host"));

        TableSchema greptimeSchema = tableSchema.toGreptimeTableSchema();

        assertEquals(List.of("region", "host", "cpu", "ts"), greptimeSchema.getColumnNames());
        assertEquals(Common.SemanticType.TAG, greptimeSchema.getSemanticTypes().get(0));
        assertEquals(Common.SemanticType.TAG, greptimeSchema.getSemanticTypes().get(1));
    }

    @Test
    void buildsDeleteSchemaFromRowKeyColumnsInPhysicalOrder() {
        GreptimeTableSchema tableSchema = GreptimeTableSchema.from(
                "metrics",
                DataTypes.ROW(
                        DataTypes.FIELD("region", DataTypes.STRING()),
                        DataTypes.FIELD("cpu", DataTypes.DOUBLE()),
                        DataTypes.FIELD("host", DataTypes.STRING()),
                        DataTypes.FIELD("ts", DataTypes.TIMESTAMP(3).notNull())),
                "ts",
                List.of("region", "host"));

        TableSchema greptimeSchema = tableSchema.toGreptimeDeleteTableSchema();

        assertEquals(List.of("region", "host", "ts"), greptimeSchema.getColumnNames());
        assertEquals(Common.SemanticType.TAG, greptimeSchema.getSemanticTypes().get(0));
        assertEquals(Common.SemanticType.TAG, greptimeSchema.getSemanticTypes().get(1));
        assertEquals(
                Common.SemanticType.TIMESTAMP, greptimeSchema.getSemanticTypes().get(2));
    }

    @Test
    void rejectsTagColumnsThatDoNotFollowPhysicalColumnOrder() {
        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> GreptimeTableSchema.from(
                        "metrics",
                        DataTypes.ROW(
                                DataTypes.FIELD("region", DataTypes.STRING()),
                                DataTypes.FIELD("host", DataTypes.STRING()),
                                DataTypes.FIELD("cpu", DataTypes.DOUBLE()),
                                DataTypes.FIELD("ts", DataTypes.TIMESTAMP(3).notNull())),
                        "ts",
                        List.of("host", "region")));

        assertEquals(
                "`tags` must follow the physical column order of tag columns, tags=[host, region], physicalTagOrder=[region, host]",
                error.getMessage());
    }

    @Test
    void rejectsNonTimestampTimeIndex() {
        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> GreptimeTableSchema.from(
                        "metrics",
                        DataTypes.ROW(
                                DataTypes.FIELD("host", DataTypes.STRING()),
                                DataTypes.FIELD("cpu", DataTypes.DOUBLE()),
                                DataTypes.FIELD("ts", DataTypes.STRING())),
                        "ts",
                        List.of("host")));

        assertEquals("time-index column must be TIMESTAMP or TIMESTAMP_LTZ, but was: STRING", error.getMessage());
    }

    @Test
    void acceptsTimestampLtzTimeIndex() {
        GreptimeTableSchema tableSchema = GreptimeTableSchema.from(
                "metrics",
                DataTypes.ROW(
                        DataTypes.FIELD("host", DataTypes.STRING()),
                        DataTypes.FIELD("cpu", DataTypes.DOUBLE()),
                        DataTypes.FIELD("ts", DataTypes.TIMESTAMP_LTZ(3).notNull())),
                "ts",
                List.of("host"));

        assertEquals(
                Common.SemanticType.TIMESTAMP,
                tableSchema.toGreptimeTableSchema().getSemanticTypes().get(2));
    }

    @Test
    void mapsSupportedTimestampPrecisionsExactly() {
        assertEquals(
                Common.ColumnDataType.TIMESTAMP_SECOND,
                schemaWithTimestampPrecision(0)
                        .toGreptimeTableSchema()
                        .getDataTypes()
                        .get(2));
        assertEquals(
                Common.ColumnDataType.TIMESTAMP_MILLISECOND,
                schemaWithTimestampPrecision(3)
                        .toGreptimeTableSchema()
                        .getDataTypes()
                        .get(2));
        assertEquals(
                Common.ColumnDataType.TIMESTAMP_MICROSECOND,
                schemaWithTimestampPrecision(6)
                        .toGreptimeTableSchema()
                        .getDataTypes()
                        .get(2));
        assertEquals(
                Common.ColumnDataType.TIMESTAMP_NANOSECOND,
                schemaWithTimestampPrecision(9)
                        .toGreptimeTableSchema()
                        .getDataTypes()
                        .get(2));
    }

    @Test
    void rejectsUnsupportedTimestampPrecision() {
        IllegalArgumentException error =
                assertThrows(IllegalArgumentException.class, () -> schemaWithTimestampPrecision(1));

        assertEquals("Unsupported timestamp precision for column `ts`: TIMESTAMP(1) NOT NULL", error.getMessage());
    }

    @Test
    void rejectsNullableTimeIndex() {
        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> GreptimeTableSchema.from(
                        "metrics",
                        DataTypes.ROW(
                                DataTypes.FIELD("host", DataTypes.STRING()),
                                DataTypes.FIELD("cpu", DataTypes.DOUBLE()),
                                DataTypes.FIELD("ts", DataTypes.TIMESTAMP(3))),
                        "ts",
                        List.of("host")));

        assertEquals("time-index column must be declared NOT NULL: ts", error.getMessage());
    }

    @Test
    void rejectsBlankTableName() {
        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> GreptimeTableSchema.from(
                        " ",
                        DataTypes.ROW(
                                DataTypes.FIELD("host", DataTypes.STRING()),
                                DataTypes.FIELD("cpu", DataTypes.DOUBLE()),
                                DataTypes.FIELD("ts", DataTypes.TIMESTAMP(3).notNull())),
                        "ts",
                        List.of("host")));

        assertEquals("tableName must not be blank", error.getMessage());
    }

    @Test
    void rejectsTableNameWithLeadingOrTrailingWhitespace() {
        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> GreptimeTableSchema.from(
                        " metrics ",
                        DataTypes.ROW(
                                DataTypes.FIELD("host", DataTypes.STRING()),
                                DataTypes.FIELD("cpu", DataTypes.DOUBLE()),
                                DataTypes.FIELD("ts", DataTypes.TIMESTAMP(3).notNull())),
                        "ts",
                        List.of("host")));

        assertEquals("tableName must not have leading or trailing whitespace", error.getMessage());
    }

    private static GreptimeTableSchema schemaWithTimestampPrecision(int precision) {
        return GreptimeTableSchema.from(
                "metrics",
                DataTypes.ROW(
                        DataTypes.FIELD("host", DataTypes.STRING()),
                        DataTypes.FIELD("cpu", DataTypes.DOUBLE()),
                        DataTypes.FIELD("ts", DataTypes.TIMESTAMP(precision).notNull())),
                "ts",
                List.of("host"));
    }
}
