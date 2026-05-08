package io.greptime.flink.preflight;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import io.greptime.flink.metadata.GreptimeColumnMetadata;
import io.greptime.flink.metadata.GreptimeTableMetadata;
import io.greptime.flink.sink.schema.GreptimeTableSchema;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.flink.table.api.DataTypes;
import org.junit.jupiter.api.Test;

class GreptimeTableInspectorTest {
    private final GreptimeTableInspector inspector = new GreptimeTableInspector();

    @Test
    void bulkExactMatchSucceeds() {
        assertTrue(inspect(tableSchema(), matchingMetadata()).isEmpty());
    }

    @Test
    void bulkBooleanAndTimestampMicrosecondTypesMatch() {
        GreptimeTableSchema schema = GreptimeTableSchema.from(
                "metrics",
                DataTypes.ROW(
                        DataTypes.FIELD("host", DataTypes.STRING()),
                        DataTypes.FIELD("active", DataTypes.BOOLEAN()),
                        DataTypes.FIELD("ts", DataTypes.TIMESTAMP(6).notNull())),
                "ts",
                List.of("host"));
        GreptimeTableMetadata metadata = metadata(List.of(
                column("host", 1, "String", "TAG", "PRI", true),
                column("active", 2, "Boolean", "FIELD", "", true),
                column("ts", 3, "TimestampMicrosecond", "TIMESTAMP", "TIME INDEX", false)));

        assertTrue(inspect(schema, metadata).isEmpty());
    }

    @Test
    void bulkMissingTableFails() {
        List<PreflightFinding> findings = inspector.inspectBulkSink("public", tableSchema(), Optional.empty());

        assertEquals(List.of("missing-table"), categories(findings));
    }

    @Test
    void bulkColumnOrderMismatchFails() {
        GreptimeTableMetadata metadata = metadata(List.of(
                column("host", 1, "String", "TAG", "PRI", true),
                column("cpu", 2, "Float64", "FIELD", "", true),
                column("region", 3, "String", "TAG", "PRI", true),
                column("ts", 4, "TimestampMillisecond", "TIMESTAMP", "TIME INDEX", false)));

        assertEquals(
                List.of("column-order-mismatch", "column-order-mismatch"),
                categories(inspect(tableSchema(), metadata)));
    }

    @Test
    void bulkTypeMismatchFails() {
        GreptimeTableMetadata metadata = metadata(List.of(
                column("host", 1, "String", "TAG", "PRI", true),
                column("region", 2, "String", "TAG", "PRI", true),
                column("cpu", 3, "String", "FIELD", "", true),
                column("ts", 4, "TimestampMillisecond", "TIMESTAMP", "TIME INDEX", false)));

        assertEquals(List.of("type-mismatch"), categories(inspect(tableSchema(), metadata)));
    }

    @Test
    void bulkTimeIndexAndTagSetMismatchesFail() {
        GreptimeTableMetadata metadata = metadata(List.of(
                column("host", 1, "String", "TAG", "PRI", true),
                column("region", 2, "String", "FIELD", "", true),
                column("cpu", 3, "Float64", "FIELD", "", true),
                column("ts", 4, "TimestampMillisecond", "FIELD", "", true)));

        List<String> categories = categories(inspect(tableSchema(), metadata));

        assertTrue(categories.contains("time-index-mismatch"), categories.toString());
        assertTrue(categories.contains("row-key-set-mismatch"), categories.toString());
    }

    @Test
    void bulkNullableLocalToRemoteNotNullFails() {
        GreptimeTableMetadata metadata = metadata(List.of(
                column("host", 1, "String", "TAG", "PRI", true),
                column("region", 2, "String", "TAG", "PRI", true),
                column("cpu", 3, "Float64", "FIELD", "", false),
                column("ts", 4, "TimestampMillisecond", "TIMESTAMP", "TIME INDEX", false)));

        assertEquals(List.of("nullability-mismatch"), categories(inspect(tableSchema(), metadata)));
    }

    @Test
    void bulkDecimalPrecisionMismatchFails() {
        GreptimeTableMetadata metadata = metadata(List.of(
                column("host", 1, "String", "TAG", "PRI", true),
                column("region", 2, "String", "TAG", "PRI", true),
                decimalColumn("cpu", 3, 10, 2),
                column("ts", 4, "TimestampMillisecond", "TIMESTAMP", "TIME INDEX", false)));

        assertEquals(List.of("type-mismatch"), categories(inspect(decimalSchema(), metadata)));
    }

    @Test
    void bulkDecimalMetadataTypeMatchSucceeds() {
        assertTrue(inspect(decimalSchema(), decimalMetadata()).isEmpty());
    }

    private List<PreflightFinding> inspect(GreptimeTableSchema tableSchema, GreptimeTableMetadata metadata) {
        return inspector.inspectBulkSink("public", tableSchema, Optional.of(metadata));
    }

    private static GreptimeTableSchema tableSchema() {
        return GreptimeTableSchema.from(
                "metrics",
                DataTypes.ROW(
                        DataTypes.FIELD("host", DataTypes.STRING()),
                        DataTypes.FIELD("region", DataTypes.STRING()),
                        DataTypes.FIELD("cpu", DataTypes.DOUBLE()),
                        DataTypes.FIELD("ts", DataTypes.TIMESTAMP(3).notNull())),
                "ts",
                List.of("host", "region"));
    }

    private static GreptimeTableSchema decimalSchema() {
        return GreptimeTableSchema.from(
                "metrics",
                DataTypes.ROW(
                        DataTypes.FIELD("host", DataTypes.STRING()),
                        DataTypes.FIELD("region", DataTypes.STRING()),
                        DataTypes.FIELD("cpu", DataTypes.DECIMAL(12, 2)),
                        DataTypes.FIELD("ts", DataTypes.TIMESTAMP(3).notNull())),
                "ts",
                List.of("host", "region"));
    }

    private static GreptimeTableMetadata matchingMetadata() {
        return metadata(List.of(
                column("host", 1, "String", "TAG", "PRI", true),
                column("region", 2, "String", "TAG", "PRI", true),
                column("cpu", 3, "Float64", "FIELD", "", true),
                column("ts", 4, "TimestampMillisecond", "TIMESTAMP", "TIME INDEX", false)));
    }

    private static GreptimeTableMetadata decimalMetadata() {
        return metadata(List.of(
                column("host", 1, "String", "TAG", "PRI", true),
                column("region", 2, "String", "TAG", "PRI", true),
                decimalColumn("cpu", 3, 12, 2),
                column("ts", 4, "TimestampMillisecond", "TIMESTAMP", "TIME INDEX", false)));
    }

    private static GreptimeTableMetadata metadata(List<GreptimeColumnMetadata> columns) {
        return new GreptimeTableMetadata("public", "metrics", columns);
    }

    private static GreptimeColumnMetadata column(
            String name,
            int ordinalPosition,
            String greptimeDataType,
            String semanticType,
            String columnKey,
            boolean nullable) {
        return new GreptimeColumnMetadata(
                name,
                ordinalPosition,
                greptimeDataType.toLowerCase(),
                greptimeDataType,
                semanticType,
                columnKey,
                null,
                nullable,
                null,
                null,
                null);
    }

    private static GreptimeColumnMetadata decimalColumn(String name, int ordinalPosition, int precision, int scale) {
        return new GreptimeColumnMetadata(
                name,
                ordinalPosition,
                "decimal",
                "Decimal(" + precision + ", " + scale + ")",
                "FIELD",
                "",
                null,
                true,
                null,
                precision,
                scale);
    }

    private static List<String> categories(List<PreflightFinding> findings) {
        return findings.stream().map(PreflightFinding::getCategory).collect(Collectors.toList());
    }
}
