package io.greptime.flink.metadata;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

class GreptimeTableMetadataTest {
    @Test
    void buildsTableMetadataIndexes() {
        GreptimeTableMetadata metadata = new GreptimeTableMetadata(
                "public",
                "metrics",
                List.of(
                        column("host", 1, "string", "String", "TAG", "PRI"),
                        column("cpu", 2, "double", "Float64", "FIELD", ""),
                        column("ts", 3, "timestamp", "TimestampMillisecond", "TIMESTAMP", "TIME INDEX")));

        assertEquals("public", metadata.getDatabase());
        assertEquals("metrics", metadata.getTable());
        assertEquals("ts", metadata.getTimeIndexColumn().orElseThrow());
        assertEquals(Set.of("host"), metadata.getPrimaryKeyColumnSet());
        assertTrue(metadata.column("cpu").isPresent());
    }

    @Test
    void rejectsInvalidColumnSets() {
        assertEquals(
                "columns must not be empty",
                assertThrows(
                                IllegalArgumentException.class,
                                () -> new GreptimeTableMetadata("public", "metrics", List.of()))
                        .getMessage());

        assertTrue(assertThrows(
                        IllegalArgumentException.class,
                        () -> new GreptimeTableMetadata(
                                "public",
                                "metrics",
                                List.of(
                                        column("host", 1, "string", "String", "TAG", "PRI"),
                                        column("cpu", 3, "double", "Float64", "FIELD", ""))))
                .getMessage()
                .contains("ordinal_position must be strictly increasing"));

        assertEquals(
                "duplicate column name: host",
                assertThrows(
                                IllegalArgumentException.class,
                                () -> new GreptimeTableMetadata(
                                        "public",
                                        "metrics",
                                        List.of(
                                                column("host", 1, "string", "String", "TAG", "PRI"),
                                                column("host", 2, "string", "String", "FIELD", ""))))
                        .getMessage());

        assertTrue(assertThrows(
                        IllegalArgumentException.class,
                        () -> new GreptimeTableMetadata(
                                "public",
                                "metrics",
                                List.of(
                                        column("host", 1, "string", "String", "TAG", "PRI"),
                                        column("Host", 2, "string", "String", "FIELD", ""))))
                .getMessage()
                .contains("case-ambiguous column names"));

        assertEquals(
                "multiple time index columns are not supported",
                assertThrows(
                                IllegalArgumentException.class,
                                () -> new GreptimeTableMetadata(
                                        "public",
                                        "metrics",
                                        List.of(
                                                column(
                                                        "ts",
                                                        1,
                                                        "timestamp",
                                                        "TimestampMillisecond",
                                                        "TIMESTAMP",
                                                        "TIME INDEX"),
                                                column(
                                                        "ts2",
                                                        2,
                                                        "timestamp",
                                                        "TimestampMillisecond",
                                                        "TIMESTAMP",
                                                        "TIME INDEX"))))
                        .getMessage());
    }

    private static GreptimeColumnMetadata column(
            String name,
            int ordinalPosition,
            String dataType,
            String greptimeDataType,
            String semanticType,
            String key) {
        return new GreptimeColumnMetadata(
                name, ordinalPosition, dataType, greptimeDataType, semanticType, key, null, true, null, null, null);
    }
}
