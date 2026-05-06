package io.greptime.flink.source;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import io.greptime.flink.query.GreptimeQueryConfig;
import java.util.Optional;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.connector.source.InputFormatProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.junit.jupiter.api.Test;

class GreptimeDynamicTableSourceTest {
    @Test
    void buildsSingleSplitBoundedInputFormatWithProjectionAndLimit() {
        GreptimeDynamicTableSource source = new GreptimeDynamicTableSource(queryConfig(), physicalRowDataType());
        source.applyProjection(
                new int[][] {{2}, {0}},
                DataTypes.ROW(DataTypes.FIELD("cpu", DataTypes.DOUBLE()), DataTypes.FIELD("host", DataTypes.STRING())));
        source.applyLimit(10L);

        InputFormatProvider provider = (InputFormatProvider) source.getScanRuntimeProvider(null);
        GreptimeJdbcInputFormat inputFormat = (GreptimeJdbcInputFormat) provider.createInputFormat();

        assertEquals(Optional.of(1), provider.getParallelism());
        assertTrue(provider.isBounded());
        assertEquals("SELECT `cpu`, `host` FROM `public`.`metrics` LIMIT 10", inputFormat.getSql());
        assertEquals(2, producedRowSize(inputFormat));
        assertEquals(1, inputFormat.createInputSplits(4).length);
        assertEquals(new GenericInputSplit(0, 1), inputFormat.createInputSplits(4)[0]);
    }

    @Test
    void buildsEmptyProjectionSql() {
        GreptimeDynamicTableSource source = new GreptimeDynamicTableSource(queryConfig(), physicalRowDataType());
        source.applyProjection(new int[][] {}, DataTypes.ROW());

        InputFormatProvider provider = (InputFormatProvider) source.getScanRuntimeProvider(null);
        GreptimeJdbcInputFormat inputFormat = (GreptimeJdbcInputFormat) provider.createInputFormat();

        assertEquals("SELECT 1 FROM `public`.`metrics`", inputFormat.getSql());
        assertEquals(0, producedRowSize(inputFormat));
        assertEquals(0, inputFormat.getProjectedColumns().size());
    }

    @Test
    void copyPreservesPlanningStateWithoutSharingProjectedFieldArray() {
        GreptimeDynamicTableSource source = new GreptimeDynamicTableSource(queryConfig(), physicalRowDataType());
        int[][] projection = new int[][] {{1}, {0}};
        source.applyProjection(
                projection,
                DataTypes.ROW(
                        DataTypes.FIELD("region", DataTypes.STRING()), DataTypes.FIELD("host", DataTypes.STRING())));
        source.applyLimit(5L);

        GreptimeDynamicTableSource copy = (GreptimeDynamicTableSource) source.copy();
        projection[0][0] = 2;
        int[][] copiedProjection = copy.getProjectedFields();
        copiedProjection[0][0] = 2;

        assertArrayEquals(new int[][] {{1}, {0}}, source.getProjectedFields());
        assertArrayEquals(new int[][] {{1}, {0}}, copy.getProjectedFields());
        assertEquals(5L, copy.getLimit());
        assertEquals(source.getProducedDataType(), copy.getProducedDataType());
    }

    @Test
    void rejectsNestedProjectionAndInvalidSplit() {
        GreptimeDynamicTableSource source = new GreptimeDynamicTableSource(queryConfig(), physicalRowDataType());

        assertThrows(
                IllegalArgumentException.class,
                () -> source.applyProjection(
                        new int[][] {{1, 0}}, DataTypes.ROW(DataTypes.FIELD("x", DataTypes.STRING()))));

        GreptimeJdbcInputFormat inputFormat = new GreptimeJdbcInputFormat(
                queryConfig(),
                (org.apache.flink.table.types.logical.RowType)
                        physicalRowDataType().getLogicalType(),
                java.util.List.of("host"),
                null);

        assertThrows(java.io.IOException.class, () -> inputFormat.open(new GenericInputSplit(1, 2)));
    }

    @Test
    void rejectsUnsupportedProducedTypesWhenCreatingRuntimeProvider() {
        GreptimeDynamicTableSource source = new GreptimeDynamicTableSource(
                queryConfig(),
                DataTypes.ROW(
                        DataTypes.FIELD("host", DataTypes.STRING()),
                        DataTypes.FIELD("ts", DataTypes.TIMESTAMP_LTZ(3))));

        IllegalArgumentException error =
                assertThrows(IllegalArgumentException.class, () -> source.getScanRuntimeProvider(null));

        assertTrue(error.getMessage().contains("Unsupported GreptimeDB source field type"));
        assertTrue(error.getMessage().contains("ts"));
    }

    private static GreptimeQueryConfig queryConfig() {
        return GreptimeQueryConfig.builder()
                .jdbcUrl("jdbc:mysql://127.0.0.1:4002/public?useSSL=false")
                .database("public")
                .table("metrics")
                .build();
    }

    private static DataType physicalRowDataType() {
        return DataTypes.ROW(
                DataTypes.FIELD("host", DataTypes.STRING()),
                DataTypes.FIELD("region", DataTypes.STRING()),
                DataTypes.FIELD("cpu", DataTypes.DOUBLE()),
                DataTypes.FIELD("ts", DataTypes.TIMESTAMP(3)));
    }

    @SuppressWarnings("unchecked")
    private static int producedRowSize(GreptimeJdbcInputFormat inputFormat) {
        return ((InternalTypeInfo<RowData>) inputFormat.getProducedType()).toRowSize();
    }
}
