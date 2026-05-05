package io.greptime.flink.source;

import io.greptime.flink.query.GreptimeQueryConfig;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.InputFormatProvider;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsLimitPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

public final class GreptimeDynamicTableSource
        implements ScanTableSource, SupportsProjectionPushDown, SupportsLimitPushDown {
    private final GreptimeQueryConfig queryConfig;
    private final DataType physicalRowDataType;
    private final DeferredValidationFailure deferredValidationFailure;
    private DataType producedDataType;
    private int[][] projectedFields;
    private Long limit;

    public GreptimeDynamicTableSource(GreptimeQueryConfig queryConfig, DataType physicalRowDataType) {
        this(queryConfig, null, physicalRowDataType);
    }

    private GreptimeDynamicTableSource(
            GreptimeQueryConfig queryConfig,
            DeferredValidationFailure deferredValidationFailure,
            DataType physicalRowDataType) {
        if ((queryConfig == null) == (deferredValidationFailure == null)) {
            throw new IllegalArgumentException("Exactly one of queryConfig and deferredValidationFailure must be set");
        }
        this.queryConfig = queryConfig;
        this.physicalRowDataType = Objects.requireNonNull(physicalRowDataType, "physicalRowDataType");
        this.deferredValidationFailure = deferredValidationFailure;
        this.producedDataType = physicalRowDataType;
    }

    public static GreptimeDynamicTableSource withDeferredValidationFailure(
            DeferredValidationFailure deferredValidationFailure, DataType physicalRowDataType) {
        // Flink may wrap factory-time errors with raw table options. Defer URL-secret
        // validation so diagnostics contain only the redacted JDBC URL.
        return new GreptimeDynamicTableSource(
                null,
                Objects.requireNonNull(deferredValidationFailure, "deferredValidationFailure"),
                physicalRowDataType);
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        if (deferredValidationFailure != null) {
            throw deferredValidationFailure.asException();
        }
        GreptimeJdbcInputFormat inputFormat = new GreptimeJdbcInputFormat(
                queryConfig, (RowType) producedDataType.getLogicalType(), resolveProjectedColumns(), limit);
        return InputFormatProvider.of(inputFormat, 1);
    }

    @Override
    public DynamicTableSource copy() {
        GreptimeDynamicTableSource copy = deferredValidationFailure == null
                ? new GreptimeDynamicTableSource(queryConfig, physicalRowDataType)
                : withDeferredValidationFailure(deferredValidationFailure, physicalRowDataType);
        copy.producedDataType = producedDataType;
        copy.projectedFields = copyProjectedFields(projectedFields);
        copy.limit = limit;
        return copy;
    }

    @Override
    public String asSummaryString() {
        return "GreptimeDB Table Source";
    }

    @Override
    public boolean supportsNestedProjection() {
        return false;
    }

    @Override
    public void applyProjection(int[][] projectedFields, DataType producedDataType) {
        Objects.requireNonNull(projectedFields, "projectedFields");
        Objects.requireNonNull(producedDataType, "producedDataType");
        RowType physicalRowType = (RowType) physicalRowDataType.getLogicalType();
        RowType producedRowType = (RowType) producedDataType.getLogicalType();
        if (producedRowType.getFieldCount() != projectedFields.length) {
            throw new IllegalArgumentException("Produced field count must match projection field count");
        }
        for (int[] path : projectedFields) {
            if (path.length != 1) {
                throw new IllegalArgumentException("GreptimeDB source only supports top-level projection");
            }
            if (path[0] < 0 || path[0] >= physicalRowType.getFieldCount()) {
                throw new IllegalArgumentException("Projection field index out of range: " + path[0]);
            }
        }
        this.projectedFields = copyProjectedFields(projectedFields);
        this.producedDataType = producedDataType;
    }

    @Override
    public void applyLimit(long limit) {
        if (limit < 0) {
            throw new IllegalArgumentException("limit must be greater than or equal to 0");
        }
        this.limit = limit;
    }

    GreptimeQueryConfig getQueryConfig() {
        return queryConfig;
    }

    DataType getProducedDataType() {
        return producedDataType;
    }

    Long getLimit() {
        return limit;
    }

    int[][] getProjectedFields() {
        return copyProjectedFields(projectedFields);
    }

    private List<String> resolveProjectedColumns() {
        RowType physicalRowType = (RowType) physicalRowDataType.getLogicalType();
        List<String> fieldNames = physicalRowType.getFieldNames();
        if (projectedFields == null) {
            return fieldNames;
        }
        return Arrays.stream(projectedFields)
                .map(path -> fieldNames.get(path[0]))
                .collect(Collectors.toList());
    }

    private static int[][] copyProjectedFields(int[][] projectedFields) {
        if (projectedFields == null) {
            return null;
        }
        int[][] copy = new int[projectedFields.length][];
        for (int i = 0; i < projectedFields.length; i++) {
            copy[i] = Arrays.copyOf(projectedFields[i], projectedFields[i].length);
        }
        return copy;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof GreptimeDynamicTableSource)) {
            return false;
        }
        GreptimeDynamicTableSource that = (GreptimeDynamicTableSource) other;
        return Objects.equals(queryConfig, that.queryConfig)
                && Objects.equals(physicalRowDataType, that.physicalRowDataType)
                && Objects.equals(deferredValidationFailure, that.deferredValidationFailure)
                && Objects.equals(producedDataType, that.producedDataType)
                && Arrays.deepEquals(projectedFields, that.projectedFields)
                && Objects.equals(limit, that.limit);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(queryConfig, physicalRowDataType, deferredValidationFailure, producedDataType, limit);
        result = 31 * result + Arrays.deepHashCode(projectedFields);
        return result;
    }

    public static final class DeferredValidationFailure {
        private final String message;

        private DeferredValidationFailure(String message) {
            this.message = Objects.requireNonNull(message, "message");
        }

        public static DeferredValidationFailure sensitiveJdbcUrl(String redactedJdbcUrl) {
            return new DeferredValidationFailure(
                    "`query.jdbc-url` must not contain credentials or authentication tokens; configure `username` and `password` options instead, jdbcUrl="
                            + Objects.requireNonNull(redactedJdbcUrl, "redactedJdbcUrl"));
        }

        private ValidationException asException() {
            return new ValidationException(message);
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (!(other instanceof DeferredValidationFailure)) {
                return false;
            }
            DeferredValidationFailure that = (DeferredValidationFailure) other;
            return Objects.equals(message, that.message);
        }

        @Override
        public int hashCode() {
            return Objects.hash(message);
        }
    }
}
