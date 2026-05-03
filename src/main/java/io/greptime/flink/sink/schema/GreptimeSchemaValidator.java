package io.greptime.flink.sink.schema;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;

public final class GreptimeSchemaValidator {
    private GreptimeSchemaValidator() {}

    public static ValidationResult validate(RowType rowType, String timeIndex, List<String> tags) {
        Objects.requireNonNull(rowType, "rowType");
        Objects.requireNonNull(timeIndex, "timeIndex");
        Objects.requireNonNull(tags, "tags");

        Set<String> fieldNames = new HashSet<>();
        RowType.RowField timeIndexField = null;
        for (RowType.RowField field : rowType.getFields()) {
            fieldNames.add(field.getName());
            if (field.getName().equals(timeIndex)) {
                timeIndexField = field;
            }
        }

        if (timeIndexField == null) {
            throw new IllegalArgumentException("Unknown time-index column: " + timeIndex);
        }
        if (!isTimestamp(timeIndexField)) {
            throw new IllegalArgumentException(
                    "time-index column must be TIMESTAMP or TIMESTAMP_LTZ, but was: " + timeIndexField.getType());
        }
        if (timeIndexField.getType().isNullable()) {
            throw new IllegalArgumentException("time-index column must be declared NOT NULL: " + timeIndex);
        }

        Set<String> requestedTags = new HashSet<>();
        for (String tag : tags) {
            if (!fieldNames.contains(tag)) {
                throw new IllegalArgumentException("Unknown tag column: " + tag);
            }
            if (tag.equals(timeIndex)) {
                throw new IllegalArgumentException("Tag column cannot be the same as time-index: " + tag);
            }
            if (!requestedTags.add(tag)) {
                throw new IllegalArgumentException("Duplicate tag column: " + tag);
            }
        }

        List<String> physicalTagOrder = resolvePhysicalTagOrder(rowType, requestedTags);
        if (!tags.equals(physicalTagOrder)) {
            throw new IllegalArgumentException(String.format(
                    "`tags` must follow the physical column order of tag columns, tags=%s, physicalTagOrder=%s",
                    tags, physicalTagOrder));
        }

        return new ValidationResult(tags, physicalTagOrder);
    }

    private static List<String> resolvePhysicalTagOrder(RowType rowType, Set<String> tagColumns) {
        List<String> physicalTagOrder = new ArrayList<>(tagColumns.size());
        rowType.getFields().forEach(field -> {
            if (tagColumns.contains(field.getName())) {
                physicalTagOrder.add(field.getName());
            }
        });
        return physicalTagOrder;
    }

    private static boolean isTimestamp(RowType.RowField field) {
        LogicalTypeRoot root = field.getType().getTypeRoot();
        return root == LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE
                || root == LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE;
    }

    public static final class ValidationResult {
        private final List<String> tags;
        private final List<String> physicalTagOrder;

        private ValidationResult(List<String> tags, List<String> physicalTagOrder) {
            this.tags = List.copyOf(tags);
            this.physicalTagOrder = List.copyOf(physicalTagOrder);
        }

        public void validatePrimaryKeyMatchesTags(List<String> primaryKeyColumns) {
            if (!primaryKeyColumns.equals(physicalTagOrder)) {
                throw new IllegalArgumentException(String.format(
                        "`PRIMARY KEY` must match the Greptime tag order derived from physical columns, PRIMARY KEY=%s, tags=%s, physicalTagOrder=%s",
                        primaryKeyColumns, tags, physicalTagOrder));
            }
        }
    }
}
