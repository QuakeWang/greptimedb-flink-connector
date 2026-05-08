package io.greptime.flink.preflight;

import io.greptime.flink.metadata.GreptimeColumnMetadata;
import io.greptime.flink.metadata.GreptimeTableMetadata;
import io.greptime.flink.sink.schema.GreptimeTableSchema;
import io.greptime.models.TableSchema;
import io.greptime.v1.Common;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.flink.table.types.logical.RowType;

public final class GreptimeTableInspector {
    public List<PreflightFinding> inspectBulkSink(
            String database, GreptimeTableSchema localSchema, Optional<GreptimeTableMetadata> remoteMetadata) {
        if (remoteMetadata.isEmpty()) {
            return List.of(PreflightFinding.of(
                    "missing-table",
                    database + "." + localSchema.getTableName(),
                    "bulk sink target",
                    "missing",
                    "bulk write requires an existing GreptimeDB table"));
        }

        List<PreflightFinding> findings = new ArrayList<>();
        GreptimeTableMetadata remote = remoteMetadata.get();
        inspectColumns(localSchema, remote, findings);
        inspectTimeIndex(localSchema, remote, findings);
        inspectPrimaryKeySet(localSchema, remote, findings);
        return List.copyOf(findings);
    }

    private static void inspectColumns(
            GreptimeTableSchema localSchema, GreptimeTableMetadata remote, List<PreflightFinding> findings) {
        TableSchema expectedSchema = localSchema.toGreptimeTableSchema();
        List<String> localNames = expectedSchema.getColumnNames();
        List<GreptimeColumnMetadata> remoteColumns = remote.getColumns();
        if (localNames.size() != remoteColumns.size()) {
            findings.add(PreflightFinding.of(
                    "column-count-mismatch",
                    "columns",
                    localNames.toString(),
                    remoteColumns.stream()
                            .map(GreptimeColumnMetadata::getName)
                            .collect(Collectors.toList())
                            .toString(),
                    "bulk write requires the same number of physical columns"));
        }

        RowType rowType = (RowType) localSchema.getPhysicalRowDataType().getLogicalType();
        int comparableColumns = Math.min(localNames.size(), remoteColumns.size());
        for (int i = 0; i < comparableColumns; i++) {
            GreptimeColumnMetadata remoteColumn = remoteColumns.get(i);
            String localName = localNames.get(i);
            if (!localName.equals(remoteColumn.getName())) {
                findings.add(columnNameFinding(localName, remoteColumn, remote));
                continue;
            }
            inspectType(expectedSchema, remoteColumn, i, findings);
            inspectNullability(rowType.getFields().get(i), remoteColumn, findings);
        }
    }

    private static PreflightFinding columnNameFinding(
            String localName, GreptimeColumnMetadata remoteColumn, GreptimeTableMetadata remote) {
        String category = remote.column(localName).isPresent() ? "column-order-mismatch" : "missing-column";
        return PreflightFinding.of(
                category,
                "column " + localName,
                localName,
                remoteColumn.getName(),
                "bulk write requires exact physical column order");
    }

    private static void inspectType(
            TableSchema expectedSchema,
            GreptimeColumnMetadata remoteColumn,
            int position,
            List<PreflightFinding> findings) {
        Common.ColumnDataType localType = expectedSchema.getDataTypes().get(position);
        String expectedGreptimeType = GreptimeTypeCompatibility.expectedType(expectedSchema, position);
        if (!GreptimeTypeCompatibility.supports(localType)) {
            findings.add(PreflightFinding.of(
                    "unsupported-local-type",
                    "column " + remoteColumn.getName(),
                    localType.name(),
                    remoteType(remoteColumn),
                    "local type has no GreptimeDB metadata mapping"));
            return;
        }
        if (!GreptimeTypeCompatibility.matches(localType, remoteColumn.getGreptimeDataType())) {
            findings.add(PreflightFinding.of(
                    "type-mismatch",
                    "column " + remoteColumn.getName(),
                    expectedGreptimeType,
                    remoteType(remoteColumn),
                    "bulk write requires exact GreptimeDB column type"));
            return;
        }
        inspectDecimalType(expectedSchema, remoteColumn, position, findings);
    }

    private static void inspectDecimalType(
            TableSchema expectedSchema,
            GreptimeColumnMetadata remoteColumn,
            int position,
            List<PreflightFinding> findings) {
        if (expectedSchema.getDataTypes().get(position) != Common.ColumnDataType.DECIMAL128) {
            return;
        }
        Common.ColumnDataTypeExtension extension =
                expectedSchema.getDataTypeExtensions().get(position);
        int localPrecision = extension.getDecimalType().getPrecision();
        int localScale = extension.getDecimalType().getScale();
        if (remoteColumn.getNumericPrecision() == null || remoteColumn.getNumericScale() == null) {
            findings.add(PreflightFinding.of(
                    "unsupported-remote-metadata",
                    "column " + remoteColumn.getName(),
                    "DECIMAL(" + localPrecision + ", " + localScale + ")",
                    remoteType(remoteColumn),
                    "remote decimal precision and scale must be present"));
            return;
        }
        if (localPrecision != remoteColumn.getNumericPrecision() || localScale != remoteColumn.getNumericScale()) {
            findings.add(PreflightFinding.of(
                    "type-mismatch",
                    "column " + remoteColumn.getName(),
                    "DECIMAL(" + localPrecision + ", " + localScale + ")",
                    "DECIMAL(" + remoteColumn.getNumericPrecision() + ", " + remoteColumn.getNumericScale() + ")",
                    "bulk write requires exact decimal precision and scale"));
        }
    }

    private static void inspectNullability(
            RowType.RowField localField, GreptimeColumnMetadata remoteColumn, List<PreflightFinding> findings) {
        if (localField.getType().isNullable() && !remoteColumn.isNullable()) {
            findings.add(PreflightFinding.of(
                    "nullability-mismatch",
                    "column " + localField.getName(),
                    "nullable=true",
                    "nullable=false",
                    "local nullable column may write null into a remote NOT NULL column"));
        }
    }

    private static void inspectTimeIndex(
            GreptimeTableSchema localSchema, GreptimeTableMetadata remote, List<PreflightFinding> findings) {
        String localTimeIndex = localSchema.getTimeIndex();
        Optional<String> remoteTimeIndex = remote.getTimeIndexColumn();
        if (remoteTimeIndex.isEmpty() || !localTimeIndex.equals(remoteTimeIndex.get())) {
            findings.add(PreflightFinding.of(
                    "time-index-mismatch",
                    "time-index",
                    localTimeIndex,
                    remoteTimeIndex.orElse("missing"),
                    "bulk write requires matching GreptimeDB time index column"));
            return;
        }
        GreptimeColumnMetadata remoteTimeIndexColumn =
                remote.column(localTimeIndex).orElseThrow();
        if (remoteTimeIndexColumn.isNullable()) {
            findings.add(PreflightFinding.of(
                    "nullability-mismatch",
                    "column " + localTimeIndex,
                    "time-index nullable=false",
                    "time-index nullable=true",
                    "GreptimeDB time index must be NOT NULL"));
        }
    }

    private static void inspectPrimaryKeySet(
            GreptimeTableSchema localSchema, GreptimeTableMetadata remote, List<PreflightFinding> findings) {
        Set<String> localTags = Set.copyOf(localSchema.getTagColumns());
        if (!localTags.equals(remote.getPrimaryKeyColumnSet())) {
            findings.add(PreflightFinding.of(
                    "row-key-set-mismatch",
                    "tags",
                    localSchema.getTagColumns().toString(),
                    remote.getPrimaryKeyColumnSet().toString(),
                    "bulk write requires local tags to match remote primary key column set"));
        }
    }

    private static String remoteType(GreptimeColumnMetadata column) {
        return column.getGreptimeDataType() + " " + column.getSemanticType() + " nullable=" + column.isNullable();
    }
}
