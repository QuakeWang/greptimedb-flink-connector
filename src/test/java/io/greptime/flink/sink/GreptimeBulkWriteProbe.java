package io.greptime.flink.sink;

import io.greptime.BulkStreamWriter;
import io.greptime.BulkWrite;
import io.greptime.GreptimeDB;
import io.greptime.flink.sink.schema.GreptimeRowDataConverter;
import io.greptime.flink.sink.schema.GreptimeTableSchema;
import io.greptime.models.Table;
import io.greptime.rpc.Context;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import org.apache.flink.table.data.RowData;

final class GreptimeBulkWriteProbe {
    private final GreptimeDB client;
    private final GreptimeTableSchema tableSchema;
    private final GreptimeRowDataConverter rowDataConverter;
    private final BulkWrite.Config bulkConfig;
    private final Context context;
    private final int columnBufferSize;

    GreptimeBulkWriteProbe(
            GreptimeDB client,
            GreptimeTableSchema tableSchema,
            BulkWrite.Config bulkConfig,
            Context context,
            int columnBufferSize) {
        this.client = Objects.requireNonNull(client, "client");
        this.tableSchema = Objects.requireNonNull(tableSchema, "tableSchema");
        this.rowDataConverter = GreptimeRowDataConverter.forSchema(tableSchema);
        this.bulkConfig = Objects.requireNonNull(bulkConfig, "bulkConfig");
        this.context = Objects.requireNonNull(context, "context");
        if (columnBufferSize <= 0) {
            throw new IllegalArgumentException("columnBufferSize must be positive");
        }
        this.columnBufferSize = columnBufferSize;
    }

    Result writeRows(List<RowData> rows) throws IOException {
        Objects.requireNonNull(rows, "rows");
        if (rows.isEmpty()) {
            throw new IllegalArgumentException("rows must not be empty");
        }

        int inputRows = rows.size();
        long bytesUsed = -1L;
        Integer affectedRows = null;

        try (BulkStreamWriter writer =
                client.bulkStreamWriter(tableSchema.toGreptimeTableSchema(), bulkConfig, context)) {
            Table.TableBufferRoot table = writer.tableBufferRoot(columnBufferSize);
            for (RowData row : rows) {
                table.addRow(rowDataConverter.convert(row));
            }
            table.complete();
            bytesUsed = table.bytesUsed();

            affectedRows = writer.writeNext().get(bulkConfig.getTimeoutMsPerMessage(), TimeUnit.MILLISECONDS);
            if (affectedRows != inputRows) {
                throw new IOException("Bulk write probe returned unexpected affected rows: "
                        + describeAttempt(inputRows, bytesUsed, affectedRows));
            }

            writer.completed();
            return new Result(inputRows, affectedRows, bytesUsed);
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException("Bulk write probe failed: " + describeAttempt(inputRows, bytesUsed, affectedRows), e);
        }
    }

    private String describeAttempt(int inputRows, long bytesUsed, Integer affectedRows) {
        return "table="
                + tableSchema.getTableName()
                + ",inputRows="
                + inputRows
                + ",affectedRows="
                + valueOrUnknown(affectedRows)
                + ",bytesUsed="
                + valueOrUnknown(bytesUsed)
                + ",columnBufferSize="
                + columnBufferSize
                + ",allocatorInitReservation="
                + bulkConfig.getAllocatorInitReservation()
                + ",allocatorMaxAllocation="
                + bulkConfig.getAllocatorMaxAllocation()
                + ",timeoutMsPerMessage="
                + bulkConfig.getTimeoutMsPerMessage()
                + ",maxRequestsInFlight="
                + bulkConfig.getMaxRequestsInFlight();
    }

    private static String valueOrUnknown(Integer value) {
        return value == null ? "unknown" : value.toString();
    }

    private static String valueOrUnknown(long value) {
        return value < 0 ? "unknown" : Long.toString(value);
    }

    static final class Result {
        private final int inputRows;
        private final int affectedRows;
        private final long bytesUsed;

        private Result(int inputRows, int affectedRows, long bytesUsed) {
            this.inputRows = inputRows;
            this.affectedRows = affectedRows;
            this.bytesUsed = bytesUsed;
        }

        int getInputRows() {
            return inputRows;
        }

        int getAffectedRows() {
            return affectedRows;
        }

        long getBytesUsed() {
            return bytesUsed;
        }
    }
}
