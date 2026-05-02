package io.greptime.flink.sink;

import io.greptime.WriteOp;
import io.greptime.flink.cfg.GreptimeChangelogMode;
import java.io.IOException;
import org.apache.flink.types.RowKind;

enum GreptimeRowOperation {
    INSERT("insert", WriteOp.Insert),
    DELETE("delete", WriteOp.Delete);

    private final String metricName;
    private final WriteOp writeOp;

    GreptimeRowOperation(String metricName, WriteOp writeOp) {
        this.metricName = metricName;
        this.writeOp = writeOp;
    }

    String metricName() {
        return metricName;
    }

    WriteOp toWriteOp() {
        return writeOp;
    }

    static GreptimeRowOperation fromRowKind(RowKind rowKind, GreptimeChangelogMode changelogMode) throws IOException {
        if (changelogMode == GreptimeChangelogMode.INSERT_ONLY) {
            if (rowKind == RowKind.INSERT) {
                return INSERT;
            }
            throw new IOException("GreptimeDB sink only supports INSERT RowKind, but got: " + rowKind);
        }

        switch (rowKind) {
            case INSERT:
            case UPDATE_AFTER:
                return INSERT;
            case UPDATE_BEFORE:
            case DELETE:
                return DELETE;
            default:
                throw new IOException("Unsupported GreptimeDB changelog RowKind: " + rowKind);
        }
    }
}
