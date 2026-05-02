package io.greptime.flink.sink;

import io.greptime.flink.cfg.GreptimeSinkConfig;
import io.greptime.flink.sink.schema.GreptimeTableSchema;
import java.util.List;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.InstantiationUtil;
import org.junit.jupiter.api.Test;

class GreptimeSinkSerializationTest {
    @Test
    void sinkIsSerializableForFlinkJobSubmission() throws Exception {
        GreptimeSinkConfig sinkConfig = GreptimeSinkConfig.builder()
                .endpoints(List.of("127.0.0.1:4001"))
                .batchMaxRows(100)
                .build();

        GreptimeTableSchema tableSchema = GreptimeTableSchema.from(
                "metrics",
                DataTypes.ROW(
                        DataTypes.FIELD("host", DataTypes.STRING()),
                        DataTypes.FIELD("cpu", DataTypes.DOUBLE()),
                        DataTypes.FIELD("ts", DataTypes.TIMESTAMP(3).notNull())),
                "ts",
                List.of("host"));

        GreptimeSink<RowData> sink = new GreptimeSink<>(sinkConfig, tableSchema);
        InstantiationUtil.clone(sink);
    }
}
