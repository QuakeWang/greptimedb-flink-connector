package io.greptime.flink.sink;

import io.greptime.GreptimeDB;
import io.greptime.flink.cfg.GreptimeSinkConfig;
import io.greptime.flink.cfg.GreptimeWriteMode;
import io.greptime.flink.connection.GreptimeClientFactory;
import io.greptime.flink.preflight.GreptimePreflightConfig;
import io.greptime.flink.preflight.GreptimePreflightRunner;
import io.greptime.flink.sink.schema.GreptimeRowDataConverter;
import io.greptime.flink.sink.schema.GreptimeTableSchema;
import java.io.IOException;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.common.operators.ProcessingTimeService;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.table.data.RowData;

public final class GreptimeSink<IN extends RowData> implements Sink<IN> {
    private final GreptimeSinkConfig sinkConfig;
    private final GreptimeTableSchema tableSchema;
    private final GreptimePreflightConfig preflightConfig;

    public GreptimeSink(GreptimeSinkConfig sinkConfig, GreptimeTableSchema tableSchema) {
        this(sinkConfig, tableSchema, GreptimePreflightConfig.disabled());
    }

    public GreptimeSink(
            GreptimeSinkConfig sinkConfig, GreptimeTableSchema tableSchema, GreptimePreflightConfig preflightConfig) {
        this.sinkConfig = sinkConfig;
        this.tableSchema = tableSchema;
        this.preflightConfig = preflightConfig;
    }

    @Override
    public SinkWriter<IN> createWriter(InitContext context) throws IOException {
        return createWriterInternal(
                context.getProcessingTimeService(), context.getMailboxExecutor(), context.metricGroup());
    }

    @Override
    public SinkWriter<IN> createWriter(WriterInitContext context) throws IOException {
        return createWriterInternal(
                context.getProcessingTimeService(), context.getMailboxExecutor(), context.metricGroup());
    }

    private SinkWriter<IN> createWriterInternal(
            ProcessingTimeService processingTimeService,
            MailboxExecutor mailboxExecutor,
            SinkWriterMetricGroup metricGroup)
            throws IOException {
        new GreptimePreflightRunner().runSink(preflightConfig, sinkConfig, tableSchema);
        GreptimeDB client = new GreptimeClientFactory().create(sinkConfig);
        GreptimeRowDataConverter converter = GreptimeRowDataConverter.forSchema(tableSchema);
        if (sinkConfig.getWriteMode() == GreptimeWriteMode.BULK) {
            return new GreptimeBulkSinkWriter<>(
                    new GreptimeDbBulkWriteClient(
                            client, tableSchema, sinkConfig.getBulkWriteConfig(), sinkConfig.createBulkWriteContext()),
                    sinkConfig,
                    tableSchema,
                    converter,
                    processingTimeService,
                    mailboxExecutor,
                    metricGroup);
        }
        return new GreptimeSinkWriter<>(
                new GreptimeDbWriteClient(client),
                sinkConfig,
                tableSchema,
                converter,
                GreptimeRowDataConverter.forKeyColumns(tableSchema),
                processingTimeService,
                mailboxExecutor,
                metricGroup);
    }
}
