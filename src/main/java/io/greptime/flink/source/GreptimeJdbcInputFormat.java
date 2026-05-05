package io.greptime.flink.source;

import io.greptime.flink.query.GreptimeQueryClient;
import io.greptime.flink.query.GreptimeQueryConfig;
import io.greptime.flink.query.GreptimeQuerySqlBuilder;
import io.greptime.flink.query.GreptimeResultSetRowDataConverter;
import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Objects;
import org.apache.flink.api.common.io.DefaultInputSplitAssigner;
import org.apache.flink.api.common.io.NonParallelInput;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

public final class GreptimeJdbcInputFormat extends RichInputFormat<RowData, GenericInputSplit>
        implements NonParallelInput, ResultTypeQueryable<RowData> {
    private static final long serialVersionUID = 1L;

    private final GreptimeQueryConfig queryConfig;
    private final RowType producedRowType;
    private final List<String> projectedColumns;
    private final Long limit;
    private final String sql;
    private final GreptimeResultSetRowDataConverter converter;

    private transient GreptimeQueryClient queryClient;
    private transient GreptimeQueryClient.QueryResult queryResult;
    private transient boolean hasNext;

    public GreptimeJdbcInputFormat(
            GreptimeQueryConfig queryConfig, RowType producedRowType, List<String> projectedColumns, Long limit) {
        this.queryConfig = Objects.requireNonNull(queryConfig, "queryConfig");
        this.producedRowType = Objects.requireNonNull(producedRowType, "producedRowType");
        this.projectedColumns = List.copyOf(projectedColumns);
        this.limit = limit;
        this.sql = new GreptimeQuerySqlBuilder(queryConfig.getDialect())
                .buildSelect(queryConfig.getDatabase(), queryConfig.getTable(), this.projectedColumns, limit);
        this.converter = new GreptimeResultSetRowDataConverter(producedRowType);
    }

    @Override
    public void configure(Configuration parameters) {}

    @Override
    public BaseStatistics getStatistics(BaseStatistics cachedStatistics) {
        return cachedStatistics;
    }

    @Override
    public GenericInputSplit[] createInputSplits(int minNumSplits) {
        return new GenericInputSplit[] {new GenericInputSplit(0, 1)};
    }

    @Override
    public InputSplitAssigner getInputSplitAssigner(GenericInputSplit[] inputSplits) {
        return new DefaultInputSplitAssigner(inputSplits);
    }

    @Override
    public void open(GenericInputSplit split) throws IOException {
        if (split.getSplitNumber() != 0 || split.getTotalNumberOfSplits() != 1) {
            throw new IOException("GreptimeDB source expects exactly one input split, but was: " + split);
        }
        close();
        queryClient = new GreptimeQueryClient(queryConfig);
        try {
            queryResult = queryClient.executeQuery(sql, projectedColumns, limit);
            advance();
        } catch (IOException e) {
            closeAfterOpenFailure(e);
            throw e;
        }
    }

    @Override
    public boolean reachedEnd() {
        return !hasNext;
    }

    @Override
    public RowData nextRecord(RowData reuse) throws IOException {
        if (!hasNext) {
            return null;
        }
        try {
            RowData row = converter.convert(queryResult.getResultSet());
            advance();
            return row;
        } catch (SQLException e) {
            throw queryClient.queryException("read-row", projectedColumns, limit, e);
        }
    }

    @Override
    public void close() throws IOException {
        hasNext = false;
        GreptimeQueryClient.QueryResult result = queryResult;
        queryResult = null;
        if (result != null) {
            result.close();
        }
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return InternalTypeInfo.of(producedRowType);
    }

    String getSql() {
        return sql;
    }

    List<String> getProjectedColumns() {
        return projectedColumns;
    }

    private void advance() throws IOException {
        try {
            hasNext = queryResult.next();
        } catch (SQLException e) {
            throw queryClient.queryException("read-next", projectedColumns, limit, e);
        }
    }

    private void closeAfterOpenFailure(IOException openError) {
        try {
            close();
        } catch (IOException closeError) {
            openError.addSuppressed(closeError);
        }
    }
}
