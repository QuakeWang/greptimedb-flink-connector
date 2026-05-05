package io.greptime.flink.table;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import io.greptime.flink.source.GreptimeDynamicTableSource;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Test;

class GreptimeDynamicTableSourceFactoryTest {
    @Test
    void exposesNoGlobalRequiredOptions() {
        GreptimeDynamicTableFactory factory = new GreptimeDynamicTableFactory();

        assertTrue(factory.requiredOptions().isEmpty());
        assertTrue(factory.forwardOptions().isEmpty());
        assertTrue(factory.optionalOptions().contains(GreptimeConnectorOptions.ENDPOINTS));
        assertTrue(factory.optionalOptions().contains(GreptimeConnectorOptions.TIME_INDEX));
        assertTrue(factory.optionalOptions().contains(GreptimeConnectorOptions.QUERY_JDBC_URL));
    }

    @Test
    void createsSourceWithoutSinkRequiredOptions() {
        Map<String, String> options = sourceOptions();

        DynamicTableSource source = new GreptimeDynamicTableFactory()
                .createDynamicTableSource(new TestFactoryContext(options, resolvedSchema(), "identifier_metrics"));

        assertTrue(source instanceof GreptimeDynamicTableSource);
    }

    @Test
    void sourceIgnoresSinkOnlyValidation() {
        Map<String, String> options = sourceOptions();
        options.put(GreptimeConnectorOptions.SINK_WRITE_MODE.key(), "invalid-write-mode");
        options.put(GreptimeConnectorOptions.BATCH_MAX_ROWS.key(), "abc");
        options.put(GreptimeConnectorOptions.TIME_INDEX.key(), "missing_column");
        options.put(GreptimeConnectorOptions.TAGS.key(), "missing_tag");

        DynamicTableSource source = new GreptimeDynamicTableFactory()
                .createDynamicTableSource(new TestFactoryContext(options, resolvedSchema(), "identifier_metrics"));

        assertTrue(source instanceof GreptimeDynamicTableSource);
    }

    @Test
    void sourceStillRejectsUnknownOptions() {
        Map<String, String> options = sourceOptions();
        options.put("batch.max-row", "100");

        ValidationException error = assertThrows(ValidationException.class, () -> new GreptimeDynamicTableFactory()
                .createDynamicTableSource(new TestFactoryContext(options, resolvedSchema(), "identifier_metrics")));

        assertTrue(error.getMessage().contains("Unsupported options"));
        assertTrue(error.getMessage().contains("batch.max-row"));
    }

    @Test
    void rejectsMissingAndUnsupportedSourceJdbcUrl() {
        IllegalArgumentException missing =
                assertThrows(IllegalArgumentException.class, () -> new GreptimeDynamicTableFactory()
                        .createDynamicTableSource(new TestFactoryContext(Map.of(), resolvedSchema(), "metrics")));
        assertEquals("`query.jdbc-url` is required for GreptimeDB source", missing.getMessage());

        Map<String, String> postgresOptions = sourceOptions();
        postgresOptions.put(GreptimeConnectorOptions.QUERY_JDBC_URL.key(), "jdbc:postgresql://127.0.0.1:4003/public");

        IllegalArgumentException unsupported =
                assertThrows(IllegalArgumentException.class, () -> new GreptimeDynamicTableFactory()
                        .createDynamicTableSource(
                                new TestFactoryContext(postgresOptions, resolvedSchema(), "metrics")));
        assertTrue(unsupported.getMessage().contains("currently supports only MySQL JDBC"));
    }

    @Test
    void tableEnvironmentSourceCreationFailureDoesNotExposeExplicitPassword() throws Exception {
        TableEnvironment tableEnv = TableEnvironment.create(
                EnvironmentSettings.newInstance().inBatchMode().build());
        tableEnv.executeSql("CREATE TEMPORARY TABLE source_failure ("
                        + " host STRING"
                        + ") WITH ("
                        + " 'connector' = 'greptimedb',"
                        + " 'query.jdbc-url' = 'jdbc:postgresql://127.0.0.1:4003/public',"
                        + " 'username' = 'source_user',"
                        + " 'password' = 'source-secret'"
                        + ")")
                .await();

        ValidationException error =
                assertThrows(ValidationException.class, () -> tableEnv.explainSql("SELECT host FROM source_failure"));
        String messages = collectMessages(error);

        assertTrue(messages.contains("currently supports only MySQL JDBC"));
        assertTrue(messages.contains("jdbc:postgresql://127.0.0.1:4003/public"));
        assertFalse(messages.contains("source-secret"));
    }

    @Test
    void tableEnvironmentSourceUrlSecretFailureDoesNotExposeSecret() throws Exception {
        TableEnvironment tableEnv = TableEnvironment.create(
                EnvironmentSettings.newInstance().inBatchMode().build());
        tableEnv.executeSql("CREATE TEMPORARY TABLE source_url_secret_failure ("
                        + " host STRING"
                        + ") WITH ("
                        + " 'connector' = 'greptimedb',"
                        + " 'query.jdbc-url' = 'jdbc:mysql://127.0.0.1:4002/public?trustCertificateKeyStorePassword=source-secret&useSSL=false'"
                        + ")")
                .await();

        Exception error = assertThrows(Exception.class, () -> {
            collectAllRows(tableEnv, "SELECT COUNT(*) FROM source_url_secret_failure");
        });
        String messages = collectMessages(error);

        assertTrue(messages.contains("`query.jdbc-url` must not contain credentials or authentication tokens"));
        assertTrue(messages.contains("trustCertificateKeyStorePassword=****"));
        assertFalse(messages.contains("source-secret"));
    }

    @Test
    void sinkStillRequiresSinkOptions() {
        Map<String, String> noEndpoints = new HashMap<>();
        noEndpoints.put(GreptimeConnectorOptions.TIME_INDEX.key(), "ts");
        IllegalArgumentException missingEndpoints =
                assertThrows(IllegalArgumentException.class, () -> new GreptimeDynamicTableFactory()
                        .createDynamicTableSink(new TestFactoryContext(noEndpoints, resolvedSchema(), "metrics")));
        assertEquals("Missing required GreptimeDB sink option: `endpoints`", missingEndpoints.getMessage());

        Map<String, String> noTimeIndex = new HashMap<>();
        noTimeIndex.put(GreptimeConnectorOptions.ENDPOINTS.key(), "127.0.0.1:4001");
        IllegalArgumentException missingTimeIndex =
                assertThrows(IllegalArgumentException.class, () -> new GreptimeDynamicTableFactory()
                        .createDynamicTableSink(new TestFactoryContext(noTimeIndex, resolvedSchema(), "metrics")));
        assertEquals("Missing required GreptimeDB sink option: `time-index`", missingTimeIndex.getMessage());
    }

    @Test
    void sinkIgnoresSourceOnlyValidation() {
        Map<String, String> options = sinkOptions();
        options.put(GreptimeConnectorOptions.QUERY_FETCH_SIZE.key(), "abc");

        DynamicTableSink sink = new GreptimeDynamicTableFactory()
                .createDynamicTableSink(new TestFactoryContext(options, sinkResolvedSchema(), "identifier_metrics"));

        assertTrue(sink instanceof GreptimeDynamicTableSink);
    }

    private static Map<String, String> sourceOptions() {
        Map<String, String> options = new HashMap<>();
        options.put(GreptimeConnectorOptions.QUERY_JDBC_URL.key(), "jdbc:mysql://127.0.0.1:4002/public?useSSL=false");
        return options;
    }

    private static Map<String, String> sinkOptions() {
        Map<String, String> options = new HashMap<>();
        options.put(GreptimeConnectorOptions.ENDPOINTS.key(), "127.0.0.1:4001");
        options.put(GreptimeConnectorOptions.TIME_INDEX.key(), "ts");
        options.put(GreptimeConnectorOptions.TAGS.key(), "host");
        return options;
    }

    private static ResolvedSchema resolvedSchema() {
        return new ResolvedSchema(
                List.of(
                        Column.physical("host", DataTypes.STRING()),
                        Column.physical("region", DataTypes.STRING()),
                        Column.physical("cpu", DataTypes.DOUBLE()),
                        Column.physical("ts", DataTypes.TIMESTAMP(3))),
                List.of(),
                null);
    }

    private static ResolvedSchema sinkResolvedSchema() {
        return new ResolvedSchema(
                List.of(
                        Column.physical("host", DataTypes.STRING()),
                        Column.physical("region", DataTypes.STRING()),
                        Column.physical("cpu", DataTypes.DOUBLE()),
                        Column.physical("ts", DataTypes.TIMESTAMP(3).notNull())),
                List.of(),
                null);
    }

    private static void collectAllRows(TableEnvironment tableEnv, String sql) throws Exception {
        try (CloseableIterator<Row> rows = tableEnv.executeSql(sql).collect()) {
            while (rows.hasNext()) {
                rows.next();
            }
        }
    }

    private static String collectMessages(Throwable error) {
        StringBuilder builder = new StringBuilder();
        Throwable current = error;
        while (current != null) {
            if (builder.length() > 0) {
                builder.append(" | ");
            }
            builder.append(current.getClass().getSimpleName());
            builder.append(": ");
            builder.append(current.getMessage());
            for (Throwable suppressed : current.getSuppressed()) {
                builder.append(" | suppressed ");
                builder.append(suppressed.getClass().getSimpleName());
                builder.append(": ");
                builder.append(suppressed.getMessage());
            }
            current = current.getCause();
        }
        return builder.toString();
    }

    private static final class TestFactoryContext implements DynamicTableFactory.Context {
        private final ObjectIdentifier objectIdentifier;
        private final ResolvedCatalogTable catalogTable;
        private final Configuration configuration = new Configuration();

        private TestFactoryContext(Map<String, String> options, ResolvedSchema resolvedSchema, String objectName) {
            CatalogTable table = CatalogTable.newBuilder()
                    .schema(Schema.newBuilder()
                            .fromResolvedSchema(resolvedSchema)
                            .build())
                    .options(options)
                    .build();
            this.objectIdentifier = ObjectIdentifier.of("default_catalog", "default_database", objectName);
            this.catalogTable = new ResolvedCatalogTable(table, resolvedSchema);
        }

        @Override
        public ObjectIdentifier getObjectIdentifier() {
            return objectIdentifier;
        }

        @Override
        public ResolvedCatalogTable getCatalogTable() {
            return catalogTable;
        }

        @Override
        public Map<String, String> getEnrichmentOptions() {
            return Map.of();
        }

        @Override
        public ReadableConfig getConfiguration() {
            return configuration;
        }

        @Override
        public ClassLoader getClassLoader() {
            return Thread.currentThread().getContextClassLoader();
        }

        @Override
        public boolean isTemporary() {
            return false;
        }
    }
}
