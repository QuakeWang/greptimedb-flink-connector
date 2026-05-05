package io.greptime.flink.table;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

final class GreptimeShadedSourceDriverMissingProbe {
    private static final String JDBC_URL = "jdbc:mysql://127.0.0.1:4002/public?useSSL=false";

    private GreptimeShadedSourceDriverMissingProbe() {}

    public static void main(String[] args) throws Exception {
        try {
            run(args);
            System.exit(0);
        } catch (Throwable error) {
            error.printStackTrace(System.out);
            System.exit(1);
        }
    }

    private static void run(String[] args) throws Exception {
        if (args.length != 0) {
            throw new IllegalArgumentException("Expected no arguments");
        }

        TableEnvironment tableEnv = TableEnvironment.create(
                EnvironmentSettings.newInstance().inBatchMode().build());
        tableEnv.executeSql("CREATE TEMPORARY TABLE shaded_missing_driver_source ("
                        + " host STRING"
                        + ") WITH ("
                        + " 'connector' = 'greptimedb',"
                        + " 'database' = 'public',"
                        + " 'table' = 'missing_driver_metrics',"
                        + " 'query.jdbc-url' = '"
                        + JDBC_URL
                        + "'"
                        + ")")
                .await();

        try (CloseableIterator<Row> rows = tableEnv.executeSql("SELECT COUNT(*) FROM shaded_missing_driver_source")
                .collect()) {
            while (rows.hasNext()) {
                rows.next();
            }
        } catch (Throwable error) {
            String messages = collectMessages(error);
            System.out.println(messages);
            assertContains(messages, "MySQL-compatible JDBC driver must be available on the Flink classpath");
            assertContains(messages, JDBC_URL);
            assertNotContains(messages, "source-secret");
            assertNotContains(messages, "source-token");
            assertNotContains(messages, "source_user");
            return;
        }

        throw new AssertionError("Expected GreptimeDB source query to fail because MySQL JDBC driver is missing");
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

    private static void assertContains(String value, String expected) {
        if (!value.contains(expected)) {
            throw new AssertionError("Expected output to contain `" + expected + "`, but was: " + value);
        }
    }

    private static void assertNotContains(String value, String unexpected) {
        if (value.contains(unexpected)) {
            throw new AssertionError("Expected output not to contain `" + unexpected + "`, but was: " + value);
        }
    }
}
