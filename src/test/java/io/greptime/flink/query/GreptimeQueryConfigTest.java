package io.greptime.flink.query;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;
import org.junit.jupiter.api.Test;

class GreptimeQueryConfigTest {
    @Test
    void createsMysqlJdbcQueryConfig() {
        GreptimeQueryConfig config = GreptimeQueryConfig.builder()
                .jdbcUrl("jdbc:mysql://127.0.0.1:4002/public?useSSL=false")
                .database("public")
                .table("metrics")
                .credentials("greptime", "secret")
                .connectTimeoutMs(1234)
                .socketTimeoutMs(5678)
                .fetchSize(256)
                .build();

        Properties properties = config.createJdbcProperties();

        assertEquals(GreptimeQueryDialect.MYSQL, config.getDialect());
        assertEquals("mysql-jdbc", config.getDialect().protocolName());
        assertEquals("1234", properties.getProperty("connectTimeout"));
        assertEquals("5678", properties.getProperty("socketTimeout"));
        assertEquals("greptime", properties.getProperty("user"));
        assertEquals("secret", properties.getProperty("password"));
        assertEquals(256, config.getFetchSize());
    }

    @Test
    void ensuresMysqlJdbcDriverIsLoadable() {
        assertDoesNotThrow(() -> GreptimeQueryDialect.MYSQL.ensureJdbcDriverAvailable());
    }

    @Test
    void rejectsUnsupportedJdbcUrlScheme() {
        IllegalArgumentException error =
                assertThrows(IllegalArgumentException.class, () -> GreptimeQueryConfig.builder()
                        .jdbcUrl("jdbc:postgresql://alice:secret@127.0.0.1:4003/public?password1=secret")
                        .database("public")
                        .table("metrics")
                        .build());

        assertTrue(error.getMessage().contains("currently supports only MySQL JDBC"));
        assertFalse(error.getMessage().contains("secret"));
        assertFalse(error.getMessage().contains("alice"));
        assertTrue(error.getMessage().contains("jdbc:postgresql://****@127.0.0.1:4003/public?password1=****"));
        assertTrue(error.getMessage().contains("password1=****"));
    }

    @Test
    void rejectsDuplicateTimeoutOptionsInJdbcUrl() {
        IllegalArgumentException error =
                assertThrows(IllegalArgumentException.class, () -> GreptimeQueryConfig.builder()
                        .jdbcUrl("jdbc:mysql://127.0.0.1:4002/public?connectTimeout=1")
                        .database("public")
                        .table("metrics")
                        .build());

        assertEquals(
                "`query.jdbc-url` must not configure `connectTimeout`; use the typed query timeout options instead",
                error.getMessage());
    }

    @Test
    void rejectsUrlCredentials() {
        IllegalArgumentException error =
                assertThrows(IllegalArgumentException.class, () -> GreptimeQueryConfig.builder()
                        .jdbcUrl("jdbc:mysql://127.0.0.1:4002/public?user=url_user")
                        .database("public")
                        .table("metrics")
                        .build());

        assertEquals(
                "`query.jdbc-url` must not contain credentials or authentication tokens; configure `username` and `password` options instead",
                error.getMessage());
    }

    @Test
    void rejectsAuthorityCredentials() {
        IllegalArgumentException error =
                assertThrows(IllegalArgumentException.class, () -> GreptimeQueryConfig.builder()
                        .jdbcUrl("jdbc:mysql://alice:secret@127.0.0.1:4002/public")
                        .database("public")
                        .table("metrics")
                        .build());

        assertEquals(
                "`query.jdbc-url` must not contain credentials or authentication tokens; configure `username` and `password` options instead",
                error.getMessage());
    }

    @Test
    void decodesQueryKeysBeforeConflictDetection() {
        IllegalArgumentException error =
                assertThrows(IllegalArgumentException.class, () -> GreptimeQueryConfig.builder()
                        .jdbcUrl("jdbc:mysql://127.0.0.1:4002/public?pa%73sword=url_secret")
                        .database("public")
                        .table("metrics")
                        .build());

        assertEquals(
                "`query.jdbc-url` must not contain credentials or authentication tokens; configure `username` and `password` options instead",
                error.getMessage());
    }

    @Test
    void rejectsUrlSensitiveQueryKeys() {
        IllegalArgumentException error =
                assertThrows(IllegalArgumentException.class, () -> GreptimeQueryConfig.builder()
                        .jdbcUrl(
                                "jdbc:mysql://127.0.0.1:4002/public?password1=secret&trustCertificateKeyStorePassword=trust-secret&api_key=api-secret&xdevapiAuth=auth-secret")
                        .database("public")
                        .table("metrics")
                        .build());

        assertEquals(
                "`query.jdbc-url` must not contain credentials or authentication tokens; configure `username` and `password` options instead",
                error.getMessage());
    }

    @Test
    void redactsSensitiveJdbcUrlQueryParametersInDiagnostics() {
        String diagnostics = GreptimeQueryDialect.MYSQL.redactJdbcUrl(
                "jdbc:mysql://alice:secret@127.0.0.1:4002/public?user=bob&password1=password-secret&trustCertificateKeyStorePassword=trust-secret&api_key=api-secret&xdevapiAuth=auth-secret&useSSL=false");

        assertTrue(
                diagnostics.contains(
                        "jdbc:mysql://****@127.0.0.1:4002/public?user=****&password1=****&trustCertificateKeyStorePassword=****&api_key=****&xdevapiAuth=****&useSSL=false"));
        assertFalse(diagnostics.contains("alice"));
        assertFalse(diagnostics.contains("bob"));
        assertFalse(diagnostics.contains("secret"));
        assertFalse(diagnostics.contains("password-secret"));
        assertFalse(diagnostics.contains("trust-secret"));
        assertFalse(diagnostics.contains("api-secret"));
        assertFalse(diagnostics.contains("auth-secret"));
    }

    @Test
    void rejectsInvalidPercentEncodingInJdbcUrlQuery() {
        IllegalArgumentException error =
                assertThrows(IllegalArgumentException.class, () -> GreptimeQueryConfig.builder()
                        .jdbcUrl("jdbc:mysql://127.0.0.1:4002/public?useSSL=sec%zzret")
                        .database("public")
                        .table("metrics")
                        .build());

        assertEquals("Invalid percent-encoding in `query.jdbc-url`", error.getMessage());
    }

    @Test
    void validatesSourceQueryOptions() {
        assertEquals(
                "`query.jdbc-url` is required for GreptimeDB source",
                assertThrows(IllegalArgumentException.class, () -> GreptimeQueryConfig.builder()
                                .database("public")
                                .table("metrics")
                                .build())
                        .getMessage());

        assertEquals(
                "`username` and `password` must be configured together",
                assertThrows(IllegalArgumentException.class, () -> GreptimeQueryConfig.builder()
                                .jdbcUrl("jdbc:mysql://127.0.0.1:4002/public")
                                .database("public")
                                .table("metrics")
                                .credentials("greptime", null)
                                .build())
                        .getMessage());

        assertEquals(
                "`query.fetch-size` must be greater than or equal to 0",
                assertThrows(IllegalArgumentException.class, () -> GreptimeQueryConfig.builder()
                                .jdbcUrl("jdbc:mysql://127.0.0.1:4002/public")
                                .database("public")
                                .table("metrics")
                                .fetchSize(-1)
                                .build())
                        .getMessage());
    }

    @Test
    void missingDriverDiagnosticIncludesRedactedQueryContext() {
        GreptimeQueryConfig config = GreptimeQueryConfig.builder()
                .jdbcUrl("jdbc:mysql://127.0.0.1:4002/public?useSSL=false")
                .database("public")
                .table("metrics")
                .build();
        GreptimeQueryClient client = new GreptimeQueryClient(config);

        String message = client.queryException(
                        "execute-query", List.of("host"), 10L, new SQLException("No suitable driver found"))
                .getMessage();

        assertTrue(message.contains("protocol=mysql-jdbc"));
        assertTrue(message.contains("table=public.metrics"));
        assertTrue(message.contains("columns=[host]"));
        assertTrue(message.contains("limit=10"));
        assertTrue(message.contains("MySQL-compatible JDBC driver must be available on the Flink classpath"));
        assertFalse(message.contains("secret"));

        Throwable cause = client.queryException(
                        "execute-query",
                        List.of("host"),
                        10L,
                        new SQLException(
                                "No suitable driver found for jdbc:mysql://127.0.0.1:4002/public?password=secret"))
                .getCause();
        assertTrue(cause.getMessage().contains("jdbc:mysql://127.0.0.1:4002/public?useSSL=false"));
        assertFalse(cause.getMessage().contains("secret"));
    }

    @Test
    void queryExceptionUsesSanitizedGenericCause() {
        String jdbcUrl = "jdbc:mysql://127.0.0.1:4002/public?useSSL=false";
        GreptimeQueryConfig config = GreptimeQueryConfig.builder()
                .jdbcUrl(jdbcUrl)
                .database("public")
                .table("metrics")
                .build();
        GreptimeQueryClient client = new GreptimeQueryClient(config);

        IOException error = client.queryException(
                "execute-query",
                List.of("host"),
                null,
                new SQLException(
                        "Access denied for user 'alice' at " + jdbcUrl + " password query-secret", "28000", 1045));

        assertTrue(error.getMessage().contains("jdbc:mysql://127.0.0.1:4002/public?useSSL=false"));
        assertTrue(error.getCause().getMessage().contains("original driver message is hidden"));
        assertEquals("28000", ((SQLException) error.getCause()).getSQLState());
        assertEquals(1045, ((SQLException) error.getCause()).getErrorCode());
        assertFalse(error.getMessage().contains("alice"));
        assertFalse(error.getMessage().contains("secret"));
        assertFalse(error.getMessage().contains("query-secret"));
        assertFalse(error.getCause().getMessage().contains("alice"));
        assertFalse(error.getCause().getMessage().contains("secret"));
        assertFalse(error.getCause().getMessage().contains("query-secret"));
    }
}
