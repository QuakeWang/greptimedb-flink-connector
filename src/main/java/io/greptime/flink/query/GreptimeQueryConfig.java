package io.greptime.flink.query;

import io.greptime.flink.cfg.GreptimeConfigValidator;
import java.io.Serializable;
import java.util.Locale;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;

public final class GreptimeQueryConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    public static final int DEFAULT_CONNECT_TIMEOUT_MS = 10_000;
    public static final int DEFAULT_SOCKET_TIMEOUT_MS = 300_000;
    public static final int DEFAULT_FETCH_SIZE = 0;

    private final String jdbcUrl;
    private final GreptimeQueryDialect dialect;
    private final String database;
    private final String table;
    private final String username;
    private final String password;
    private final int connectTimeoutMs;
    private final int socketTimeoutMs;
    private final int fetchSize;

    private GreptimeQueryConfig(Builder builder) {
        String validatedJdbcUrl = validateRequiredOption("`query.jdbc-url`", builder.jdbcUrl);
        GreptimeQueryDialect resolvedDialect = GreptimeQueryDialect.fromJdbcUrl(validatedJdbcUrl);
        String validatedDatabase = validateRequiredOption("`database`", builder.database);
        String validatedTable = validateRequiredOption("`table`", builder.table);
        String normalizedUsername = GreptimeConfigValidator.normalizeBlank(builder.username);
        String normalizedPassword = GreptimeConfigValidator.normalizeBlank(builder.password);

        GreptimeConfigValidator.validateCredentialsPair(
                "`username`", normalizedUsername, "`password`", normalizedPassword);
        GreptimeConfigValidator.validatePositive("`query.connect-timeout-ms`", builder.connectTimeoutMs);
        GreptimeConfigValidator.validatePositive("`query.socket-timeout-ms`", builder.socketTimeoutMs);
        GreptimeConfigValidator.validateNonNegative("`query.fetch-size`", builder.fetchSize);
        validateJdbcUrlConflicts(resolvedDialect, validatedJdbcUrl);

        this.jdbcUrl = validatedJdbcUrl;
        this.dialect = resolvedDialect;
        this.database = validatedDatabase;
        this.table = validatedTable;
        this.username = normalizedUsername;
        this.password = normalizedPassword;
        this.connectTimeoutMs = builder.connectTimeoutMs;
        this.socketTimeoutMs = builder.socketTimeoutMs;
        this.fetchSize = builder.fetchSize;
    }

    public static Builder builder() {
        return new Builder();
    }

    public String getJdbcUrl() {
        return jdbcUrl;
    }

    public GreptimeQueryDialect getDialect() {
        return dialect;
    }

    public String getDatabase() {
        return database;
    }

    public String getTable() {
        return table;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public int getConnectTimeoutMs() {
        return connectTimeoutMs;
    }

    public int getSocketTimeoutMs() {
        return socketTimeoutMs;
    }

    public int getFetchSize() {
        return fetchSize;
    }

    public Properties createJdbcProperties() {
        Properties properties = new Properties();
        properties.setProperty(dialect.connectTimeoutProperty(), Integer.toString(connectTimeoutMs));
        properties.setProperty(dialect.socketTimeoutProperty(), Integer.toString(socketTimeoutMs));
        if (username != null) {
            properties.setProperty("user", username);
            properties.setProperty("password", password);
        }
        return properties;
    }

    public String describeConnectionContext() {
        return "protocol="
                + dialect.protocolName()
                + ", table="
                + database
                + "."
                + table
                + ", jdbcUrl="
                + dialect.redactJdbcUrl(jdbcUrl)
                + ", connectTimeoutMs="
                + connectTimeoutMs
                + ", socketTimeoutMs="
                + socketTimeoutMs
                + ", fetchSize="
                + fetchSize;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof GreptimeQueryConfig)) {
            return false;
        }
        GreptimeQueryConfig that = (GreptimeQueryConfig) other;
        return connectTimeoutMs == that.connectTimeoutMs
                && socketTimeoutMs == that.socketTimeoutMs
                && fetchSize == that.fetchSize
                && Objects.equals(jdbcUrl, that.jdbcUrl)
                && dialect == that.dialect
                && Objects.equals(database, that.database)
                && Objects.equals(table, that.table)
                && Objects.equals(username, that.username)
                && Objects.equals(password, that.password);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                jdbcUrl, dialect, database, table, username, password, connectTimeoutMs, socketTimeoutMs, fetchSize);
    }

    private static void validateJdbcUrlConflicts(GreptimeQueryDialect dialect, String jdbcUrl) {
        if (dialect.hasSensitiveMaterial(jdbcUrl)) {
            throw new IllegalArgumentException(
                    "`query.jdbc-url` must not contain credentials or authentication tokens; configure `username` and `password` options instead");
        }

        Set<String> queryKeys = dialect.queryParameterKeys(jdbcUrl);

        for (String timeoutKey : dialect.timeoutQueryKeys()) {
            if (queryKeys.contains(timeoutKey.toLowerCase(Locale.ROOT))) {
                throw new IllegalArgumentException("`query.jdbc-url` must not configure `" + timeoutKey
                        + "`; use the typed query timeout options instead");
            }
        }
    }

    private static String validateRequiredOption(String name, String value) {
        if (value == null) {
            throw new IllegalArgumentException(name + " is required for GreptimeDB source");
        }
        return GreptimeConfigValidator.validateRequiredText(name, value);
    }

    public static final class Builder {
        private String jdbcUrl;
        private String database;
        private String table;
        private String username;
        private String password;
        private int connectTimeoutMs = DEFAULT_CONNECT_TIMEOUT_MS;
        private int socketTimeoutMs = DEFAULT_SOCKET_TIMEOUT_MS;
        private int fetchSize = DEFAULT_FETCH_SIZE;

        private Builder() {}

        public Builder jdbcUrl(String jdbcUrl) {
            this.jdbcUrl = jdbcUrl;
            return this;
        }

        public Builder database(String database) {
            this.database = database;
            return this;
        }

        public Builder table(String table) {
            this.table = table;
            return this;
        }

        public Builder credentials(String username, String password) {
            this.username = username;
            this.password = password;
            return this;
        }

        public Builder connectTimeoutMs(int connectTimeoutMs) {
            this.connectTimeoutMs = connectTimeoutMs;
            return this;
        }

        public Builder socketTimeoutMs(int socketTimeoutMs) {
            this.socketTimeoutMs = socketTimeoutMs;
            return this;
        }

        public Builder fetchSize(int fetchSize) {
            this.fetchSize = fetchSize;
            return this;
        }

        public GreptimeQueryConfig build() {
            return new GreptimeQueryConfig(this);
        }
    }
}
