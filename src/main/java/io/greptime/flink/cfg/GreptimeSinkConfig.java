package io.greptime.flink.cfg;

import io.greptime.options.GreptimeOptions;
import io.greptime.rpc.Compression;
import io.greptime.rpc.Context;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

public final class GreptimeSinkConfig implements Serializable {
    private static final long serialVersionUID = 1L;
    public static final String DEFAULT_DATABASE = "public";
    public static final int DEFAULT_BATCH_MAX_ROWS = 1000;
    public static final long DEFAULT_FLUSH_INTERVAL_MS = 0L;
    public static final boolean DEFAULT_AUTO_CREATE_TABLE = true;
    public static final boolean DEFAULT_APPEND_MODE = false;
    public static final int DEFAULT_WRITE_MAX_RETRIES = GreptimeOptions.DEFAULT_WRITE_MAX_RETRIES;
    public static final int DEFAULT_WRITE_MAX_IN_FLIGHT_POINTS = GreptimeOptions.DEFAULT_MAX_IN_FLIGHT_WRITE_POINTS;
    public static final WriteLimitPolicy DEFAULT_WRITE_LIMIT_POLICY = WriteLimitPolicy.ABORT_ON_BLOCKING_TIMEOUT;
    public static final long DEFAULT_WRITE_LIMIT_TIMEOUT_MS = 3000L;
    public static final WriteCompression DEFAULT_WRITE_COMPRESSION = WriteCompression.NONE;
    public static final int DEFAULT_RPC_TIMEOUT_MS = 60000;
    public static final long DEFAULT_ROUTE_REFRESH_PERIOD_SECONDS =
            GreptimeOptions.DEFAULT_ROUTE_TABLE_REFRESH_PERIOD_SECONDS;
    public static final long DEFAULT_ROUTE_HEALTH_TIMEOUT_MS = GreptimeOptions.DEFAULT_CHECK_HEALTH_TIMEOUT_MS;
    public static final GreptimeWriteMode DEFAULT_WRITE_MODE = GreptimeWriteMode.REGULAR;
    public static final GreptimeChangelogMode DEFAULT_CHANGELOG_MODE = GreptimeChangelogMode.INSERT_ONLY;

    private static final String AUTO_CREATE_TABLE_HINT = "auto_create_table";
    private static final String TTL_HINT = "ttl";
    private static final String APPEND_MODE_HINT = "append_mode";
    private static final String MERGE_MODE_HINT = "merge_mode";

    private final List<String> endpoints;
    private final String database;
    private final String username;
    private final String password;
    private final int batchMaxRows;
    private final long flushIntervalMs;
    private final boolean autoCreateTable;
    private final String ttl;
    private final Boolean appendMode;
    private final String mergeMode;
    private final int writeMaxRetries;
    private final int writeMaxInFlightPoints;
    private final WriteLimitPolicy writeLimitPolicy;
    private final long writeLimitTimeoutMs;
    private final WriteCompression writeCompression;
    private final int rpcTimeoutMs;
    private final long routeRefreshPeriodSeconds;
    private final long routeHealthTimeoutMs;
    private final GreptimeWriteMode writeMode;
    private final GreptimeChangelogMode changelogMode;
    private final GreptimeBulkWriteConfig bulkWriteConfig;

    private GreptimeSinkConfig(Builder builder) {
        String normalizedUsername = GreptimeConfigValidator.normalizeBlank(builder.username);
        String normalizedPassword = GreptimeConfigValidator.normalizeBlank(builder.password);
        String validatedTtl = GreptimeHintOptions.validateTtl("ttl", builder.ttl);
        String validatedMergeMode = GreptimeHintOptions.validateMergeMode("mergeMode", builder.mergeMode);
        GreptimeConfigValidator.validatePositive("batchMaxRows", builder.batchMaxRows);
        GreptimeConfigValidator.validateNonNegative("flushIntervalMs", builder.flushIntervalMs);
        GreptimeConfigValidator.validateNonNegative("writeMaxRetries", builder.writeMaxRetries);
        GreptimeConfigValidator.validatePositive("writeMaxInFlightPoints", builder.writeMaxInFlightPoints);
        GreptimeConfigValidator.validateNonNegative("writeLimitTimeoutMs", builder.writeLimitTimeoutMs);
        GreptimeConfigValidator.validatePositive("rpcTimeoutMs", builder.rpcTimeoutMs);
        GreptimeConfigValidator.validateNonNegative("routeRefreshPeriodSeconds", builder.routeRefreshPeriodSeconds);
        GreptimeConfigValidator.validatePositive("routeHealthTimeoutMs", builder.routeHealthTimeoutMs);
        GreptimeConfigValidator.validateCredentialsPair("username", builder.username, "password", builder.password);
        GreptimeConfigValidator.validateAppendMergeConflict(
                "appendMode=true", builder.appendMode, "mergeMode", validatedMergeMode);
        GreptimeWriteMode configuredWriteMode = Objects.requireNonNull(builder.writeMode, "writeMode");
        GreptimeChangelogMode configuredChangelogMode = Objects.requireNonNull(builder.changelogMode, "changelogMode");
        GreptimeConfigValidator.validateBulkWriteModeContract(
                configuredWriteMode,
                "sink.writeMode=bulk",
                "autoCreateTable=false",
                builder.autoCreateTable,
                "ttl",
                validatedTtl,
                "appendMode=true",
                builder.appendMode,
                "mergeMode",
                validatedMergeMode);
        validateChangelogModeContract(configuredWriteMode, configuredChangelogMode, builder.appendMode);

        List<String> configuredEndpoints = validateEndpoints(builder.endpoints);

        this.endpoints = Collections.unmodifiableList(configuredEndpoints);
        this.database = validateDatabase(builder.database);
        this.username = normalizedUsername;
        this.password = normalizedPassword;
        this.batchMaxRows = builder.batchMaxRows;
        this.flushIntervalMs = builder.flushIntervalMs;
        this.autoCreateTable = builder.autoCreateTable;
        this.ttl = validatedTtl;
        this.appendMode = builder.appendMode;
        this.mergeMode = validatedMergeMode;
        this.writeMaxRetries = builder.writeMaxRetries;
        this.writeMaxInFlightPoints = builder.writeMaxInFlightPoints;
        this.writeLimitPolicy = Objects.requireNonNull(builder.writeLimitPolicy, "writeLimitPolicy");
        this.writeLimitTimeoutMs = builder.writeLimitTimeoutMs;
        this.writeCompression = Objects.requireNonNull(builder.writeCompression, "writeCompression");
        this.rpcTimeoutMs = builder.rpcTimeoutMs;
        this.routeRefreshPeriodSeconds = builder.routeRefreshPeriodSeconds;
        this.routeHealthTimeoutMs = builder.routeHealthTimeoutMs;
        this.writeMode = configuredWriteMode;
        this.changelogMode = configuredChangelogMode;
        this.bulkWriteConfig = Objects.requireNonNull(builder.bulkWriteConfig, "bulkWriteConfig");
    }

    public static Builder builder() {
        return new Builder();
    }

    public List<String> getEndpoints() {
        return endpoints;
    }

    public String getDatabase() {
        return database;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public int getBatchMaxRows() {
        return batchMaxRows;
    }

    public long getFlushIntervalMs() {
        return flushIntervalMs;
    }

    public boolean isFlushIntervalEnabled() {
        return flushIntervalMs > 0;
    }

    public int getWriteMaxRetries() {
        return writeMaxRetries;
    }

    public int getWriteMaxInFlightPoints() {
        return writeMaxInFlightPoints;
    }

    public WriteLimitPolicy getWriteLimitPolicy() {
        return writeLimitPolicy;
    }

    public long getWriteLimitTimeoutMs() {
        return writeLimitTimeoutMs;
    }

    public WriteCompression getWriteCompression() {
        return writeCompression;
    }

    public int getRpcTimeoutMs() {
        return rpcTimeoutMs;
    }

    public long getRouteRefreshPeriodSeconds() {
        return routeRefreshPeriodSeconds;
    }

    public long getRouteHealthTimeoutMs() {
        return routeHealthTimeoutMs;
    }

    public GreptimeWriteMode getWriteMode() {
        return writeMode;
    }

    public GreptimeChangelogMode getChangelogMode() {
        return changelogMode;
    }

    public GreptimeBulkWriteConfig getBulkWriteConfig() {
        return bulkWriteConfig;
    }

    public Context createWriteContext() {
        Context context = Context.newDefault();
        context.withCompression(writeCompression.toSdkCompression());
        createWriteHints().forEach(context::withHint);

        return context;
    }

    public Context createBulkWriteContext() {
        return Context.newDefault();
    }

    public String describeWriteHints() {
        StringBuilder summary = new StringBuilder();
        for (Map.Entry<String, String> hint : createWriteHints().entrySet()) {
            if (summary.length() > 0) {
                summary.append(',');
            }
            summary.append(hint.getKey()).append('=').append(hint.getValue());
        }
        return summary.toString();
    }

    public String describeWriteSettings() {
        return "writeMaxRetries="
                + writeMaxRetries
                + ",writeMaxInFlightPoints="
                + writeMaxInFlightPoints
                + ",writeLimitPolicy="
                + writeLimitPolicy.optionValue()
                + ",writeLimitTimeoutMs="
                + writeLimitTimeoutMs
                + ",writeCompression="
                + writeCompression.optionValue()
                + ",rpcTimeoutMs="
                + rpcTimeoutMs
                + ",routeRefreshPeriodSeconds="
                + routeRefreshPeriodSeconds
                + ",routeHealthTimeoutMs="
                + routeHealthTimeoutMs;
    }

    public String describeWriteModeSettings() {
        return "writeMode=" + writeMode.optionValue() + ",changelogMode=" + changelogMode.optionValue() + ","
                + bulkWriteConfig.describe();
    }

    private Map<String, String> createWriteHints() {
        Map<String, String> hints = new LinkedHashMap<>();
        hints.put(AUTO_CREATE_TABLE_HINT, Boolean.toString(autoCreateTable));
        if (ttl != null) {
            hints.put(TTL_HINT, ttl);
        }
        if (appendMode != null) {
            hints.put(APPEND_MODE_HINT, appendMode.toString());
        }
        if (mergeMode != null) {
            hints.put(MERGE_MODE_HINT, mergeMode);
        }
        return hints;
    }

    private static List<String> validateEndpoints(List<String> endpoints) {
        Objects.requireNonNull(endpoints, "endpoints");
        if (endpoints.isEmpty()) {
            throw new IllegalArgumentException("endpoints must not be empty");
        }

        List<String> validatedEndpoints = new ArrayList<>(endpoints.size());
        for (String endpoint : endpoints) {
            validatedEndpoints.add(validateEndpoint(endpoint));
        }
        return validatedEndpoints;
    }

    private static String validateEndpoint(String endpoint) {
        if (endpoint == null) {
            throw new IllegalArgumentException("endpoint must not be null");
        }

        String trimmed = endpoint.trim();
        int separator = trimmed.indexOf(':');
        if (separator <= 0 || separator != trimmed.lastIndexOf(':') || separator == trimmed.length() - 1) {
            throw invalidEndpoint(trimmed);
        }

        String host = trimmed.substring(0, separator).trim();
        String portText = trimmed.substring(separator + 1).trim();
        if (host.isEmpty() || portText.isEmpty()) {
            throw invalidEndpoint(trimmed);
        }

        int port;
        try {
            port = Integer.parseInt(portText);
        } catch (NumberFormatException e) {
            throw invalidEndpoint(trimmed);
        }
        if (port <= 0 || port > 65535) {
            throw invalidEndpoint(trimmed);
        }

        return host + ":" + port;
    }

    private static IllegalArgumentException invalidEndpoint(String endpoint) {
        return new IllegalArgumentException("Invalid endpoint, expected host:port: " + endpoint);
    }

    private static String validateDatabase(String database) {
        return GreptimeConfigValidator.validateRequiredText("database", database);
    }

    private static void validateChangelogModeContract(
            GreptimeWriteMode writeMode, GreptimeChangelogMode changelogMode, Boolean appendMode) {
        if (changelogMode != GreptimeChangelogMode.RETRACT) {
            return;
        }
        if (writeMode == GreptimeWriteMode.BULK) {
            throw new IllegalArgumentException("sink.changelogMode=retract is not supported when sink.writeMode=bulk");
        }
        if (Boolean.TRUE.equals(appendMode)) {
            throw new IllegalArgumentException("appendMode=true cannot be used with sink.changelogMode=retract");
        }
    }

    public enum WriteLimitPolicy {
        ABORT("abort"),
        BLOCKING("blocking"),
        BLOCKING_TIMEOUT("blocking-timeout"),
        ABORT_ON_BLOCKING_TIMEOUT("abort-on-blocking-timeout");

        private final String optionValue;

        WriteLimitPolicy(String optionValue) {
            this.optionValue = optionValue;
        }

        public String optionValue() {
            return optionValue;
        }

        public static WriteLimitPolicy fromOptionValue(String value) {
            String normalized = normalize(value);
            for (WriteLimitPolicy policy : values()) {
                if (policy.optionValue.equals(normalized)) {
                    return policy;
                }
            }
            throw new IllegalArgumentException("Unsupported write limit policy: " + value);
        }
    }

    public enum WriteCompression {
        NONE("none", Compression.None),
        GZIP("gzip", Compression.Gzip),
        ZSTD("zstd", Compression.Zstd);

        private final String optionValue;
        private final Compression sdkCompression;

        WriteCompression(String optionValue, Compression sdkCompression) {
            this.optionValue = optionValue;
            this.sdkCompression = sdkCompression;
        }

        public String optionValue() {
            return optionValue;
        }

        public Compression toSdkCompression() {
            return sdkCompression;
        }

        public static WriteCompression fromOptionValue(String value) {
            String normalized = normalize(value);
            for (WriteCompression compression : values()) {
                if (compression.optionValue.equals(normalized)) {
                    return compression;
                }
            }
            throw new IllegalArgumentException("Unsupported write compression: " + value);
        }
    }

    private static String normalize(String value) {
        return Objects.requireNonNull(value, "value").toLowerCase(Locale.ROOT);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof GreptimeSinkConfig)) {
            return false;
        }
        GreptimeSinkConfig that = (GreptimeSinkConfig) other;
        return batchMaxRows == that.batchMaxRows
                && flushIntervalMs == that.flushIntervalMs
                && autoCreateTable == that.autoCreateTable
                && Objects.equals(endpoints, that.endpoints)
                && Objects.equals(database, that.database)
                && Objects.equals(username, that.username)
                && Objects.equals(password, that.password)
                && Objects.equals(ttl, that.ttl)
                && Objects.equals(appendMode, that.appendMode)
                && Objects.equals(mergeMode, that.mergeMode)
                && writeMaxRetries == that.writeMaxRetries
                && writeMaxInFlightPoints == that.writeMaxInFlightPoints
                && writeLimitPolicy == that.writeLimitPolicy
                && writeLimitTimeoutMs == that.writeLimitTimeoutMs
                && writeCompression == that.writeCompression
                && rpcTimeoutMs == that.rpcTimeoutMs
                && routeRefreshPeriodSeconds == that.routeRefreshPeriodSeconds
                && routeHealthTimeoutMs == that.routeHealthTimeoutMs
                && writeMode == that.writeMode
                && changelogMode == that.changelogMode
                && Objects.equals(bulkWriteConfig, that.bulkWriteConfig);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                endpoints,
                database,
                username,
                password,
                batchMaxRows,
                flushIntervalMs,
                autoCreateTable,
                ttl,
                appendMode,
                mergeMode,
                writeMaxRetries,
                writeMaxInFlightPoints,
                writeLimitPolicy,
                writeLimitTimeoutMs,
                writeCompression,
                rpcTimeoutMs,
                routeRefreshPeriodSeconds,
                routeHealthTimeoutMs,
                writeMode,
                changelogMode,
                bulkWriteConfig);
    }

    public static final class Builder {
        private List<String> endpoints;
        private String database = DEFAULT_DATABASE;
        private String username;
        private String password;
        private int batchMaxRows = DEFAULT_BATCH_MAX_ROWS;
        private long flushIntervalMs = DEFAULT_FLUSH_INTERVAL_MS;
        private boolean autoCreateTable = DEFAULT_AUTO_CREATE_TABLE;
        private String ttl;
        private Boolean appendMode;
        private String mergeMode;
        private int writeMaxRetries = DEFAULT_WRITE_MAX_RETRIES;
        private int writeMaxInFlightPoints = DEFAULT_WRITE_MAX_IN_FLIGHT_POINTS;
        private WriteLimitPolicy writeLimitPolicy = DEFAULT_WRITE_LIMIT_POLICY;
        private long writeLimitTimeoutMs = DEFAULT_WRITE_LIMIT_TIMEOUT_MS;
        private WriteCompression writeCompression = DEFAULT_WRITE_COMPRESSION;
        private int rpcTimeoutMs = DEFAULT_RPC_TIMEOUT_MS;
        private long routeRefreshPeriodSeconds = DEFAULT_ROUTE_REFRESH_PERIOD_SECONDS;
        private long routeHealthTimeoutMs = DEFAULT_ROUTE_HEALTH_TIMEOUT_MS;
        private GreptimeWriteMode writeMode = DEFAULT_WRITE_MODE;
        private GreptimeChangelogMode changelogMode = DEFAULT_CHANGELOG_MODE;
        private GreptimeBulkWriteConfig bulkWriteConfig = GreptimeBulkWriteConfig.defaults();

        private Builder() {}

        public Builder endpoints(List<String> endpoints) {
            this.endpoints = endpoints;
            return this;
        }

        public Builder database(String database) {
            this.database = database;
            return this;
        }

        public Builder credentials(String username, String password) {
            this.username = username;
            this.password = password;
            return this;
        }

        public Builder batchMaxRows(int batchMaxRows) {
            this.batchMaxRows = batchMaxRows;
            return this;
        }

        public Builder flushIntervalMs(long flushIntervalMs) {
            this.flushIntervalMs = flushIntervalMs;
            return this;
        }

        public Builder autoCreateTable(boolean autoCreateTable) {
            this.autoCreateTable = autoCreateTable;
            return this;
        }

        public Builder ttl(String ttl) {
            this.ttl = ttl;
            return this;
        }

        public Builder appendMode(Boolean appendMode) {
            this.appendMode = appendMode;
            return this;
        }

        public Builder mergeMode(String mergeMode) {
            this.mergeMode = mergeMode;
            return this;
        }

        public Builder writeMaxRetries(int writeMaxRetries) {
            this.writeMaxRetries = writeMaxRetries;
            return this;
        }

        public Builder writeMaxInFlightPoints(int writeMaxInFlightPoints) {
            this.writeMaxInFlightPoints = writeMaxInFlightPoints;
            return this;
        }

        public Builder writeLimitPolicy(WriteLimitPolicy writeLimitPolicy) {
            this.writeLimitPolicy = writeLimitPolicy;
            return this;
        }

        public Builder writeLimitTimeoutMs(long writeLimitTimeoutMs) {
            this.writeLimitTimeoutMs = writeLimitTimeoutMs;
            return this;
        }

        public Builder writeCompression(WriteCompression writeCompression) {
            this.writeCompression = writeCompression;
            return this;
        }

        public Builder rpcTimeoutMs(int rpcTimeoutMs) {
            this.rpcTimeoutMs = rpcTimeoutMs;
            return this;
        }

        public Builder routeRefreshPeriodSeconds(long routeRefreshPeriodSeconds) {
            this.routeRefreshPeriodSeconds = routeRefreshPeriodSeconds;
            return this;
        }

        public Builder routeHealthTimeoutMs(long routeHealthTimeoutMs) {
            this.routeHealthTimeoutMs = routeHealthTimeoutMs;
            return this;
        }

        public Builder writeMode(GreptimeWriteMode writeMode) {
            this.writeMode = writeMode;
            return this;
        }

        public Builder changelogMode(GreptimeChangelogMode changelogMode) {
            this.changelogMode = changelogMode;
            return this;
        }

        public Builder bulkWriteConfig(GreptimeBulkWriteConfig bulkWriteConfig) {
            this.bulkWriteConfig = bulkWriteConfig;
            return this;
        }

        public GreptimeSinkConfig build() {
            return new GreptimeSinkConfig(this);
        }
    }
}
