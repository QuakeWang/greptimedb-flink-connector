package io.greptime.flink.table;

import io.greptime.flink.cfg.GreptimeBulkWriteConfig;
import io.greptime.flink.cfg.GreptimeChangelogMode;
import io.greptime.flink.cfg.GreptimeConfigValidator;
import io.greptime.flink.cfg.GreptimeHintOptions;
import io.greptime.flink.cfg.GreptimeSinkConfig;
import io.greptime.flink.cfg.GreptimeWriteMode;
import io.greptime.flink.sink.schema.GreptimeSchemaValidator;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

final class GreptimeConnectorOptions {
    static final String IDENTIFIER = "greptimedb";

    static final ConfigOption<List<String>> ENDPOINTS =
            ConfigOptions.key("endpoints").stringType().asList().noDefaultValue();

    static final ConfigOption<String> DATABASE =
            ConfigOptions.key("database").stringType().defaultValue(GreptimeSinkConfig.DEFAULT_DATABASE);

    static final ConfigOption<String> TABLE =
            ConfigOptions.key("table").stringType().noDefaultValue();

    static final ConfigOption<String> USERNAME =
            ConfigOptions.key("username").stringType().noDefaultValue();

    static final ConfigOption<String> PASSWORD =
            ConfigOptions.key("password").stringType().noDefaultValue();

    static final ConfigOption<String> TIME_INDEX =
            ConfigOptions.key("time-index").stringType().noDefaultValue();

    static final ConfigOption<List<String>> TAGS =
            ConfigOptions.key("tags").stringType().asList().defaultValues();

    static final ConfigOption<Boolean> AUTO_CREATE_TABLE = ConfigOptions.key("auto-create-table")
            .booleanType()
            .defaultValue(GreptimeSinkConfig.DEFAULT_AUTO_CREATE_TABLE);

    static final ConfigOption<Boolean> APPEND_MODE =
            ConfigOptions.key("append-mode").booleanType().defaultValue(GreptimeSinkConfig.DEFAULT_APPEND_MODE);

    static final ConfigOption<String> MERGE_MODE =
            ConfigOptions.key("merge-mode").stringType().noDefaultValue();

    static final ConfigOption<String> TTL =
            ConfigOptions.key("ttl").stringType().noDefaultValue();

    static final ConfigOption<Integer> BATCH_MAX_ROWS =
            ConfigOptions.key("batch.max-rows").intType().defaultValue(GreptimeSinkConfig.DEFAULT_BATCH_MAX_ROWS);

    static final ConfigOption<Long> FLUSH_INTERVAL_MS = ConfigOptions.key("flush.interval-ms")
            .longType()
            .defaultValue(GreptimeSinkConfig.DEFAULT_FLUSH_INTERVAL_MS);

    static final ConfigOption<String> SINK_WRITE_MODE = ConfigOptions.key("sink.write-mode")
            .stringType()
            .defaultValue(GreptimeSinkConfig.DEFAULT_WRITE_MODE.optionValue());

    static final ConfigOption<String> SINK_CHANGELOG_MODE = ConfigOptions.key("sink.changelog-mode")
            .stringType()
            .defaultValue(GreptimeSinkConfig.DEFAULT_CHANGELOG_MODE.optionValue());

    static final ConfigOption<Integer> SINK_PARALLELISM =
            ConfigOptions.key("sink.parallelism").intType().noDefaultValue();

    static final ConfigOption<Integer> BULK_COLUMN_BUFFER_SIZE = ConfigOptions.key("bulk.column-buffer-size")
            .intType()
            .defaultValue(GreptimeBulkWriteConfig.DEFAULT_COLUMN_BUFFER_SIZE);

    static final ConfigOption<Long> BULK_TIMEOUT_MS_PER_MESSAGE = ConfigOptions.key("bulk.timeout-ms-per-message")
            .longType()
            .defaultValue(GreptimeBulkWriteConfig.DEFAULT_TIMEOUT_MS_PER_MESSAGE);

    static final ConfigOption<Integer> BULK_MAX_REQUESTS_IN_FLIGHT = ConfigOptions.key("bulk.max-requests-in-flight")
            .intType()
            .defaultValue(GreptimeBulkWriteConfig.DEFAULT_MAX_REQUESTS_IN_FLIGHT);

    static final ConfigOption<Long> BULK_ALLOCATOR_INIT_RESERVATION_BYTES = ConfigOptions.key(
                    "bulk.allocator-init-reservation-bytes")
            .longType()
            .defaultValue(GreptimeBulkWriteConfig.DEFAULT_ALLOCATOR_INIT_RESERVATION_BYTES);

    static final ConfigOption<Long> BULK_ALLOCATOR_MAX_ALLOCATION_BYTES = ConfigOptions.key(
                    "bulk.allocator-max-allocation-bytes")
            .longType()
            .defaultValue(GreptimeBulkWriteConfig.DEFAULT_ALLOCATOR_MAX_ALLOCATION_BYTES);

    static final ConfigOption<Integer> WRITE_MAX_RETRIES =
            ConfigOptions.key("write.max-retries").intType().defaultValue(GreptimeSinkConfig.DEFAULT_WRITE_MAX_RETRIES);

    static final ConfigOption<Integer> WRITE_MAX_IN_FLIGHT_POINTS = ConfigOptions.key("write.max-in-flight-points")
            .intType()
            .defaultValue(GreptimeSinkConfig.DEFAULT_WRITE_MAX_IN_FLIGHT_POINTS);

    static final ConfigOption<String> WRITE_LIMIT_POLICY = ConfigOptions.key("write.limit-policy")
            .stringType()
            .defaultValue(GreptimeSinkConfig.DEFAULT_WRITE_LIMIT_POLICY.optionValue());

    static final ConfigOption<Long> WRITE_LIMIT_TIMEOUT_MS = ConfigOptions.key("write.limit-timeout-ms")
            .longType()
            .defaultValue(GreptimeSinkConfig.DEFAULT_WRITE_LIMIT_TIMEOUT_MS);

    static final ConfigOption<String> WRITE_COMPRESSION = ConfigOptions.key("write.compression")
            .stringType()
            .defaultValue(GreptimeSinkConfig.DEFAULT_WRITE_COMPRESSION.optionValue());

    static final ConfigOption<Integer> RPC_TIMEOUT_MS =
            ConfigOptions.key("rpc.timeout-ms").intType().defaultValue(GreptimeSinkConfig.DEFAULT_RPC_TIMEOUT_MS);

    static final ConfigOption<Long> ROUTE_REFRESH_PERIOD_S = ConfigOptions.key("route.refresh-period-s")
            .longType()
            .defaultValue(GreptimeSinkConfig.DEFAULT_ROUTE_REFRESH_PERIOD_SECONDS);

    static final ConfigOption<Long> ROUTE_HEALTH_TIMEOUT_MS = ConfigOptions.key("route.health-timeout-ms")
            .longType()
            .defaultValue(GreptimeSinkConfig.DEFAULT_ROUTE_HEALTH_TIMEOUT_MS);

    private static final Set<String> WRITE_LIMIT_POLICIES = Arrays.stream(GreptimeSinkConfig.WriteLimitPolicy.values())
            .map(GreptimeSinkConfig.WriteLimitPolicy::optionValue)
            .collect(Collectors.toUnmodifiableSet());

    private static final Set<String> WRITE_COMPRESSIONS = Arrays.stream(GreptimeSinkConfig.WriteCompression.values())
            .map(GreptimeSinkConfig.WriteCompression::optionValue)
            .collect(Collectors.toUnmodifiableSet());

    private static final Set<String> WRITE_MODES = Arrays.stream(GreptimeWriteMode.values())
            .map(GreptimeWriteMode::optionValue)
            .collect(Collectors.toUnmodifiableSet());

    private static final Set<String> CHANGELOG_MODES = Arrays.stream(GreptimeChangelogMode.values())
            .map(GreptimeChangelogMode::optionValue)
            .collect(Collectors.toUnmodifiableSet());

    private static final List<ConfigOption<?>> REGULAR_ONLY_OPTIONS = List.of(
            WRITE_MAX_RETRIES,
            WRITE_MAX_IN_FLIGHT_POINTS,
            WRITE_LIMIT_POLICY,
            WRITE_LIMIT_TIMEOUT_MS,
            WRITE_COMPRESSION,
            RPC_TIMEOUT_MS);

    private GreptimeConnectorOptions() {}

    static void validate(ReadableConfig options, ResolvedSchema resolvedSchema) {
        GreptimeConfigValidator.validatePositive(formatOption(BATCH_MAX_ROWS), options.get(BATCH_MAX_ROWS));
        GreptimeConfigValidator.validateNonNegative(formatOption(FLUSH_INTERVAL_MS), options.get(FLUSH_INTERVAL_MS));
        GreptimeConfigValidator.validateSupportedValue(
                formatOption(SINK_WRITE_MODE), options.get(SINK_WRITE_MODE), WRITE_MODES);
        GreptimeConfigValidator.validateSupportedValue(
                formatOption(SINK_CHANGELOG_MODE), options.get(SINK_CHANGELOG_MODE), CHANGELOG_MODES);
        validateSinkParallelism(options);
        validateBulkWriteOptions(options);
        validateIdentifiers(options);
        String validatedMergeMode = validateHintOptions(options);
        GreptimeConfigValidator.validateAppendMergeConflict(
                "`append-mode=true`", options.get(APPEND_MODE), formatOption(MERGE_MODE), validatedMergeMode);
        validateChangelogModeCombinations(options);
        validateBulkWriteModeCombinations(options, validatedMergeMode);
        validateProductionWriteOptions(options);
        validateAuthPair(options);

        GreptimeChangelogMode changelogMode = GreptimeChangelogMode.fromOptionValue(options.get(SINK_CHANGELOG_MODE));
        DataType physicalRowDataType = resolvedSchema.toPhysicalRowDataType();
        RowType rowType = (RowType) physicalRowDataType.getLogicalType();
        GreptimeSchemaValidator.ValidationResult validation =
                GreptimeSchemaValidator.validate(rowType, options.get(TIME_INDEX), options.get(TAGS));

        if (options.get(AUTO_CREATE_TABLE) || changelogMode == GreptimeChangelogMode.RETRACT) {
            resolvedSchema
                    .getPrimaryKey()
                    .map(UniqueConstraint::getColumns)
                    .ifPresent(validation::validatePrimaryKeyMatchesTags);
        }
    }

    private static void validateAuthPair(ReadableConfig options) {
        GreptimeConfigValidator.validateCredentialsPair(
                formatOption(USERNAME),
                options.getOptional(USERNAME).orElse(null),
                formatOption(PASSWORD),
                options.getOptional(PASSWORD).orElse(null));
    }

    private static void validateIdentifiers(ReadableConfig options) {
        validateIdentifier(DATABASE.key(), options.get(DATABASE));
        options.getOptional(TABLE).ifPresent(table -> validateIdentifier(TABLE.key(), table));
    }

    private static void validateIdentifier(String optionName, String value) {
        GreptimeConfigValidator.validateRequiredText(String.format("`%s`", optionName), value);
    }

    private static void validateProductionWriteOptions(ReadableConfig options) {
        GreptimeConfigValidator.validateNonNegative(formatOption(WRITE_MAX_RETRIES), options.get(WRITE_MAX_RETRIES));
        GreptimeConfigValidator.validatePositive(
                formatOption(WRITE_MAX_IN_FLIGHT_POINTS), options.get(WRITE_MAX_IN_FLIGHT_POINTS));
        GreptimeConfigValidator.validateSupportedValue(
                formatOption(WRITE_LIMIT_POLICY), options.get(WRITE_LIMIT_POLICY), WRITE_LIMIT_POLICIES);
        GreptimeConfigValidator.validateNonNegative(
                formatOption(WRITE_LIMIT_TIMEOUT_MS), options.get(WRITE_LIMIT_TIMEOUT_MS));
        GreptimeConfigValidator.validateSupportedValue(
                formatOption(WRITE_COMPRESSION), options.get(WRITE_COMPRESSION), WRITE_COMPRESSIONS);
        GreptimeConfigValidator.validatePositive(formatOption(RPC_TIMEOUT_MS), options.get(RPC_TIMEOUT_MS));
        GreptimeConfigValidator.validateNonNegative(
                formatOption(ROUTE_REFRESH_PERIOD_S), options.get(ROUTE_REFRESH_PERIOD_S));
        GreptimeConfigValidator.validatePositive(
                formatOption(ROUTE_HEALTH_TIMEOUT_MS), options.get(ROUTE_HEALTH_TIMEOUT_MS));
    }

    private static String validateHintOptions(ReadableConfig options) {
        options.getOptional(TTL).ifPresent(ttl -> GreptimeHintOptions.validateTtl(formatOption(TTL), ttl));
        return options.getOptional(MERGE_MODE)
                .map(mergeMode -> GreptimeHintOptions.validateMergeMode(formatOption(MERGE_MODE), mergeMode))
                .orElse(null);
    }

    private static void validateBulkWriteOptions(ReadableConfig options) {
        GreptimeConfigValidator.validatePositive(
                formatOption(BULK_COLUMN_BUFFER_SIZE), options.get(BULK_COLUMN_BUFFER_SIZE));
        GreptimeConfigValidator.validatePositive(
                formatOption(BULK_TIMEOUT_MS_PER_MESSAGE), options.get(BULK_TIMEOUT_MS_PER_MESSAGE));
        GreptimeConfigValidator.validatePositive(
                formatOption(BULK_MAX_REQUESTS_IN_FLIGHT), options.get(BULK_MAX_REQUESTS_IN_FLIGHT));
        GreptimeConfigValidator.validateNonNegative(
                formatOption(BULK_ALLOCATOR_INIT_RESERVATION_BYTES),
                options.get(BULK_ALLOCATOR_INIT_RESERVATION_BYTES));
        GreptimeConfigValidator.validatePositive(
                formatOption(BULK_ALLOCATOR_MAX_ALLOCATION_BYTES), options.get(BULK_ALLOCATOR_MAX_ALLOCATION_BYTES));
        if (options.get(BULK_ALLOCATOR_MAX_ALLOCATION_BYTES) < options.get(BULK_ALLOCATOR_INIT_RESERVATION_BYTES)) {
            throw new IllegalArgumentException(formatOption(BULK_ALLOCATOR_MAX_ALLOCATION_BYTES)
                    + " must be greater than or equal to "
                    + formatOption(BULK_ALLOCATOR_INIT_RESERVATION_BYTES));
        }
    }

    private static void validateBulkWriteModeCombinations(ReadableConfig options, String validatedMergeMode) {
        GreptimeWriteMode writeMode = GreptimeWriteMode.fromOptionValue(options.get(SINK_WRITE_MODE));
        if (writeMode != GreptimeWriteMode.BULK) {
            return;
        }
        GreptimeConfigValidator.validateBulkWriteModeContract(
                writeMode,
                "`sink.write-mode=bulk`",
                "`auto-create-table=false`",
                options.get(AUTO_CREATE_TABLE),
                formatOption(TTL),
                options.getOptional(TTL).orElse(null),
                "`append-mode=true`",
                options.get(APPEND_MODE),
                formatOption(MERGE_MODE),
                validatedMergeMode);
        List<String> configuredRegularOnlyOptions = REGULAR_ONLY_OPTIONS.stream()
                .map(ConfigOption::key)
                .filter(options.toMap()::containsKey)
                .map(key -> String.format("`%s`", key))
                .collect(Collectors.toList());
        if (!configuredRegularOnlyOptions.isEmpty()) {
            throw new IllegalArgumentException("Regular Write options are not supported when `sink.write-mode=bulk`: "
                    + String.join(", ", configuredRegularOnlyOptions));
        }
    }

    private static void validateChangelogModeCombinations(ReadableConfig options) {
        GreptimeChangelogMode changelogMode = GreptimeChangelogMode.fromOptionValue(options.get(SINK_CHANGELOG_MODE));
        if (changelogMode != GreptimeChangelogMode.RETRACT) {
            return;
        }
        GreptimeWriteMode writeMode = GreptimeWriteMode.fromOptionValue(options.get(SINK_WRITE_MODE));
        if (writeMode == GreptimeWriteMode.BULK) {
            throw new IllegalArgumentException(
                    "`sink.changelog-mode=retract` is not supported when `sink.write-mode=bulk`");
        }
        if (options.get(APPEND_MODE)) {
            throw new IllegalArgumentException("`append-mode=true` cannot be used with `sink.changelog-mode=retract`");
        }
        options.getOptional(SINK_PARALLELISM).ifPresent(parallelism -> {
            if (parallelism != 1) {
                throw new IllegalArgumentException("`sink.parallelism` must be 1 when `sink.changelog-mode=retract`");
            }
        });
    }

    private static void validateSinkParallelism(ReadableConfig options) {
        options.getOptional(SINK_PARALLELISM)
                .ifPresent(parallelism ->
                        GreptimeConfigValidator.validatePositive(formatOption(SINK_PARALLELISM), parallelism));
    }

    private static String formatOption(ConfigOption<?> option) {
        return String.format("`%s`", option.key());
    }
}
