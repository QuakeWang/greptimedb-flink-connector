package io.greptime.flink.cfg;

import java.util.List;
import java.util.Locale;

/** Shared validation kernel for Greptime write hints. */
public final class GreptimeHintOptions {
    private static final String MERGE_MODE_LAST_ROW = "last_row";
    private static final String MERGE_MODE_LAST_NON_NULL = "last_non_null";
    private static final List<String> SUPPORTED_MERGE_MODES = List.of(MERGE_MODE_LAST_ROW, MERGE_MODE_LAST_NON_NULL);

    private GreptimeHintOptions() {}

    public static String validateTtl(String optionName, String ttl) {
        return validateHintValue(optionName, ttl);
    }

    public static String validateMergeMode(String optionName, String mergeMode) {
        String validated = validateHintValue(optionName, mergeMode);
        if (validated == null) {
            return null;
        }
        String normalized = validated.toLowerCase(Locale.ROOT);
        if (!SUPPORTED_MERGE_MODES.contains(normalized)) {
            throw new IllegalArgumentException(
                    optionName + " must be one of " + SUPPORTED_MERGE_MODES + ", but was: " + mergeMode);
        }
        return normalized;
    }

    private static String validateHintValue(String optionName, String value) {
        if (value == null) {
            return null;
        }
        GreptimeConfigValidator.validateRequiredText(optionName, value);
        GreptimeConfigValidator.validateNoComma(optionName, value);
        return value;
    }
}
