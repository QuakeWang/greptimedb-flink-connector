package io.greptime.flink.cfg;

import java.util.Collection;
import java.util.Locale;
import java.util.Objects;
import org.apache.flink.annotation.Internal;

@Internal
public final class GreptimeConfigValidator {
    private GreptimeConfigValidator() {}

    public static void validatePositive(String name, int value) {
        if (value <= 0) {
            throw new IllegalArgumentException(name + " must be greater than 0");
        }
    }

    public static void validatePositive(String name, long value) {
        if (value <= 0) {
            throw new IllegalArgumentException(name + " must be greater than 0");
        }
    }

    public static void validateNonNegative(String name, int value) {
        if (value < 0) {
            throw new IllegalArgumentException(name + " must be greater than or equal to 0");
        }
    }

    public static void validateNonNegative(String name, long value) {
        if (value < 0) {
            throw new IllegalArgumentException(name + " must be greater than or equal to 0");
        }
    }

    public static String validateRequiredText(String name, String value) {
        Objects.requireNonNull(value, name);
        String trimmed = value.trim();
        if (trimmed.isEmpty()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        if (!value.equals(trimmed)) {
            throw new IllegalArgumentException(name + " must not have leading or trailing whitespace");
        }
        return value;
    }

    public static void validateNoComma(String name, String value) {
        if (value.indexOf(',') >= 0) {
            throw new IllegalArgumentException(name + " must not contain ','");
        }
    }

    public static void validateCredentialsPair(
            String usernameName, String username, String passwordName, String password) {
        if ((normalizeBlank(username) == null) != (normalizeBlank(password) == null)) {
            throw new IllegalArgumentException(usernameName + " and " + passwordName + " must be configured together");
        }
    }

    public static void validateAppendMergeConflict(
            String appendModeTrueName, Boolean appendMode, String mergeModeName, String mergeMode) {
        if (Boolean.TRUE.equals(appendMode) && mergeMode != null) {
            throw new IllegalArgumentException(appendModeTrueName + " cannot be used with " + mergeModeName);
        }
    }

    public static void validateBulkWriteModeContract(
            GreptimeWriteMode writeMode,
            String bulkWriteModeName,
            String autoCreateTableFalseName,
            boolean autoCreateTable,
            String ttlName,
            String ttl,
            String appendModeTrueName,
            Boolean appendMode,
            String mergeModeName,
            String mergeMode) {
        if (writeMode != GreptimeWriteMode.BULK) {
            return;
        }
        if (autoCreateTable) {
            throw new IllegalArgumentException(bulkWriteModeName + " requires " + autoCreateTableFalseName
                    + " because Bulk Write does not create tables");
        }
        if (ttl != null) {
            throw new IllegalArgumentException(ttlName + " is not supported when " + bulkWriteModeName);
        }
        if (Boolean.TRUE.equals(appendMode)) {
            throw new IllegalArgumentException(appendModeTrueName + " is not supported when " + bulkWriteModeName);
        }
        if (mergeMode != null) {
            throw new IllegalArgumentException(mergeModeName + " is not supported when " + bulkWriteModeName);
        }
    }

    public static void validateSupportedValue(String name, String value, Collection<String> supportedValues) {
        String normalized = Objects.requireNonNull(value, name).toLowerCase(Locale.ROOT);
        if (!supportedValues.contains(normalized)) {
            throw new IllegalArgumentException(name + " must be one of " + supportedValues + ", but was: " + value);
        }
    }

    public static String normalizeBlank(String value) {
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        return trimmed.isEmpty() ? null : value;
    }
}
