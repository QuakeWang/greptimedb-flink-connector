package io.greptime.flink.cfg;

import java.io.Serializable;
import java.util.Locale;
import java.util.Objects;

public enum GreptimeWriteMode implements Serializable {
    REGULAR("regular"),
    BULK("bulk");

    private final String optionValue;

    GreptimeWriteMode(String optionValue) {
        this.optionValue = optionValue;
    }

    public String optionValue() {
        return optionValue;
    }

    public static GreptimeWriteMode fromOptionValue(String value) {
        String normalized = Objects.requireNonNull(value, "value").toLowerCase(Locale.ROOT);
        for (GreptimeWriteMode mode : values()) {
            if (mode.optionValue.equals(normalized)) {
                return mode;
            }
        }
        throw new IllegalArgumentException("Unsupported write mode: " + value);
    }
}
