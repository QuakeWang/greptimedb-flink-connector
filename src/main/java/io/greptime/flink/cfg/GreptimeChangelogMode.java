package io.greptime.flink.cfg;

import java.io.Serializable;
import java.util.Locale;
import java.util.Objects;

public enum GreptimeChangelogMode implements Serializable {
    INSERT_ONLY("insert-only"),
    RETRACT("retract");

    private final String optionValue;

    GreptimeChangelogMode(String optionValue) {
        this.optionValue = optionValue;
    }

    public String optionValue() {
        return optionValue;
    }

    public static GreptimeChangelogMode fromOptionValue(String value) {
        String normalized = Objects.requireNonNull(value, "value").toLowerCase(Locale.ROOT);
        for (GreptimeChangelogMode mode : values()) {
            if (mode.optionValue.equals(normalized)) {
                return mode;
            }
        }
        throw new IllegalArgumentException("Unsupported changelog mode: " + value);
    }
}
