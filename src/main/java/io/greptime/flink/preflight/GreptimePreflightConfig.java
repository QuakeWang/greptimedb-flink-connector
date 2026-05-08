package io.greptime.flink.preflight;

import io.greptime.flink.query.GreptimeQueryConfig;
import java.io.Serializable;
import java.util.Objects;
import java.util.Optional;

public final class GreptimePreflightConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    private static final GreptimePreflightConfig DISABLED = new GreptimePreflightConfig(false, null);

    private final boolean enabled;
    private final GreptimeQueryConfig queryConfig;

    private GreptimePreflightConfig(boolean enabled, GreptimeQueryConfig queryConfig) {
        this.enabled = enabled;
        this.queryConfig = queryConfig;
    }

    public static GreptimePreflightConfig disabled() {
        return DISABLED;
    }

    public static GreptimePreflightConfig enabled(GreptimeQueryConfig queryConfig) {
        return new GreptimePreflightConfig(true, Objects.requireNonNull(queryConfig, "queryConfig"));
    }

    public boolean isEnabled() {
        return enabled;
    }

    public Optional<GreptimeQueryConfig> getQueryConfig() {
        return Optional.ofNullable(queryConfig);
    }

    public String describe() {
        if (!enabled) {
            return "preflight=disabled";
        }
        return "preflight=enabled, " + queryConfig.describeMetadataConnectionContext();
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof GreptimePreflightConfig)) {
            return false;
        }
        GreptimePreflightConfig that = (GreptimePreflightConfig) other;
        return enabled == that.enabled && Objects.equals(queryConfig, that.queryConfig);
    }

    @Override
    public int hashCode() {
        return Objects.hash(enabled, queryConfig);
    }
}
