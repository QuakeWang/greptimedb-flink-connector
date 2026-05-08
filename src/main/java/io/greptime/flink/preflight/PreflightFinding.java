package io.greptime.flink.preflight;

import java.io.Serializable;
import java.util.Objects;

public final class PreflightFinding implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String category;
    private final String subject;
    private final String local;
    private final String remote;
    private final String detail;

    private PreflightFinding(String category, String subject, String local, String remote, String detail) {
        this.category = Objects.requireNonNull(category, "category");
        this.subject = Objects.requireNonNull(subject, "subject");
        this.local = local;
        this.remote = remote;
        this.detail = Objects.requireNonNull(detail, "detail");
    }

    public static PreflightFinding of(String category, String subject, String local, String remote, String detail) {
        return new PreflightFinding(category, subject, local, remote, detail);
    }

    public String getCategory() {
        return category;
    }

    public String format() {
        StringBuilder builder = new StringBuilder();
        builder.append("category=").append(category).append(", subject=").append(subject);
        if (local != null) {
            builder.append(", local=").append(local);
        }
        if (remote != null) {
            builder.append(", remote=").append(remote);
        }
        builder.append(", detail=").append(detail);
        return builder.toString();
    }
}
