package io.greptime.flink.sink;

enum FlushReason {
    BATCH_FULL("batch_full"),
    PERIODIC("periodic"),
    OPERATION_CHANGED("operation_changed"),
    FLINK_FLUSH("flink_flush"),
    END_OF_INPUT("end_of_input"),
    CLOSE("close");

    private final String metricName;

    FlushReason(String metricName) {
        this.metricName = metricName;
    }

    String metricName() {
        return metricName;
    }
}
