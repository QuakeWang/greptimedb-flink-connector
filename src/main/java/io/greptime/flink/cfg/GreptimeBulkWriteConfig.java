package io.greptime.flink.cfg;

import io.greptime.BulkWrite;
import java.io.Serializable;
import java.util.Objects;

public final class GreptimeBulkWriteConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    public static final int DEFAULT_COLUMN_BUFFER_SIZE = 1024;
    public static final long DEFAULT_TIMEOUT_MS_PER_MESSAGE = 60000L;
    public static final int DEFAULT_MAX_REQUESTS_IN_FLIGHT = 8;
    public static final long DEFAULT_ALLOCATOR_INIT_RESERVATION_BYTES = 0L;
    public static final long DEFAULT_ALLOCATOR_MAX_ALLOCATION_BYTES = 1073741824L;

    private final int columnBufferSize;
    private final long timeoutMsPerMessage;
    private final int maxRequestsInFlight;
    private final long allocatorInitReservationBytes;
    private final long allocatorMaxAllocationBytes;

    private GreptimeBulkWriteConfig(Builder builder) {
        GreptimeConfigValidator.validatePositive("bulkColumnBufferSize", builder.columnBufferSize);
        GreptimeConfigValidator.validatePositive("bulkTimeoutMsPerMessage", builder.timeoutMsPerMessage);
        GreptimeConfigValidator.validatePositive("bulkMaxRequestsInFlight", builder.maxRequestsInFlight);
        GreptimeConfigValidator.validateNonNegative(
                "bulkAllocatorInitReservationBytes", builder.allocatorInitReservationBytes);
        GreptimeConfigValidator.validatePositive(
                "bulkAllocatorMaxAllocationBytes", builder.allocatorMaxAllocationBytes);
        if (builder.allocatorMaxAllocationBytes < builder.allocatorInitReservationBytes) {
            throw new IllegalArgumentException(
                    "bulkAllocatorMaxAllocationBytes must be greater than or equal to bulkAllocatorInitReservationBytes");
        }

        this.columnBufferSize = builder.columnBufferSize;
        this.timeoutMsPerMessage = builder.timeoutMsPerMessage;
        this.maxRequestsInFlight = builder.maxRequestsInFlight;
        this.allocatorInitReservationBytes = builder.allocatorInitReservationBytes;
        this.allocatorMaxAllocationBytes = builder.allocatorMaxAllocationBytes;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static GreptimeBulkWriteConfig defaults() {
        return builder().build();
    }

    public int getColumnBufferSize() {
        return columnBufferSize;
    }

    public long getTimeoutMsPerMessage() {
        return timeoutMsPerMessage;
    }

    public int getMaxRequestsInFlight() {
        return maxRequestsInFlight;
    }

    public long getAllocatorInitReservationBytes() {
        return allocatorInitReservationBytes;
    }

    public long getAllocatorMaxAllocationBytes() {
        return allocatorMaxAllocationBytes;
    }

    public BulkWrite.Config toSdkConfig() {
        return BulkWrite.Config.newBuilder()
                .allocatorInitReservation(allocatorInitReservationBytes)
                .allocatorMaxAllocation(allocatorMaxAllocationBytes)
                .timeoutMsPerMessage(timeoutMsPerMessage)
                .maxRequestsInFlight(maxRequestsInFlight)
                .build();
    }

    public String describe() {
        return "bulkColumnBufferSize="
                + columnBufferSize
                + ",bulkTimeoutMsPerMessage="
                + timeoutMsPerMessage
                + ",bulkMaxRequestsInFlight="
                + maxRequestsInFlight
                + ",bulkAllocatorInitReservationBytes="
                + allocatorInitReservationBytes
                + ",bulkAllocatorMaxAllocationBytes="
                + allocatorMaxAllocationBytes;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof GreptimeBulkWriteConfig)) {
            return false;
        }
        GreptimeBulkWriteConfig that = (GreptimeBulkWriteConfig) other;
        return columnBufferSize == that.columnBufferSize
                && timeoutMsPerMessage == that.timeoutMsPerMessage
                && maxRequestsInFlight == that.maxRequestsInFlight
                && allocatorInitReservationBytes == that.allocatorInitReservationBytes
                && allocatorMaxAllocationBytes == that.allocatorMaxAllocationBytes;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                columnBufferSize,
                timeoutMsPerMessage,
                maxRequestsInFlight,
                allocatorInitReservationBytes,
                allocatorMaxAllocationBytes);
    }

    public static final class Builder {
        private int columnBufferSize = DEFAULT_COLUMN_BUFFER_SIZE;
        private long timeoutMsPerMessage = DEFAULT_TIMEOUT_MS_PER_MESSAGE;
        private int maxRequestsInFlight = DEFAULT_MAX_REQUESTS_IN_FLIGHT;
        private long allocatorInitReservationBytes = DEFAULT_ALLOCATOR_INIT_RESERVATION_BYTES;
        private long allocatorMaxAllocationBytes = DEFAULT_ALLOCATOR_MAX_ALLOCATION_BYTES;

        private Builder() {}

        public Builder columnBufferSize(int columnBufferSize) {
            this.columnBufferSize = columnBufferSize;
            return this;
        }

        public Builder timeoutMsPerMessage(long timeoutMsPerMessage) {
            this.timeoutMsPerMessage = timeoutMsPerMessage;
            return this;
        }

        public Builder maxRequestsInFlight(int maxRequestsInFlight) {
            this.maxRequestsInFlight = maxRequestsInFlight;
            return this;
        }

        public Builder allocatorInitReservationBytes(long allocatorInitReservationBytes) {
            this.allocatorInitReservationBytes = allocatorInitReservationBytes;
            return this;
        }

        public Builder allocatorMaxAllocationBytes(long allocatorMaxAllocationBytes) {
            this.allocatorMaxAllocationBytes = allocatorMaxAllocationBytes;
            return this;
        }

        public GreptimeBulkWriteConfig build() {
            return new GreptimeBulkWriteConfig(this);
        }
    }
}
