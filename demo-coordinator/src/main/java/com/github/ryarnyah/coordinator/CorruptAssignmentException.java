package com.github.ryarnyah.coordinator;

import org.apache.kafka.common.errors.RetriableException;

public class CorruptAssignmentException extends RetriableException {
    private static final long serialVersionUID = 1L;

    public CorruptAssignmentException() {
        super("This message has failed its CRC checksum, exceeds the valid size, or is otherwise corrupt.");
    }

    public CorruptAssignmentException(String message) {
        super(message);
    }

    public CorruptAssignmentException(Throwable cause) {
        super(cause);
    }

    public CorruptAssignmentException(String message, Throwable cause) {
        super(message, cause);
    }
}