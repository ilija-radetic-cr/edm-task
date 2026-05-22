package com.example.orderapi.exception;

public class PublishException extends RuntimeException {

    private final String correlationId;

    public PublishException(String message, String correlationId) {
        super(message);
        this.correlationId = correlationId;
    }

    public PublishException(String message, String correlationId, Throwable cause) {
        super(message, cause);
        this.correlationId = correlationId;
    }

    public String getCorrelationId() {
        return correlationId;
    }
}
