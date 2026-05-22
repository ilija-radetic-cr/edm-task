package com.example.orderapi.dto;

public class ErrorResponse {

    private String error;
    private String message;
    private String correlationId;

    public ErrorResponse() {
    }

    public ErrorResponse(String error, String message, String correlationId) {
        this.error = error;
        this.message = message;
        this.correlationId = correlationId;
    }

    public String getError() {
        return error;
    }

    public void setError(String error) {
        this.error = error;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public String getCorrelationId() {
        return correlationId;
    }

    public void setCorrelationId(String correlationId) {
        this.correlationId = correlationId;
    }
}
