package com.example.orderapi.dto;

public class OrderResponse {

    private String orderId;
    private String correlationId;
    private String status;

    public OrderResponse() {
    }

    public OrderResponse(String orderId, String correlationId, String status) {
        this.orderId = orderId;
        this.correlationId = correlationId;
        this.status = status;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getCorrelationId() {
        return correlationId;
    }

    public void setCorrelationId(String correlationId) {
        this.correlationId = correlationId;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }
}
