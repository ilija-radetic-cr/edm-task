package com.example.common.event;

public class OrderPayload {

    private String orderId;
    private String itemId;
    private Integer quantity;

    public OrderPayload() {
    }

    public OrderPayload(String orderId, String itemId, Integer quantity) {
        this.orderId = orderId;
        this.itemId = itemId;
        this.quantity = quantity;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getItemId() {
        return itemId;
    }

    public void setItemId(String itemId) {
        this.itemId = itemId;
    }

    public Integer getQuantity() {
        return quantity;
    }

    public void setQuantity(Integer quantity) {
        this.quantity = quantity;
    }

    @Override
    public String toString() {
        return "OrderPayload{" +
                "orderId='" + orderId + '\'' +
                ", itemId='" + itemId + '\'' +
                ", quantity=" + quantity +
                '}';
    }
}
