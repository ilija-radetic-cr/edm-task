package com.example.inventory.model;

public record ReservationResult(
        Status status,
        int stockBefore,
        int stockAfter,
        int quantityRequested
) {
    public enum Status {
        ACCEPTED,
        REJECTED
    }

    public static ReservationResult accepted(int stockBefore, int stockAfter, int quantity) {
        return new ReservationResult(Status.ACCEPTED, stockBefore, stockAfter, quantity);
    }

    public static ReservationResult rejected(int currentStock, int quantity) {
        return new ReservationResult(Status.REJECTED, currentStock, currentStock, quantity);
    }
}
