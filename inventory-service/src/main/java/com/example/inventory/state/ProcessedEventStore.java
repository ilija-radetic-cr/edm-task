package com.example.inventory.state;

import org.springframework.stereotype.Component;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Tracks processed commands for business-level idempotency.
 *
 * Key format: eventType + ":" + orderId
 *
 * Why not just eventId?
 * - Client retry of same POST /orders generates new eventId for same orderId
 * - eventId dedup would not catch business duplicates
 *
 * Why not just orderId?
 * - Would block OrderCancelled after OrderCreated for same order
 * - Different event types for same order are valid business operations
 *
 * In production: would persist to Redis/DB to survive restarts.
 */
@Component
public class ProcessedEventStore {

    private final Set<String> processedCommands = ConcurrentHashMap.newKeySet();

    public boolean isProcessed(String idempotencyKey) {
        return processedCommands.contains(idempotencyKey);
    }

    public void markProcessed(String idempotencyKey) {
        processedCommands.add(idempotencyKey);
    }

    public int size() {
        return processedCommands.size();
    }
}
