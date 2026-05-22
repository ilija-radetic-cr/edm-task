package com.example.inventory.service;

import com.example.common.event.Event;
import com.example.common.event.OrderPayload;
import com.example.inventory.model.ReservationResult;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

@Service
public class InventoryService {

    private static final Logger log = LoggerFactory.getLogger(InventoryService.class);

    private final Map<String, Integer> inventory = new ConcurrentHashMap<>();
    private final ObjectMapper objectMapper;
    private final Resource stockFile;

    public InventoryService(
            ObjectMapper objectMapper,
            @Value("${inventory.stock-file:classpath:stock-init.json}") Resource stockFile) {
        this.objectMapper = objectMapper;
        this.stockFile = stockFile;
    }

    @PostConstruct
    public void init() {
        loadInitialStock();
    }

    private void loadInitialStock() {
        try {
            Map<String, Integer> initialStock = objectMapper.readValue(
                    stockFile.getInputStream(),
                    new TypeReference<>() {}
            );
            inventory.putAll(initialStock);
            log.info("Loaded initial stock: {}", initialStock);
        } catch (IOException e) {
            log.warn("Could not load initial stock from {}: {}", stockFile, e.getMessage());
        }
    }

    public ReservationResult reserveInventory(Event<OrderPayload> event) {
        OrderPayload order = event.getPayload();
        String itemId = order.getItemId();
        int quantity = order.getQuantity();

        AtomicReference<ReservationResult> result = new AtomicReference<>();

        inventory.compute(itemId, (key, currentStock) -> {
            int current = currentStock != null ? currentStock : 0;

            if (current >= quantity) {
                int newStock = current - quantity;
                result.set(ReservationResult.accepted(current, newStock, quantity));
                return newStock;
            } else {
                result.set(ReservationResult.rejected(current, quantity));
                return current;
            }
        });

        ReservationResult reservationResult = result.get();

        log.info("Inventory {}: eventId={}, correlationId={}, itemId={}, quantity={}, stock: {} -> {}",
                reservationResult.status(),
                event.getEventId(),
                event.getCorrelationId(),
                itemId,
                quantity,
                reservationResult.stockBefore(),
                reservationResult.stockAfter());

        return reservationResult;
    }

    public void releaseInventory(Event<OrderPayload> event) {
        OrderPayload order = event.getPayload();
        String itemId = order.getItemId();
        int quantity = order.getQuantity();

        inventory.compute(itemId, (key, currentStock) -> {
            int current = currentStock != null ? currentStock : 0;
            int newStock = current + quantity;

            log.info("Inventory RELEASED: eventId={}, correlationId={}, itemId={}, quantity={}, {} -> {}",
                    event.getEventId(), event.getCorrelationId(), itemId, quantity, current, newStock);

            return newStock;
        });
    }

    public int getStock(String itemId) {
        return inventory.getOrDefault(itemId, 0);
    }
}
