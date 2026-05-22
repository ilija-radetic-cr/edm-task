package com.example.inventory.controller;

import com.example.inventory.listener.OrderEventListener;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
public class HealthController {

    private final OrderEventListener orderEventListener;

    public HealthController(OrderEventListener orderEventListener) {
        this.orderEventListener = orderEventListener;
    }

    @GetMapping("/health")
    public ResponseEntity<Map<String, String>> health() {
        return ResponseEntity.ok(Map.of("status", "UP"));
    }

    @GetMapping("/ready")
    public ResponseEntity<Map<String, String>> ready() {
        if (orderEventListener.isConsumerHealthy()) {
            return ResponseEntity.ok(Map.of("status", "READY"));
        }
        return ResponseEntity.status(503).body(Map.of("status", "NOT_READY"));
    }
}
