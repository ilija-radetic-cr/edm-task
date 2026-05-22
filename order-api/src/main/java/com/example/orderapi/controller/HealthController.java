package com.example.orderapi.controller;

import com.example.common.event.Event;
import com.example.common.event.OrderPayload;
import org.apache.kafka.clients.producer.Producer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
public class HealthController {

    private final Producer<String, Event<OrderPayload>> producer;
    private final String topic;

    public HealthController(
            Producer<String, Event<OrderPayload>> producer,
            @Value("${kafka.topic.orders:orders}") String topic) {
        this.producer = producer;
        this.topic = topic;
    }

    @GetMapping("/health")
    public ResponseEntity<Map<String, String>> health() {
        return ResponseEntity.ok(Map.of("status", "UP"));
    }

    @GetMapping("/ready")
    public ResponseEntity<Map<String, String>> ready() {
        try {
            producer.partitionsFor(topic);
            return ResponseEntity.ok(Map.of("status", "READY"));
        } catch (Exception e) {
            return ResponseEntity.status(503).body(Map.of("status", "NOT_READY"));
        }
    }
}
