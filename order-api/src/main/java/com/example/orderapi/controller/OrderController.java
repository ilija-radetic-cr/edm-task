package com.example.orderapi.controller;

import com.example.orderapi.dto.CreateOrderRequest;
import com.example.orderapi.dto.OrderResponse;
import com.example.orderapi.service.OrderService;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.UUID;

@RestController
@RequestMapping("/orders")
public class OrderController {

    private final OrderService orderService;

    public OrderController(OrderService orderService) {
        this.orderService = orderService;
    }

    @PostMapping
    public ResponseEntity<OrderResponse> createOrder(
            @Valid @RequestBody CreateOrderRequest request,
            @RequestHeader(value = "X-Correlation-Id", required = false) String correlationId) {

        if (correlationId == null || correlationId.isBlank()) {
            correlationId = UUID.randomUUID().toString();
        }

        OrderResponse response = orderService.publishOrderCreated(request, correlationId);
        return ResponseEntity.status(HttpStatus.ACCEPTED).body(response);
    }
}
