package com.example.orderapi.service;

import com.example.common.event.Event;
import com.example.common.event.EventTypes;
import com.example.common.event.OrderPayload;
import com.example.orderapi.dto.CreateOrderRequest;
import com.example.orderapi.dto.OrderResponse;
import com.example.orderapi.exception.PublishException;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

@Service
public class OrderService {

    private static final Logger log = LoggerFactory.getLogger(OrderService.class);
    private static final String PRODUCER = "order-api";

    private final Producer<String, Event<OrderPayload>> producer;
    private final String topic;
    private final int timeoutMs;

    public OrderService(
            Producer<String, Event<OrderPayload>> producer,
            @Value("${kafka.topic.orders:orders}") String topic,
            @Value("${kafka.producer.timeout-ms:5000}") int timeoutMs) {
        this.producer = producer;
        this.topic = topic;
        this.timeoutMs = timeoutMs;
    }

    public OrderResponse publishOrderCreated(CreateOrderRequest request, String correlationId) {
        OrderPayload payload = new OrderPayload(
                request.getOrderId(),
                request.getItemId(),
                request.getQuantity()
        );

        Event<OrderPayload> event = Event.<OrderPayload>builder()
                .eventType(EventTypes.ORDER_CREATED)
                .producer(PRODUCER)
                .correlationId(correlationId)
                .payload(payload)
                .build();

        ProducerRecord<String, Event<OrderPayload>> record =
                new ProducerRecord<>(topic, request.getItemId(), event);

        // Add headers
        record.headers()
                .add("correlationId", correlationId.getBytes(StandardCharsets.UTF_8))
                .add("eventId", event.getEventId().getBytes(StandardCharsets.UTF_8))
                .add("eventType", event.getEventType().getBytes(StandardCharsets.UTF_8))
                .add("eventVersion", event.getEventVersion().getBytes(StandardCharsets.UTF_8))
                .add("producer", PRODUCER.getBytes(StandardCharsets.UTF_8));

        try {
            var metadata = producer.send(record).get(timeoutMs, TimeUnit.MILLISECONDS);
            log.info("Event published: eventId={}, type={}, partition={}, offset={}",
                    event.getEventId(), event.getEventType(), metadata.partition(), metadata.offset());
            return new OrderResponse(request.getOrderId(), correlationId, "accepted");
        } catch (Exception e) {
            log.error("Failed to publish event: eventId={}, correlationId={}", event.getEventId(), correlationId, e);
            throw new PublishException("Failed to publish event", correlationId, e);
        }
    }
}
