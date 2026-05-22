package com.example.orderapi.service;

import com.example.common.event.Event;
import com.example.common.event.EventTypes;
import com.example.common.event.OrderPayload;
import com.example.orderapi.config.JsonSerializer;
import com.example.orderapi.dto.CreateOrderRequest;
import com.example.orderapi.exception.PublishException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.Test;
import org.apache.kafka.common.serialization.StringSerializer;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class OrderServiceTest {

    @Test
    void publishOrderCreatedUsesItemIdAsKafkaKeyAndAddsTraceHeaders() {
        MockProducer<String, Event<OrderPayload>> producer = successfulProducer();
        OrderService service = new OrderService(producer, "orders", 5000);

        service.publishOrderCreated(new CreateOrderRequest("ORD-1", "item-1", 2), "corr-1");

        ProducerRecord<String, Event<OrderPayload>> record = producer.history().getFirst();
        Event<OrderPayload> event = record.value();

        assertThat(record.topic()).isEqualTo("orders");
        assertThat(record.key()).isEqualTo("item-1");
        assertThat(event.getEventType()).isEqualTo(EventTypes.ORDER_CREATED);
        assertThat(event.getCorrelationId()).isEqualTo("corr-1");
        assertThat(event.getPayload().getOrderId()).isEqualTo("ORD-1");
        assertThat(event.getPayload().getItemId()).isEqualTo("item-1");
        assertThat(event.getPayload().getQuantity()).isEqualTo(2);

        assertHeader(record, "correlationId", "corr-1");
        assertHeader(record, "eventId", event.getEventId());
        assertHeader(record, "eventType", EventTypes.ORDER_CREATED);
        assertHeader(record, "eventVersion", "1.0");
        assertHeader(record, "producer", "order-api");
    }

    @Test
    void publishOrderCreatedWrapsProducerFailure() {
        FailingProducer producer = new FailingProducer();
        OrderService service = new OrderService(producer, "orders", 5000);

        assertThatThrownBy(() ->
                service.publishOrderCreated(new CreateOrderRequest("ORD-1", "item-1", 2), "corr-1"))
                .isInstanceOf(PublishException.class)
                .hasMessage("Failed to publish event");
    }

    private static MockProducer<String, Event<OrderPayload>> successfulProducer() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());

        return new MockProducer<>(
                true,
                new StringSerializer(),
                new JsonSerializer<>(mapper)
        );
    }

    private static class FailingProducer extends MockProducer<String, Event<OrderPayload>> {
        FailingProducer() {
            super(true, new StringSerializer(), new JsonSerializer<>(new ObjectMapper().registerModule(new JavaTimeModule())));
        }

        @Override
        public Future<RecordMetadata> send(ProducerRecord<String, Event<OrderPayload>> record) {
            CompletableFuture<RecordMetadata> failed = new CompletableFuture<>();
            failed.completeExceptionally(new RuntimeException("broker unavailable"));
            return failed;
        }
    }

    private static void assertHeader(ProducerRecord<String, Event<OrderPayload>> record,
                                     String name,
                                     String expectedValue) {
        assertThat(record.headers().lastHeader(name)).isNotNull();
        assertThat(new String(record.headers().lastHeader(name).value(), StandardCharsets.UTF_8))
                .isEqualTo(expectedValue);
    }
}
