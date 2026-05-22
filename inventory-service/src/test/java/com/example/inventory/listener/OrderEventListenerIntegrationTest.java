package com.example.inventory.listener;

import com.example.common.event.Event;
import com.example.common.event.EventTypes;
import com.example.common.event.OrderPayload;
import com.example.inventory.service.InventoryService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.time.Duration;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(properties = {
        "kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "kafka.topic.orders=orders-test",
        "kafka.topic.dlt=orders-dlt-test",
        "kafka.consumer.group-id=inventory-service-test"
})
@EmbeddedKafka(partitions = 3, topics = {"orders-test", "orders-dlt-test"})
class OrderEventListenerIntegrationTest {

    private static final String ORDERS_TOPIC = "orders-test";
    private static final String DLT_TOPIC = "orders-dlt-test";

    @Autowired
    private InventoryService inventoryService;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafka;

    @Value("${spring.embedded.kafka.brokers}")
    private String bootstrapServers;

    @Test
    void consumesOrderCreatedEventAndSkipsDuplicateBusinessCommand() throws Exception {
        String orderId = "ORD-" + UUID.randomUUID();
        Event<OrderPayload> event = Event.<OrderPayload>builder()
                .eventType(EventTypes.ORDER_CREATED)
                .producer("test")
                .correlationId("corr-" + orderId)
                .payload(new OrderPayload(orderId, "item-1", 10))
                .build();

        send("item-1", objectMapper.writeValueAsBytes(event));

        waitForStock("item-1", 90);

        send("item-1", objectMapper.writeValueAsBytes(event));

        Thread.sleep(700);
        assertThat(inventoryService.getStock("item-1")).isEqualTo(90);
    }

    @Test
    void sendsMalformedMessagesToDeadLetterTopic() {
        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps(
                "dlt-test-" + UUID.randomUUID(), "true", embeddedKafka);
        consumerProps.put("key.deserializer", StringDeserializer.class);
        consumerProps.put("value.deserializer", ByteArrayDeserializer.class);

        try (Consumer<String, byte[]> dltConsumer =
                     new org.apache.kafka.clients.consumer.KafkaConsumer<>(consumerProps)) {
            embeddedKafka.consumeFromAnEmbeddedTopic(dltConsumer, DLT_TOPIC);

            send("item-1", "{not-json".getBytes());

            ConsumerRecord<String, byte[]> record =
                    KafkaTestUtils.getSingleRecord(dltConsumer, DLT_TOPIC, Duration.ofSeconds(10));

            assertThat(record.key()).isEqualTo("item-1");
            assertThat(record.headers().lastHeader("x-failure-reason")).isNotNull();
            assertThat(new String(record.headers().lastHeader("x-failure-reason").value()))
                    .isEqualTo("DESERIALIZATION_FAILED");
        }
    }

    private void send(String key, byte[] value) {
        Map<String, Object> producerProps = KafkaTestUtils.producerProps(bootstrapServers);
        producerProps.put("key.serializer", StringSerializer.class);
        producerProps.put("value.serializer", ByteArraySerializer.class);

        try (Producer<String, byte[]> producer = new KafkaProducer<>(producerProps)) {
            producer.send(new ProducerRecord<>(ORDERS_TOPIC, key, value));
            producer.flush();
        }
    }

    private void waitForStock(String itemId, int expectedStock) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline) {
            if (inventoryService.getStock(itemId) == expectedStock) {
                return;
            }
            Thread.sleep(100);
        }
        assertThat(inventoryService.getStock(itemId)).isEqualTo(expectedStock);
    }
}
