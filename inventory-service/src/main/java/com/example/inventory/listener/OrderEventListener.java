package com.example.inventory.listener;

import com.example.common.event.Event;
import com.example.common.event.EventTypes;
import com.example.common.event.OrderPayload;
import com.example.inventory.config.RawBytesDeserializer;
import com.example.inventory.config.RawBytesDeserializer.DeserializationResult;
import com.example.inventory.messaging.DltProducer;
import com.example.inventory.model.ReservationResult;
import com.example.inventory.service.InventoryService;
import com.example.inventory.state.ProcessedEventStore;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

@Component
public class OrderEventListener {

    private static final Logger log = LoggerFactory.getLogger(OrderEventListener.class);
    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("HH:mm:ss.SSS")
            .withZone(ZoneId.systemDefault());

    private final InventoryService inventoryService;
    private final ProcessedEventStore processedEventStore;
    private final DltProducer dltProducer;
    private final String bootstrapServers;
    private final String topic;
    private final String groupId;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final AtomicBoolean consumerHealthy = new AtomicBoolean(false);
    private KafkaConsumer<String, DeserializationResult> consumer;
    private Thread consumerThread;

    public OrderEventListener(
            InventoryService inventoryService,
            ProcessedEventStore processedEventStore,
            DltProducer dltProducer,
            @Value("${kafka.bootstrap-servers}") String bootstrapServers,
            @Value("${kafka.topic.orders:orders}") String topic,
            @Value("${kafka.consumer.group-id:inventory-service}") String groupId) {
        this.inventoryService = inventoryService;
        this.processedEventStore = processedEventStore;
        this.dltProducer = dltProducer;
        this.bootstrapServers = bootstrapServers;
        this.topic = topic;
        this.groupId = groupId;
    }

    @PostConstruct
    public void start() {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());

        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("group.id", groupId);
        props.put("auto.offset.reset", "earliest");
        props.put("enable.auto.commit", "false");
        props.put("max.poll.records", "100");
        props.put("max.poll.interval.ms", "300000");
        props.put("session.timeout.ms", "30000");
        props.put("heartbeat.interval.ms", "10000");
        props.put("partition.assignment.strategy",
                "org.apache.kafka.clients.consumer.CooperativeStickyAssignor");
        // Note: read_committed only needed with transactional producers, not used here

        consumer = new KafkaConsumer<>(
                props,
                new StringDeserializer(),
                new RawBytesDeserializer(objectMapper)
        );

        consumer.subscribe(List.of(topic));

        consumerThread = new Thread(this::pollLoop, "kafka-consumer");
        consumerThread.start();

        log.info("Kafka consumer started for topic: {}", topic);
    }

    private void pollLoop() {
        try {
            consumerHealthy.set(true);
            while (running.get()) {
                ConsumerRecords<String, DeserializationResult> records = consumer.poll(Duration.ofMillis(500));

                for (ConsumerRecord<String, DeserializationResult> record : records) {
                    DeserializationResult result = record.value();

                    // Handle deserialization failure
                    if (result.isFailed()) {
                        log.warn("Deserialization failed at partition={}, offset={}, sending to DLT",
                                record.partition(), record.offset());
                        dltProducer.sendToDlt(
                                record.topic(),
                                record.partition(),
                                record.offset(),
                                record.key(),
                                result.rawBytes(),
                                "DESERIALIZATION_FAILED",
                                result.errorMessage()
                        );
                        continue;
                    }

                    Event<OrderPayload> event = result.event();
                    if (event == null) {
                        continue;
                    }

                    // Business idempotency check using eventType + orderId
                    // Why not just eventId? Client retry generates new eventId for same orderId
                    // Why not just orderId? Would block OrderCancelled after OrderCreated
                    String orderId = event.getPayload().getOrderId();
                    String idempotencyKey = event.getEventType() + ":" + orderId;
                    if (processedEventStore.isProcessed(idempotencyKey)) {
                        log.info("Skipping duplicate: key={}, eventId={}",
                                idempotencyKey, event.getEventId());
                        continue;
                    }

                    try {
                        logEvent(record, event);
                        routeEvent(event);

                        // Mark as processed (by eventType:orderId for business idempotency)
                        processedEventStore.markProcessed(idempotencyKey);
                    } catch (Exception e) {
                        log.error("Processing failed for eventId={}, sending to DLT",
                                event.getEventId(), e);
                        dltProducer.sendToDlt(
                                record.topic(),
                                record.partition(),
                                record.offset(),
                                record.key(),
                                result.rawBytes(),
                                "PROCESSING_FAILED",
                                e.getMessage()
                        );
                    }
                }

                // Manual commit after batch
                if (!records.isEmpty()) {
                    consumer.commitSync();
                }
            }
        } catch (WakeupException e) {
            if (running.get()) {
                throw e;
            }
        } finally {
            consumerHealthy.set(false);
            consumer.close();
            log.info("Kafka consumer closed");
        }
    }

    private void routeEvent(Event<OrderPayload> event) {
        switch (event.getEventType()) {
            case EventTypes.ORDER_CREATED -> {
                ReservationResult result = inventoryService.reserveInventory(event);
            }
            case EventTypes.ORDER_CANCELLED -> inventoryService.releaseInventory(event);
            default -> log.warn("Unknown event type: {}", event.getEventType());
        }
    }

    private void logEvent(ConsumerRecord<String, DeserializationResult> record, Event<OrderPayload> event) {
        log.info("=== Event Received ===");
        log.info("  Kafka Metadata:");
        log.info("    Topic:     {}", record.topic());
        log.info("    Partition: {}", record.partition());
        log.info("    Offset:    {}", record.offset());
        log.info("    Timestamp: {} ({})", record.timestamp(), formatter.format(java.time.Instant.ofEpochMilli(record.timestamp())));
        log.info("  Event Envelope:");
        log.info("    EventId:       {}", event.getEventId());
        log.info("    EventType:     {}", event.getEventType());
        log.info("    Producer:      {}", event.getProducer());
        log.info("    OccurredAt:    {}", event.getOccurredAt());
        log.info("    CorrelationId: {}", event.getCorrelationId());
        log.info("    Metadata:      {}", event.getMetadata());
        log.info("  Payload: {}", event.getPayload());
        log.info("======================");
    }

    public boolean isConsumerHealthy() {
        return consumerHealthy.get();
    }

    @PreDestroy
    public void stop() {
        running.set(false);
        consumer.wakeup();
        try {
            consumerThread.join(5000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
