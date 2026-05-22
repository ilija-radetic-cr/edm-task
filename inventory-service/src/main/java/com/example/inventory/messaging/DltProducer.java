package com.example.inventory.messaging;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Properties;

@Component
public class DltProducer {

    private static final Logger log = LoggerFactory.getLogger(DltProducer.class);

    private final String bootstrapServers;
    private final String dltTopic;
    private Producer<String, byte[]> producer;

    public DltProducer(
            @Value("${kafka.bootstrap-servers}") String bootstrapServers,
            @Value("${kafka.topic.dlt:orders-dlt}") String dltTopic) {
        this.bootstrapServers = bootstrapServers;
        this.dltTopic = dltTopic;
    }

    @PostConstruct
    public void init() {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("acks", "all");
        props.put("retries", 3);

        producer = new KafkaProducer<>(props, new StringSerializer(), new ByteArraySerializer());
        log.info("DLT producer initialized for topic: {}", dltTopic);
    }

    public void sendToDlt(String originalTopic, int partition, long offset, String key,
                          byte[] rawValue, String reason, String message) {
        ProducerRecord<String, byte[]> record = new ProducerRecord<>(dltTopic, key, rawValue);

        Headers headers = record.headers();
        headers.add("x-original-topic", originalTopic.getBytes(StandardCharsets.UTF_8));
        headers.add("x-original-partition", String.valueOf(partition).getBytes(StandardCharsets.UTF_8));
        headers.add("x-original-offset", String.valueOf(offset).getBytes(StandardCharsets.UTF_8));
        headers.add("x-failure-reason", reason.getBytes(StandardCharsets.UTF_8));
        headers.add("x-failure-message", message.getBytes(StandardCharsets.UTF_8));
        headers.add("x-failed-at", Instant.now().toString().getBytes(StandardCharsets.UTF_8));

        producer.send(record, (metadata, exception) -> {
            if (exception == null) {
                log.info("Message sent to DLT: topic={}, partition={}, offset={}, reason={}",
                        dltTopic, metadata.partition(), metadata.offset(), reason);
            } else {
                log.error("Failed to send message to DLT: reason={}", reason, exception);
            }
        });
    }

    @PreDestroy
    public void close() {
        if (producer != null) {
            producer.close();
            log.info("DLT producer closed");
        }
    }
}
