package com.example.orderapi.config;

import com.example.common.event.Event;
import com.example.common.event.OrderPayload;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Properties;

@Configuration
public class KafkaProducerConfig {

    @Value("${kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${kafka.producer.timeout-ms:5000}")
    private int producerTimeoutMs;

    @Bean
    public ObjectMapper objectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        return mapper;
    }

    @Bean(destroyMethod = "close")
    public Producer<String, Event<OrderPayload>> kafkaProducer(ObjectMapper objectMapper) {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("acks", "all");
        props.put("retries", 3);
        props.put("enable.idempotence", "true");
        props.put("max.in.flight.requests.per.connection", "5");  // Safe with idempotence since Kafka 2.0+
        props.put("request.timeout.ms", String.valueOf(producerTimeoutMs));
        props.put("delivery.timeout.ms", String.valueOf(producerTimeoutMs + 1000));

        return new KafkaProducer<>(props, new StringSerializer(), new JsonSerializer<>(objectMapper));
    }
}
