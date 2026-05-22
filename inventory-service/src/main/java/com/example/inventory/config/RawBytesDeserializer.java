package com.example.inventory.config;

import com.example.common.event.Event;
import com.example.common.event.OrderPayload;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class RawBytesDeserializer implements Deserializer<RawBytesDeserializer.DeserializationResult> {

    private static final Logger log = LoggerFactory.getLogger(RawBytesDeserializer.class);

    private final ObjectMapper objectMapper;
    private final TypeReference<Event<OrderPayload>> typeReference;

    public RawBytesDeserializer(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
        this.typeReference = new TypeReference<>() {};
    }

    @Override
    public DeserializationResult deserialize(String topic, byte[] data) {
        if (data == null) {
            return new DeserializationResult(null, null, null);
        }
        try {
            Event<OrderPayload> event = objectMapper.readValue(data, typeReference);
            return new DeserializationResult(event, data, null);
        } catch (IOException e) {
            log.error("Deserialization failed for topic {}: {}", topic, e.getMessage());
            return new DeserializationResult(null, data, e.getMessage());
        }
    }

    public record DeserializationResult(
            Event<OrderPayload> event,
            byte[] rawBytes,
            String errorMessage
    ) {
        public boolean isSuccess() {
            return event != null;
        }

        public boolean isFailed() {
            return event == null && rawBytes != null;
        }
    }
}
