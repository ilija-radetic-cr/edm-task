package com.example.inventory.config;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class JsonDeserializer<T> implements Deserializer<T> {

    private static final Logger log = LoggerFactory.getLogger(JsonDeserializer.class);

    private final ObjectMapper objectMapper;
    private final TypeReference<T> typeReference;

    public JsonDeserializer(ObjectMapper objectMapper, TypeReference<T> typeReference) {
        this.objectMapper = objectMapper;
        this.typeReference = typeReference;
    }

    @Override
    public T deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }
        try {
            return objectMapper.readValue(data, typeReference);
        } catch (IOException e) {
            log.error("Deserialization failed for topic {}: {}", topic, e.getMessage());
            return null;  // Let listener handle DLT routing
        }
    }
}
