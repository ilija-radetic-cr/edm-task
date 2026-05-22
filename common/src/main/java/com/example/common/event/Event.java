package com.example.common.event;

import com.fasterxml.jackson.annotation.JsonFormat;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class Event<T> {

    private String eventId;
    private String eventType;
    private String eventVersion = "1.0";
    private String producer;

    @JsonFormat(shape = JsonFormat.Shape.STRING)
    private Instant occurredAt;

    private String correlationId;
    private Map<String, String> metadata;
    private T payload;

    public Event() {
        this.metadata = new HashMap<>();
    }

    public static <T> Builder<T> builder() {
        return new Builder<>();
    }

    public static class Builder<T> {
        private final Event<T> event = new Event<>();

        public Builder() {
            event.eventId = UUID.randomUUID().toString();
            event.occurredAt = Instant.now();
            event.metadata = new HashMap<>();
        }

        public Builder<T> eventType(String eventType) {
            event.eventType = eventType;
            return this;
        }

        public Builder<T> eventVersion(String eventVersion) {
            event.eventVersion = eventVersion;
            return this;
        }

        public Builder<T> producer(String producer) {
            event.producer = producer;
            return this;
        }

        public Builder<T> correlationId(String correlationId) {
            event.correlationId = correlationId;
            return this;
        }

        public Builder<T> metadata(String key, String value) {
            event.metadata.put(key, value);
            return this;
        }

        public Builder<T> payload(T payload) {
            event.payload = payload;
            return this;
        }

        public Event<T> build() {
            return event;
        }
    }

    // Getters and setters

    public String getEventId() {
        return eventId;
    }

    public void setEventId(String eventId) {
        this.eventId = eventId;
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public String getEventVersion() {
        return eventVersion;
    }

    public void setEventVersion(String eventVersion) {
        this.eventVersion = eventVersion;
    }

    public String getProducer() {
        return producer;
    }

    public void setProducer(String producer) {
        this.producer = producer;
    }

    public Instant getOccurredAt() {
        return occurredAt;
    }

    public void setOccurredAt(Instant occurredAt) {
        this.occurredAt = occurredAt;
    }

    public String getCorrelationId() {
        return correlationId;
    }

    public void setCorrelationId(String correlationId) {
        this.correlationId = correlationId;
    }

    public Map<String, String> getMetadata() {
        return metadata;
    }

    public void setMetadata(Map<String, String> metadata) {
        this.metadata = metadata;
    }

    public T getPayload() {
        return payload;
    }

    public void setPayload(T payload) {
        this.payload = payload;
    }

    @Override
    public String toString() {
        return "Event{" +
                "eventId='" + eventId + '\'' +
                ", eventType='" + eventType + '\'' +
                ", eventVersion='" + eventVersion + '\'' +
                ", producer='" + producer + '\'' +
                ", occurredAt=" + occurredAt +
                ", correlationId='" + correlationId + '\'' +
                ", metadata=" + metadata +
                ", payload=" + payload +
                '}';
    }
}
