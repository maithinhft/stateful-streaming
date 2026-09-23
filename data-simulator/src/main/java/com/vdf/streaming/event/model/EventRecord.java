package com.vdf.streaming.event.model;

import com.vdf.streaming.event.kafka.KafkaClusterType;

/**
 * Một bản ghi sự kiện sẵn sàng được đẩy vào Kafka hoặc ghi ra file.
 */
public class EventRecord {
    private final String topic;
    private final KafkaClusterType clusterType;
    private final String key;
    private final String payloadJson;
    private final TransactionContext context;

    public EventRecord(String topic, KafkaClusterType clusterType, String key, String payloadJson, TransactionContext context) {
        this.topic = topic;
        this.clusterType = clusterType;
        this.key = key;
        this.payloadJson = payloadJson;
        this.context = context;
    }

    public String getTopic() { return topic; }
    public KafkaClusterType getClusterType() { return clusterType; }
    public String getKey() { return key; }
    public String getPayloadJson() { return payloadJson; }
    public TransactionContext getContext() { return context; }

    @Override
    public String toString() {
        return String.format("[%s -> %s] key=%s, len=%d", clusterType, topic, key, payloadJson.length());
    }
}

