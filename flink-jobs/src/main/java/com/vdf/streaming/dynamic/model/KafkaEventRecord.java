package com.vdf.streaming.dynamic.model;

import java.io.Serializable;
import java.util.Objects;

/**
 * Lớp đại diện cho một bản tin Kafka được đọc từ DynamicKafkaSource.
 * Giữ nguyên thông tin metadata cần thiết như tên topic, khóa phân vùng và timestamp.
 */
public class KafkaEventRecord implements Serializable {

    private static final long serialVersionUID = 1L;

    private String topic;
    private String key;
    private String payload;
    private long timestamp;

    public KafkaEventRecord() {}

    public KafkaEventRecord(String topic, String key, String payload, long timestamp) {
        this.topic = topic;
        this.key = key;
        this.payload = payload;
        this.timestamp = timestamp;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getPayload() {
        return payload;
    }

    public void setPayload(String payload) {
        this.payload = payload;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        KafkaEventRecord that = (KafkaEventRecord) o;
        return timestamp == that.timestamp &&
                Objects.equals(topic, that.topic) &&
                Objects.equals(key, that.key) &&
                Objects.equals(payload, that.payload);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topic, key, payload, timestamp);
    }

    @Override
    public String toString() {
        return "KafkaEventRecord{" +
                "topic='" + topic + '\'' +
                ", key='" + key + '\'' +
                ", timestamp=" + timestamp +
                ", payloadLength=" + (payload != null ? payload.length() : 0) +
                '}';
    }
}
