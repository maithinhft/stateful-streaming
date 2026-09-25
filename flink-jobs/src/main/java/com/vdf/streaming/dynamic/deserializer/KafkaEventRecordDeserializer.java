package com.vdf.streaming.dynamic.deserializer;

import com.vdf.streaming.dynamic.model.KafkaEventRecord;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Deserializer cho DynamicKafkaSource, trích xuất toàn bộ thông tin bản tin Kafka
 * bao gồm tên topic, key, payload và timestamp sang {@link KafkaEventRecord}.
 */
public class KafkaEventRecordDeserializer implements KafkaRecordDeserializationSchema<KafkaEventRecord> {

    private static final long serialVersionUID = 1L;

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<KafkaEventRecord> out) throws IOException {
        String topic = record.topic();
        String key = (record.key() != null) ? new String(record.key(), StandardCharsets.UTF_8) : null;
        String payload = (record.value() != null) ? new String(record.value(), StandardCharsets.UTF_8) : "";
        long timestamp = record.timestamp();

        out.collect(new KafkaEventRecord(topic, key, payload, timestamp));
    }

    @Override
    public TypeInformation<KafkaEventRecord> getProducedType() {
        return TypeInformation.of(KafkaEventRecord.class);
    }
}
