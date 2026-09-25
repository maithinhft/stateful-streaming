package com.vdf.streaming;

import com.vdf.streaming.config.KafkaClusterConfig;
import com.vdf.streaming.dynamic.deserializer.KafkaEventRecordDeserializer;
import com.vdf.streaming.dynamic.metadata.PostgresKafkaMetadataService;
import com.vdf.streaming.dynamic.model.KafkaEventRecord;
import com.vdf.streaming.operators.BatchSchemaValidationFunction;
import com.vdf.streaming.operators.StreamSchemaValidationFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.dynamic.source.DynamicKafkaSource;
import org.apache.flink.connector.kafka.dynamic.source.DynamicKafkaSourceOptions;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.configuration.ExternalizedCheckpointRetention;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * Flink Job: Dynamic Kafka Pass-Through &amp; Validation Job.
 *
 * <p>Kiến trúc xử lý:
 * <ol>
 *   <li>Luồng Stream (Realtime DataStream): Đọc từ DynamicKafkaSource (metadata prefix 'kafka_stream', streamId 'stream-events', 10 topics).
 *       Thẩm định tính hợp lệ theo schema và chuẩn hóa số điện thoại E.164.</li>
 *   <li>Luồng Batch (Batch DataStream): Đọc từ DynamicKafkaSource (metadata prefix 'kafka_batch', streamId 'batch-events', 7 topics).
 *       Thẩm định theo quy trình 5 tầng của Batch Envelope Spec (Protocol, Key Normalizer, Schema Registry, Anti-Stale, Field Constraints).</li>
 *   <li>Cả 2 luồng sau khi vượt qua thẩm định được hợp nhất (union) và đẩy vào Kafka topic 'result'.</li>
 * </ol>
 */
public class DynamicPassThroughJob {
    private static final Logger LOG = LoggerFactory.getLogger(DynamicPassThroughJob.class);

    public static void main(String[] args) throws Exception {
        LOG.info("Starting Flink Dynamic Kafka Pass-Through & Validation Job...");

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        ParameterTool parameters = ParameterTool.fromArgs(args);
        env.getConfig().setGlobalJobParameters(parameters);

        int parallelism = parameters.getInt("parallelism", 4);
        env.setParallelism(parallelism);
        env.getConfig().setLatencyTrackingInterval(parameters.getLong("latency.tracking.interval", 5000L));

        // Cấu hình Checkpointing: mặc định 5 phút / lần (300,000 ms)
        long checkpointInterval = parameters.getLong("checkpoint.interval", 300_000L);
        env.enableCheckpointing(checkpointInterval, CheckpointingMode.EXACTLY_ONCE);

        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setMinPauseBetweenCheckpoints(parameters.getLong("checkpoint.min.pause", 30_000L));
        checkpointConfig.setCheckpointTimeout(parameters.getLong("checkpoint.timeout", 120_000L));
        checkpointConfig.setMaxConcurrentCheckpoints(1);
        checkpointConfig.setExternalizedCheckpointRetention(
                ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION);

        if (parameters.has("checkpoint.dir")) {
            checkpointConfig.setCheckpointStorage(parameters.get("checkpoint.dir"));
        }

        // 1. Cấu hình kết nối PostgreSQL Catalog & Metadata
        String pgHost = parameters.get("postgres.host", "postgres");
        String pgPort = parameters.get("postgres.port", "5432");
        String pgDb = parameters.get("postgres.db", "realtime_core");
        String defaultPgUrl = String.format("jdbc:postgresql://%s:%s/%s", pgHost, pgPort, pgDb);
        String pgUrl = parameters.get("postgres.url", defaultPgUrl);
        String pgUser = parameters.get("postgres.user", "postgres");
        String pgPassword = parameters.get("postgres.password", "postgres");

        // 1.1 Metadata Service cho Stream Events (kafka_stream_*)
        String streamTablePrefix = parameters.get("stream.table.prefix", "kafka_stream");
        String streamEventsId = parameters.get("stream.events.stream.id", "stream-events");
        long streamDiscoveryIntervalMs = parameters.getLong("stream.metadata.discovery.interval.ms", 30000L);

        LOG.info("Postgres Metadata Service for STREAM: {} (prefix: {}, streamId: {})",
                pgUrl, streamTablePrefix, streamEventsId);
        PostgresKafkaMetadataService streamMetadataService = new PostgresKafkaMetadataService(
                pgUrl, pgUser, pgPassword, streamTablePrefix, 5000L);

        // 1.2 Metadata Service cho Batch Events (kafka_batch_*)
        String batchTablePrefix = parameters.get("batch.table.prefix", "kafka_batch");
        String batchEventsId = parameters.get("batch.events.stream.id", "batch-events");
        long batchDiscoveryIntervalMs = parameters.getLong("batch.metadata.discovery.interval.ms", 30000L);

        LOG.info("Postgres Metadata Service for BATCH: {} (prefix: {}, streamId: {})",
                pgUrl, batchTablePrefix, batchEventsId);
        PostgresKafkaMetadataService batchMetadataService = new PostgresKafkaMetadataService(
                pgUrl, pgUser, pgPassword, batchTablePrefix, 5000L);

        // 2. Cấu hình Result Sink (cụm PLAIN: topic 'result')
        String resultBootstrap = KafkaClusterConfig.getBootstrapServers(parameters, "result", KafkaClusterConfig.CLUSTER_PLAIN);
        Properties resultProps = KafkaClusterConfig.getProducerProperties(parameters, "result", KafkaClusterConfig.CLUSTER_PLAIN);
        String resultTopic = parameters.get("result.topic", "result");
        LOG.info("Result Sink -> Bootstrap: {}, Topic: {}", resultBootstrap, resultTopic);

        KafkaSink<String> resultSink = KafkaSink.<String>builder()
                .setBootstrapServers(resultBootstrap)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic(resultTopic)
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build())
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .setKafkaProducerConfig(resultProps)
                .build();

        // 3. Watermark Strategy cho KafkaEventRecord
        WatermarkStrategy<KafkaEventRecord> eventWatermarkStrategy = WatermarkStrategy
                .<KafkaEventRecord>forBoundedOutOfOrderness(Duration.ofMinutes(1))
                .withTimestampAssigner((record, recordTimestamp) -> {
                    if (record.getTimestamp() > 0) {
                        return record.getTimestamp();
                    }
                    return System.currentTimeMillis();
                })
                .withIdleness(Duration.ofMinutes(1));

        boolean useDynamicSource = parameters.getBoolean("use.dynamic.source", true);

        // ====================================================================
        // 4. LUỒNG 1: REALTIME STREAM DATASTREAM
        // ====================================================================
        DataStream<KafkaEventRecord> streamEventStream;
        if (useDynamicSource) {
            LOG.info("Subscribing to Stream Events via DynamicKafkaSource ('{}')", streamEventsId);
            DynamicKafkaSource<KafkaEventRecord> dynamicStreamSource = DynamicKafkaSource.<KafkaEventRecord>builder()
                    .setKafkaMetadataService(streamMetadataService)
                    .setStreamIds(Collections.singleton(streamEventsId))
                    .setDeserializer(new KafkaEventRecordDeserializer())
                    .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                    .setGroupId(parameters.get("stream.events.group.id", "flink-stream-passthrough-group"))
                    .setProperty(DynamicKafkaSourceOptions.STREAM_METADATA_DISCOVERY_INTERVAL_MS.key(), String.valueOf(streamDiscoveryIntervalMs))
                    .build();

            streamEventStream = env.fromSource(
                    dynamicStreamSource,
                    eventWatermarkStrategy,
                    "Dynamic Stream Multi-Cluster Source");
        } else {
            String streamBootstrap = KafkaClusterConfig.getBootstrapServers(parameters, "stream", KafkaClusterConfig.CLUSTER_PLAIN);
            Properties streamProps = KafkaClusterConfig.getConsumerProperties(parameters, "stream", KafkaClusterConfig.CLUSTER_PLAIN);
            KafkaSource<KafkaEventRecord> staticStreamSource = KafkaSource.<KafkaEventRecord>builder()
                    .setBootstrapServers(streamBootstrap)
                    .setTopicPattern(java.util.regex.Pattern.compile(parameters.get("stream.topic.pattern", ".*")))
                    .setGroupId(parameters.get("stream.events.group.id", "flink-stream-passthrough-group"))
                    .setProperty("partition.discovery.interval.ms", "60000")
                    .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
                    .setDeserializer(new KafkaEventRecordDeserializer())
                    .setProperties(streamProps)
                    .build();

            streamEventStream = env.fromSource(
                    staticStreamSource,
                    eventWatermarkStrategy,
                    "Stream Static Kafka Source");
        }

        // ====================================================================
        // 5. LUỒNG 2: BATCH EVENT DATASTREAM
        // ====================================================================
        DataStream<KafkaEventRecord> batchEventStream;
        if (useDynamicSource) {
            LOG.info("Subscribing to Batch Events via DynamicKafkaSource ('{}')", batchEventsId);
            DynamicKafkaSource<KafkaEventRecord> dynamicBatchSource = DynamicKafkaSource.<KafkaEventRecord>builder()
                    .setKafkaMetadataService(batchMetadataService)
                    .setStreamIds(Collections.singleton(batchEventsId))
                    .setDeserializer(new KafkaEventRecordDeserializer())
                    .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                    .setGroupId(parameters.get("batch.events.group.id", "flink-batch-passthrough-group"))
                    .setProperty(DynamicKafkaSourceOptions.STREAM_METADATA_DISCOVERY_INTERVAL_MS.key(), String.valueOf(batchDiscoveryIntervalMs))
                    .build();

            batchEventStream = env.fromSource(
                    dynamicBatchSource,
                    eventWatermarkStrategy,
                    "Dynamic Batch Source");
        } else {
            String batchBootstrap = KafkaClusterConfig.getBootstrapServers(parameters, "batch", KafkaClusterConfig.CLUSTER_PLAIN);
            Properties batchProps = KafkaClusterConfig.getConsumerProperties(parameters, "batch", KafkaClusterConfig.CLUSTER_PLAIN);
            KafkaSource<KafkaEventRecord> staticBatchSource = KafkaSource.<KafkaEventRecord>builder()
                    .setBootstrapServers(batchBootstrap)
                    .setTopicPattern(java.util.regex.Pattern.compile(parameters.get("batch.topic.pattern", "batch_.*")))
                    .setGroupId(parameters.get("batch.events.group.id", "flink-batch-passthrough-group"))
                    .setProperty("partition.discovery.interval.ms", "60000")
                    .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
                    .setDeserializer(new KafkaEventRecordDeserializer())
                    .setProperties(batchProps)
                    .build();

            batchEventStream = env.fromSource(
                    staticBatchSource,
                    eventWatermarkStrategy,
                    "Batch Static Kafka Source");
        }

        // ====================================================================
        // 6. TẦNG THẨM ĐỊNH (VALIDATION PIPELINE)
        // ====================================================================
        // 6.1 Thẩm định luồng Stream
        DataStream<String> validStreamEvents = streamEventStream
                .process(new StreamSchemaValidationFunction(pgUrl, pgUser, pgPassword))
                .name("Stream Schema Validation & E.164 Normalization");

        // 6.2 Thẩm định luồng Batch theo quy trình 5 tầng
        SingleOutputStreamOperator<String> validBatchEvents = batchEventStream
                .process(new BatchSchemaValidationFunction(pgUrl, pgUser, pgPassword))
                .name("Batch 5-Level Validation & Key Normalization");

        // Luồng Side Output nhận các bản tin Batch vi phạm thẩm định đẩy vào DLQ
        DataStream<String> dlqBatchEvents = validBatchEvents.getSideOutput(BatchSchemaValidationFunction.DIRTY_BATCH_DATA_TAG);

        // ====================================================================
        // 7. HỢP NHẤT VÀ ĐẨY RA TOPIC 'RESULT'
        // ====================================================================
        DataStream<String> allValidEvents = validStreamEvents.union(validBatchEvents);

        if (parameters.getBoolean("print.output", false)) {
            allValidEvents.print("VALID_PASSTHROUGH_EVENT");
            dlqBatchEvents.print("DLQ_BATCH_EVENT");
        }

        allValidEvents.sinkTo(resultSink).name("Result Kafka Sink");

        // ====================================================================
        // 8. ĐẨY CÁC BẢN TIN LỖI BATCH RA TOPIC 'dlq_batch_events'
        // ====================================================================
        String dlqBatchBootstrap = KafkaClusterConfig.getBootstrapServers(parameters, "batch_dlq", KafkaClusterConfig.CLUSTER_PLAIN);
        Properties dlqBatchProps = KafkaClusterConfig.getProducerProperties(parameters, "batch_dlq", KafkaClusterConfig.CLUSTER_PLAIN);
        String dlqBatchTopic = parameters.get("batch.dlq.topic", "dlq_batch_events");
        LOG.info("Batch DLQ Sink -> Bootstrap: {}, Topic: {}", dlqBatchBootstrap, dlqBatchTopic);

        KafkaSink<String> dlqBatchSink = KafkaSink.<String>builder()
                .setBootstrapServers(dlqBatchBootstrap)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic(dlqBatchTopic)
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build())
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .setKafkaProducerConfig(dlqBatchProps)
                .build();

        dlqBatchEvents.sinkTo(dlqBatchSink).name("Batch DLQ Kafka Sink");

        env.execute("Flink Dynamic Kafka Pass-Through & Validation Job");
    }
}
