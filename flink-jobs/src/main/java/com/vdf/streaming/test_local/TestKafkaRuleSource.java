package com.vdf.streaming.test_local;

import com.vdf.streaming.compiler.RuleCompiler;
import com.vdf.streaming.config.ConfigLoader;
import com.vdf.streaming.config.KafkaClusterConfig;
import com.vdf.streaming.dynamic.metadata.PostgresKafkaMetadataService;
import com.vdf.streaming.models.CompiledRuleEnvelope;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.dynamic.metadata.ClusterMetadata;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class TestKafkaRuleSource {
    private static final Logger LOG = LoggerFactory.getLogger(TestKafkaRuleSource.class);

    public static void main(String[] args) {
        LOG.info("=== Khởi động Kafka Consumer (Dynamic Metadata) để parse Rule ===");

        ParameterTool params = ParameterTool.fromArgs(args);

        // 1. Cấu hình kết nối DB lấy metadata động – ưu tiên YAML config
        String pgUrl      = params.get("postgres.url",             ConfigLoader.getString("postgres.url", "jdbc:postgresql://localhost:5433/realtime_core"));
        String pgUser     = params.get("postgres.user",            ConfigLoader.getString("postgres.user", "postgres"));
        String pgPassword = params.get("postgres.password",        ConfigLoader.getString("postgres.password", ""));
        String tablePrefix = params.get("postgres.table.prefix",   ConfigLoader.getString("postgres.table_prefix", "kafka_stream"));

        LOG.info("Connecting to PostgreSQL for dynamic Kafka metadata: {}", pgUrl);
        PostgresKafkaMetadataService metadataService = new PostgresKafkaMetadataService(
                pgUrl, pgUser, pgPassword, tablePrefix, 5000L);

        String ruleStreamId = params.get("rule.stream.id", ConfigLoader.getString("kafka.stream.rule_stream_id", "rule"));
        ClusterMetadata clusterMeta = metadataService.getClusterMetadataByStreamId(ruleStreamId);

        Properties props = new Properties();
        String topic = ConfigLoader.getString("kafka.topics.rule_definitions", "rule_definitions");

        // 2. Load properties từ Postgres (nếu có), hoặc Fallback về KafkaClusterConfig
        if (clusterMeta != null && clusterMeta.getProperties() != null && !clusterMeta.getProperties().isEmpty()) {
            LOG.info("Found cluster config for stream_id='{}' from PostgreSQL", ruleStreamId);
            props.putAll(clusterMeta.getProperties());
            if (clusterMeta.getTopics() != null && !clusterMeta.getTopics().isEmpty()) {
                topic = clusterMeta.getTopics().iterator().next();
            }
        } else {
            LOG.warn("stream_id='{}' not found in DB. Falling back to KafkaClusterConfig (from args)", ruleStreamId);
            props = KafkaClusterConfig.getConsumerProperties(params, "rule", KafkaClusterConfig.CLUSTER_PLAIN);
            topic = params.get("rule.topic", topic);
        }

        // Đảm bảo các cấu hình cơ bản cho Consumer hoạt động
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "rule-compiler-test-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        LOG.info("Bootstrap Servers: {}", props.getProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
        LOG.info("Security Protocol: {}", props.getProperty("security.protocol", "PLAINTEXT"));
        LOG.info("Topic: {}", topic);

        // Khởi tạo Compiler và Consumer
        RuleCompiler compiler = new RuleCompiler();
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(topic));

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    LOG.info("---------------------------------------------------");
                    LOG.info("[NEW RULE RECEIVED] From Kafka: {}", record.value());

                    try {
                        RuleCompiler.CdcRuleEvent cdcEvent = compiler.parseCdcEvent(record.value());

                        if (compiler.isDelete(cdcEvent)) {
                            LOG.info("[DELETE/DISABLE] Rule event: {}", cdcEvent.ruleId());
                            continue;
                        }

                        CompiledRuleEnvelope compiledRule = compiler.compile(cdcEvent, -1);
                        LOG.info("[SUCCESS] Parse & Compile rule:");
                        LOG.info("     - Rule ID:   {}", compiledRule.getRuleId());
                        LOG.info("     - Rule Name: {}", compiledRule.getRuleName());
                        LOG.info("     - Rule Type: {}", compiledRule.getRuleType());
                        LOG.info("     - Triggers:  {}", compiledRule.getTriggers());
                    } catch (Exception e) {
                        LOG.error("[ERROR PARSE/COMPILE] Could not compile rule: {}", e.getMessage(), e);
                    }
                }
            }
        } finally {
            consumer.close();
            metadataService.close();
        }
    }
}
