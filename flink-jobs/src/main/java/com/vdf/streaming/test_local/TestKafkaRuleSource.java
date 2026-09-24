package com.vdf.streaming.test_local;

import com.vdf.streaming.compiler.RuleCompiler;
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

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class TestKafkaRuleSource {
    public static void main(String[] args) {
        System.out.println("=== Khởi động Kafka Consumer (Dynamic Metadata) để parse Rule ===");

        ParameterTool params = ParameterTool.fromArgs(args);

        // 1. Cấu hình kết nối DB lấy metadata động
        String pgHost = params.get("postgres.host", "postgres");
        String pgPort = params.get("postgres.port", "5432");
        String pgDb = params.get("postgres.db", "realtime_core");
        String defaultPgUrl = String.format("jdbc:postgresql://%s:%s/%s", pgHost, pgPort, pgDb);
        String pgUrl = params.get("postgres.url", defaultPgUrl);
        String pgUser = params.get("postgres.user", "postgres");
        String pgPassword = params.get("postgres.password", "postgres");
        String tablePrefix = params.get("postgres.table.prefix", "kafka_stream");

        System.out.println("Đang kết nối PostgreSQL để lấy Dynamic Kafka Metadata: " + pgUrl);
        PostgresKafkaMetadataService metadataService = new PostgresKafkaMetadataService(
                pgUrl, pgUser, pgPassword, tablePrefix, 5000L);

        String ruleStreamId = params.get("rule.stream.id", "rule");
        ClusterMetadata clusterMeta = metadataService.getClusterMetadataByStreamId(ruleStreamId);

        Properties props = new Properties();
        String topic = "rule_definitions";

        // 2. Load properties từ Postgres (nếu có), hoặc Fallback về KafkaClusterConfig
        if (clusterMeta != null && clusterMeta.getProperties() != null && !clusterMeta.getProperties().isEmpty()) {
            System.out.println("=> Đã tìm thấy cấu hình cluster cho stream_id = '" + ruleStreamId + "' trong CSDL!");
            props.putAll(clusterMeta.getProperties());
            if (clusterMeta.getTopics() != null && !clusterMeta.getTopics().isEmpty()) {
                topic = clusterMeta.getTopics().iterator().next();
            }
        } else {
            System.out.println("=> KHÔNG tìm thấy stream_id = '" + ruleStreamId + "' trong CSDL. Fallback về KafkaClusterConfig (tham số truyền vào)...");
            props = KafkaClusterConfig.getConsumerProperties(params, "rule", KafkaClusterConfig.CLUSTER_PLAIN);
            topic = params.get("rule.topic", topic);
        }

        // Đảm bảo các cấu hình cơ bản cho Consumer hoạt động
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "rule-compiler-test-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        System.out.println("Sử dụng Bootstrap Servers: " + props.getProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
        System.out.println("Security Protocol: " + props.getProperty("security.protocol", "PLAINTEXT"));
        System.out.println("Topic sẽ lắng nghe: " + topic);

        // Khởi tạo Compiler và Consumer
        RuleCompiler compiler = new RuleCompiler();
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(topic));

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println("\n---------------------------------------------------");
                    System.out.println("[NHẬN RULE MỚI] Từ Kafka: " + record.value());
                    
                    try {
                        RuleCompiler.CdcRuleEvent cdcEvent = compiler.parseCdcEvent(record.value());
                        
                        if (compiler.isDelete(cdcEvent)) {
                            System.out.println(" => Sự kiện XÓA hoặc VÔ HIỆU HÓA Rule: " + cdcEvent.ruleId());
                            continue;
                        }

                        CompiledRuleEnvelope compiledRule = compiler.compile(cdcEvent, -1);
                        System.out.println(" => [THÀNH CÔNG] Parse & Compile rule hợp lệ!");
                        System.out.println("     - Rule ID:   " + compiledRule.getRuleId());
                        System.out.println("     - Rule Name: " + compiledRule.getRuleName());
                        System.out.println("     - Rule Type: " + compiledRule.getRuleType());
                        System.out.println("     - Triggers:  " + compiledRule.getTriggers());
                    } catch (Exception e) {
                        System.err.println(" => [LỖI PARSE/COMPILE] Không thể biên dịch rule này!");
                        e.printStackTrace();
                    }
                }
            }
        } finally {
            consumer.close();
            metadataService.close();
        }
    }
}
