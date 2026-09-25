package com.vdf.streaming.test_local;

import com.vdf.streaming.compiler.RuleCompiler;
import com.vdf.streaming.config.ConfigLoader;
import com.vdf.streaming.config.KafkaClusterConfig;
import com.vdf.streaming.dynamic.metadata.PostgresKafkaMetadataService;
import com.vdf.streaming.index.InvertedIndexManager;
import com.vdf.streaming.models.CompiledRuleEnvelope;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.dynamic.metadata.ClusterMetadata;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Một Flink Job hoàn chỉnh dùng để nạp lên Flink Cluster (Docker).
 * Mục tiêu: Nối vào topic Kafka, parse rule CDC và ghi log thẳng ra console của TaskManager.
 */
public class TestFlinkRuleJob {
    private static final Logger LOG = LoggerFactory.getLogger(TestFlinkRuleJob.class);

    public static void main(String[] args) throws Exception {
        LOG.info("=== Khởi tạo Flink Job: Test Rule Compiler ===");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ParameterTool params = ParameterTool.fromArgs(args);
        env.getConfig().setGlobalJobParameters(params);

        // Lấy config từ YAML (ưu tiên), fallback về params/defaults
        String pgUrl      = params.get("postgres.url",      ConfigLoader.getString("postgres.url", "jdbc:postgresql://postgres:5432/realtime_core"));
        String pgUser     = params.get("postgres.user",     ConfigLoader.getString("postgres.user", "postgres"));
        String pgPassword = params.get("postgres.password", ConfigLoader.getString("postgres.password", ""));
        String tablePrefix = params.get("postgres.table.prefix", ConfigLoader.getString("postgres.table_prefix", "kafka_stream"));

        LOG.info("Connecting to PostgreSQL: {}", pgUrl);
        PostgresKafkaMetadataService metadataService = new PostgresKafkaMetadataService(
                pgUrl, pgUser, pgPassword, tablePrefix, 5000L);

        String ruleStreamId = params.get("rule.stream.id", ConfigLoader.getString("kafka.stream.rule_stream_id", "rule"));
        ClusterMetadata clusterMeta = metadataService.getClusterMetadataByStreamId(ruleStreamId);

        Properties props = new Properties();
        String topic = ConfigLoader.getString("kafka.topics.rule_definitions", "rule_definitions");

        if (clusterMeta != null && clusterMeta.getProperties() != null && !clusterMeta.getProperties().isEmpty()) {
            LOG.info("Found cluster config for stream_id='{}' from PostgreSQL", ruleStreamId);
            props.putAll(clusterMeta.getProperties());
            if (clusterMeta.getTopics() != null && !clusterMeta.getTopics().isEmpty()) {
                topic = clusterMeta.getTopics().iterator().next();
            }
        } else {
            LOG.warn("Stream_id='{}' not found in DB. Falling back to KafkaClusterConfig", ruleStreamId);
            props = KafkaClusterConfig.getConsumerProperties(params, "rule", KafkaClusterConfig.CLUSTER_PLAIN);
            topic = params.get("rule.topic", topic);
        }

        LOG.info("Subscribing to topic: {} | bootstrap: {}", topic, props.getProperty("bootstrap.servers", "N/A"));

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setProperties(props)
                .setTopics(topic)
                .setGroupId("flink-rule-test-group")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Rule Source")
           .process(new ProcessFunction<String, String>() {
               private static final Logger PLOG = LoggerFactory.getLogger("RuleParserProcess");
               private transient RuleCompiler compiler;
               private transient InvertedIndexManager indexManager;

               @Override
               public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
                   compiler = new RuleCompiler();
                   indexManager = new InvertedIndexManager();
               }

               @Override
               public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
                   PLOG.info("[FLINK-TASKMANAGER RECEIVED RULE]: {}", value);
                   try {
                       RuleCompiler.CdcRuleEvent cdcEvent = compiler.parseCdcEvent(value);
                       CompiledRuleEnvelope existingRule = indexManager.getRuleById(cdcEvent.ruleId());
                       if (existingRule != null && existingRule.getCdcVersion() >= cdcEvent.version()) {
                           PLOG.info("[SKIP] Rule {} already has newer or equal version (current: {}, received: {})",
                                   cdcEvent.ruleId(), existingRule.getCdcVersion(), cdcEvent.version());
                           return;
                       }
                       if (compiler.isDelete(cdcEvent)) {
                           PLOG.info("[DELETE/DISABLE] Rule event: {}", cdcEvent.ruleId());
                           indexManager.unregisterRule(cdcEvent.ruleId());
                       } else {
                           CompiledRuleEnvelope rule = compiler.compile(cdcEvent, -1);
                           PLOG.info("[SUCCESS] Compiled rule: {}", rule.getRuleName());

                           // Đăng ký hoặc Update rule vào Inverted Index
                           if (cdcEvent.op() != null && cdcEvent.op().equals("u")) {
                               indexManager.updateRule(rule);
                           } else {
                               indexManager.registerRule(rule);
                           }
                       }

                       // In ra cấu trúc của Inverted Index hiện tại (ở mức DEBUG)
                       indexManager.printDebugInfo();

                   } catch (Exception e) {
                       PLOG.error("[ERROR] Parse or Index failed: {}", e.getMessage(), e);
                   }
               }
           })
           .name("Rule Parser Process");

        env.execute("Test Rule Compiler Job");
    }
}
