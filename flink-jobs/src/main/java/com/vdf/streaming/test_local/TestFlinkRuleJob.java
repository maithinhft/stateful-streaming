package com.vdf.streaming.test_local;

import com.vdf.streaming.compiler.RuleCompiler;
import com.vdf.streaming.config.KafkaClusterConfig;
import com.vdf.streaming.dynamic.metadata.PostgresKafkaMetadataService;
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

import java.util.Properties;

/**
 * Một Flink Job hoàn chỉnh dùng để nạp lên Flink Cluster (Docker).
 * Mục tiêu: Nối vào topic Kafka, parse rule CDC và ghi log thẳng ra console của TaskManager.
 */
public class TestFlinkRuleJob {
    public static void main(String[] args) throws Exception {
        System.out.println("=== Khởi tạo Flink Job: Test Rule Compiler ===");
        
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ParameterTool params = ParameterTool.fromArgs(args);
        env.getConfig().setGlobalJobParameters(params);

        // Lấy config từ DB (Mặc định trong Docker, tên miền 'postgres' và 'kafka-plain' tự động hiểu)
        String pgUrl = params.get("postgres.url", "jdbc:postgresql://postgres:5432/realtime_core");
        String pgUser = params.get("postgres.user", "postgres");
        String pgPassword = params.get("postgres.password", "postgres");
        
        PostgresKafkaMetadataService metadataService = new PostgresKafkaMetadataService(
                pgUrl, pgUser, pgPassword, "kafka_stream", 5000L);
        
        ClusterMetadata clusterMeta = metadataService.getClusterMetadataByStreamId("rule");
        Properties props = new Properties();
        String topic = "rule_definitions";
        
        if (clusterMeta != null && clusterMeta.getProperties() != null && !clusterMeta.getProperties().isEmpty()) {
            props.putAll(clusterMeta.getProperties());
            if (clusterMeta.getTopics() != null && !clusterMeta.getTopics().isEmpty()) {
                topic = clusterMeta.getTopics().iterator().next();
            }
        } else {
            props = KafkaClusterConfig.getConsumerProperties(params, "rule", KafkaClusterConfig.CLUSTER_PLAIN);
            topic = params.get("rule.topic", topic);
        }

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setProperties(props)
                .setTopics(topic)
                .setGroupId("flink-rule-test-group")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Rule Source")
           .process(new ProcessFunction<String, String>() {
               private transient RuleCompiler compiler;
               private transient com.vdf.streaming.index.InvertedIndexManager indexManager;

               @Override
               public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
                   compiler = new RuleCompiler();
                   indexManager = new com.vdf.streaming.index.InvertedIndexManager();
               }

               @Override
               public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
                   System.out.println("\n[FLINK-TASKMANAGER NHẬN RULE]: " + value);
                   try {
                       RuleCompiler.CdcRuleEvent cdcEvent = compiler.parseCdcEvent(value);
                       if (compiler.isDelete(cdcEvent)) {
                           System.out.println(" => Sự kiện XÓA/VÔ HIỆU HÓA Rule: " + cdcEvent.ruleId());
                           indexManager.unregisterRule(cdcEvent.ruleId());
                       } else {
                           CompiledRuleEnvelope rule = compiler.compile(cdcEvent, -1);
                           System.out.println(" => [THÀNH CÔNG] Parse rule hợp lệ: " + rule.getRuleName());
                           
                           // Đăng ký hoặc Update rule vào Inverted Index
                           if (cdcEvent.op() != null && cdcEvent.op().equals("u")) {
                               indexManager.updateRule(rule);
                           } else {
                               indexManager.registerRule(rule);
                           }
                       }
                       
                       // In ra màn hình cấu trúc của Inverted Index hiện tại
                       indexManager.printDebugInfo();
                       
                   } catch (Exception e) {
                       System.err.println(" => [LỖI] Parse hoặc Index thất bại: " + e.getMessage());
                       e.printStackTrace();
                   }
               }
           })
           .name("Rule Parser Process");

        env.execute("Test Rule Compiler Job");
    }
}
