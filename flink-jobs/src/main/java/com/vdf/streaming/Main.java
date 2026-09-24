package com.vdf.streaming;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.vdf.streaming.compiler.RuleCompiler;
import com.vdf.streaming.index.InvertedIndexManager;
import com.vdf.streaming.models.CompiledRuleEnvelope;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.roaringbitmap.RoaringBitmap;

import java.util.Map;

public class Main {

    public static void main(String[] args) throws Exception {
        System.out.println("=== Khởi động Flink Job: Stateful Streaming (Test Kafka) ===");
        
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Thay đổi bootstrap servers và topic tương ứng với môi trường Kafka của bạn
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("test_events")
                .setGroupId("flink-test-group")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source")
                .map(new RuleMatchingMapper())
                .print();

        env.execute("Test Kafka Rule Matcher");
    }

    public static class RuleMatchingMapper extends RichMapFunction<String, String> {
        private transient RuleCompiler compiler;
        private transient InvertedIndexManager indexManager;
        private transient ObjectMapper mapper;

        @Override
        public void open(Configuration parameters) throws Exception {
            compiler = new RuleCompiler();
            indexManager = new InvertedIndexManager();
            mapper = new ObjectMapper();

            // Khởi tạo Rule
            String ruleJson = "{"
                    + "\"rule_id\": \"R_TOPUP_001\","
                    + "\"rule_name\": \"Cảnh báo Topup bất thường\","
                    + "\"rule_version\": \"v1.0\","
                    + "\"trigger_criteria\": [{"
                    + "    \"source\": \"CPM\","
                    + "    \"schema_version\": \"v2\","
                    + "    \"key_field\": \"msisdn\","
                    + "    \"event_time_field\": \"timestamp\","
                    + "    \"conditions\": [["
                    + "        {\"op\": \"==\", \"value\": \"TOPUP\", \"field\": \"serviceCode\"},"
                    + "        {\"op\": \">\", \"value\": \"500000\", \"field\": \"amount\"}"
                    + "    ]]"
                    + "}],"
                    + "\"condition_tree\": null"
                    + "}";

            String cdcJson = "{"
                    + "\"rule_id\": \"R_TOPUP_001\","
                    + "\"name\": \"Cảnh báo Topup\","
                    + "\"rule_json\": \"" + ruleJson.replace("\"", "\\\"") + "\","
                    + "\"cooldown_seconds\": 300,"
                    + "\"version\": 1,"
                    + "\"enabled\": true,"
                    + "\"__deleted\": \"false\","
                    + "\"__op\": \"c\""
                    + "}";

            RuleCompiler.CdcRuleEvent cdcEvent = compiler.parseCdcEvent(cdcJson);
            CompiledRuleEnvelope compiledRule = compiler.compile(cdcEvent, -1);
            indexManager.registerRule(compiledRule);
            System.out.println("Đã load rule vào bộ nhớ: " + compiledRule.getRuleName());
        }

        @Override
        public String map(String jsonEvent) throws Exception {
            try {
                @SuppressWarnings("unchecked")
                Map<String, Object> eventFields = mapper.readValue(jsonEvent, Map.class);
                
                // Mặc định source là CPM, schema là v2 để test
                RoaringBitmap candidateSlots = indexManager.findCandidateRules("CPM", "v2", eventFields);
                
                if (candidateSlots.isEmpty()) {
                    return "Event [" + jsonEvent + "] -> KHÔNG KHỚP (Bị loại ngay bởi Inverted Index).";
                } else {
                    int slot = candidateSlots.first();
                    CompiledRuleEnvelope rule = indexManager.getRule(slot);
                    
                    // Do chưa có thư mục evaluators nên ta dùng code Java if/else cơ bản 
                    // để mô phỏng ConditionTreeEvaluator (thẩm định chính xác DNF)
                    boolean exactMatch = "TOPUP".equals(eventFields.get("serviceCode"));
                    boolean complexMatch = false;
                    
                    if (eventFields.containsKey("amount")) {
                        double amount = Double.parseDouble(eventFields.get("amount").toString());
                        complexMatch = amount > 500000;
                    }

                    if (exactMatch && complexMatch) {
                        return "Event [" + jsonEvent + "] -> [THÀNH CÔNG] Khớp hoàn toàn Rule: " + rule.getRuleId();
                    } else {
                        return "Event [" + jsonEvent + "] -> [THẤT BẠI] Là Ứng viên nhưng bị loại ở bước Evaluator do chưa đủ điều kiện AND.";
                    }
                }
            } catch (Exception e) {
                return "Lỗi parse event: " + jsonEvent;
            }
        }
    }
}