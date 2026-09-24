package com.vdf.streaming.test_local;

import com.vdf.streaming.compiler.RuleCompiler;
import com.vdf.streaming.index.InvertedIndexManager;
import com.vdf.streaming.models.CompiledRuleEnvelope;
import org.roaringbitmap.RoaringBitmap;

import java.util.HashMap;
import java.util.Map;

public class TestInvertedIndexLocal {
    public static void main(String[] args) {
        System.out.println("=== Bắt đầu test Inverted Index & Rule Compiler (Local Không có Kafka) ===");

        try {
            RuleCompiler compiler = new RuleCompiler();
            InvertedIndexManager indexManager = new InvertedIndexManager();

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

            System.out.println("[1] Đang compile rule...");
            RuleCompiler.CdcRuleEvent cdcEvent = compiler.parseCdcEvent(cdcJson);
            CompiledRuleEnvelope compiledRule = compiler.compile(cdcEvent, -1);
            
            System.out.println("[2] Đăng ký rule vào Inverted Index...");
            indexManager.registerRule(compiledRule);
            System.out.println(" => Thành công: " + compiledRule.getRuleName());

            System.out.println("\n[3] Bắn Event Khớp...");
            Map<String, Object> event1 = new HashMap<>();
            event1.put("serviceCode", "TOPUP");
            event1.put("amount", "1000000");

            RoaringBitmap candidate1 = indexManager.findCandidateRules("CPM", "v2", event1);
            if (!candidate1.isEmpty()) {
                System.out.println(" => [THÀNH CÔNG] Event 1 lọt vào Candidates.");
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
