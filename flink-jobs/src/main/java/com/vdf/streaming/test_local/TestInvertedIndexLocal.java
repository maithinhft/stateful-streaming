package com.vdf.streaming.test_local;

import com.vdf.streaming.compiler.RuleCompiler;
import com.vdf.streaming.index.InvertedIndexManager;
import com.vdf.streaming.models.CompiledRuleEnvelope;
import org.roaringbitmap.RoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class TestInvertedIndexLocal {
    private static final Logger LOG = LoggerFactory.getLogger(TestInvertedIndexLocal.class);

    public static void main(String[] args) {
        LOG.info("=== Bắt đầu test Inverted Index & Rule Compiler (Local Không có Kafka) ===");

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

            LOG.info("[1] Compiling rule...");
            RuleCompiler.CdcRuleEvent cdcEvent = compiler.parseCdcEvent(cdcJson);
            CompiledRuleEnvelope compiledRule = compiler.compile(cdcEvent, -1);

            LOG.info("[2] Registering rule into Inverted Index...");
            indexManager.registerRule(compiledRule);
            LOG.info("[2] Success: {}", compiledRule.getRuleName());

            LOG.info("[3] Firing matching event...");
            Map<String, Object> event1 = new HashMap<>();
            event1.put("serviceCode", "TOPUP");
            event1.put("amount", "1000000");

            RoaringBitmap candidate1 = indexManager.findCandidateRules("CPM", "v2", event1);
            if (!candidate1.isEmpty()) {
                LOG.info("[SUCCESS] Event 1 passed into Candidates: {}", candidate1);
            } else {
                LOG.warn("[MISS] Event 1 did not match any candidate rule.");
            }

        } catch (Exception e) {
            LOG.error("Test failed with exception", e);
        }
    }
}
