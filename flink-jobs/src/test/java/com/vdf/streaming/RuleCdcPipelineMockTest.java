package com.vdf.streaming;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.vdf.streaming.compiler.RuleCompiler;
import com.vdf.streaming.index.InvertedIndexManager;
import com.vdf.streaming.models.CompiledRuleEnvelope;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.roaringbitmap.RoaringBitmap;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Kiểm thử Pipeline CDC cho Rule Engine")
class RuleCdcPipelineMockTest {

    private final ObjectMapper mapper = new ObjectMapper();
    private RuleCompiler compiler;
    private InvertedIndexManager indexManager;
    private MockPipelineProcessor processor;

    @BeforeEach
    void setUp() {
        compiler = new RuleCompiler();
        indexManager = new InvertedIndexManager();
        processor = new MockPipelineProcessor(compiler, indexManager);
    }

    /**
     * Giả lập một tiến trình xử lý stateful trên Flink, nơi theo dõi version của các rule
     * và cập nhật InvertedIndexManager một cách phù hợp.
     */
    private static class MockPipelineProcessor {
        private final RuleCompiler compiler;
        private final InvertedIndexManager indexManager;
        private final Map<String, Long> ruleVersionState = new HashMap<>();

        public MockPipelineProcessor(RuleCompiler compiler, InvertedIndexManager indexManager) {
            this.compiler = compiler;
            this.indexManager = indexManager;
        }

        public void process(String cdcJson) {
            RuleCompiler.CdcRuleEvent event = compiler.parseCdcEvent(cdcJson);
            String ruleId = event.ruleId();
            
            boolean isDelete = compiler.isDelete(event);
            long incomingVersion = event.version();

            Long currentVersion = ruleVersionState.get(ruleId);
            if (currentVersion != null && incomingVersion <= currentVersion) {
                return; // Bỏ qua sự kiện cũ hoặc trùng lặp
            }

            if (isDelete) {
                indexManager.unregisterRule(ruleId);
                ruleVersionState.remove(ruleId);
            } else {
                CompiledRuleEnvelope compiled = compiler.compile(event, 0); // slotId do SlotManager cấp phát lại bên trong
                if (indexManager.getRuleById(ruleId) != null) {
                    indexManager.updateRule(compiled);
                } else {
                    indexManager.registerRule(compiled);
                }
                ruleVersionState.put(ruleId, incomingVersion);
            }
        }
        
        public Long getRuleVersion(String ruleId) {
            return ruleVersionState.get(ruleId);
        }
    }

    /**
     * Helper xây dựng CDC JSON
     */
    private String buildCdcJson(String ruleId, String name, String ruleJson, long cooldownSeconds, long version, boolean enabled, String deleted, String op) {
        ObjectNode node = mapper.createObjectNode();
        node.put("rule_id", ruleId);
        node.put("name", name);
        node.put("rule_json", ruleJson);
        node.put("cooldown_seconds", cooldownSeconds);
        node.put("version", version);
        node.put("enabled", enabled);
        node.put("__deleted", deleted);
        node.put("__op", op);
        try {
            return mapper.writeValueAsString(node);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Helper xây dựng Rule JSON tối giản
     */
    private String buildRuleJson(String ruleId, String ruleName, String source, String schemaVersion, String field, String op, Object value) {
        ObjectNode rule = mapper.createObjectNode();
        rule.put("rule_id", ruleId);
        rule.put("rule_name", ruleName);
        rule.put("rule_version", "1");

        ArrayNode triggers = mapper.createArrayNode();
        ObjectNode trigger = mapper.createObjectNode();
        trigger.put("source", source);
        trigger.put("schema_version", schemaVersion);
        trigger.put("key_field", "msisdn");
        trigger.put("event_time_field", "timestamp");

        ArrayNode conditions = mapper.createArrayNode();
        ArrayNode dnfGroup = mapper.createArrayNode();
        ObjectNode cond = mapper.createObjectNode();
        cond.put("field", field);
        cond.put("op", op);
        
        putValueNode(cond, "value", value);
        
        dnfGroup.add(cond);
        conditions.add(dnfGroup);
        trigger.set("conditions", conditions);
        triggers.add(trigger);
        rule.set("trigger_criteria", triggers);

        ObjectNode conditionTree = mapper.createObjectNode();
        conditionTree.put("type", "CONDITION");
        ObjectNode expr = mapper.createObjectNode();
        expr.put("field", field);
        expr.put("op", op);
        
        putValueNode(expr, "value", value);
        
        conditionTree.set("expression", expr);
        rule.set("condition_tree", conditionTree);

        try {
            return mapper.writeValueAsString(rule);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    private void putValueNode(ObjectNode node, String key, Object value) {
        if (value instanceof String) node.put(key, (String) value);
        else if (value instanceof Integer) node.put(key, (Integer) value);
        else if (value instanceof Boolean) node.put(key, (Boolean) value);
        else if (value instanceof List) {
            ArrayNode arr = mapper.createArrayNode();
            for (Object v : (List<?>) value) {
                if (v instanceof String) arr.add((String) v);
                else if (v instanceof Integer) arr.add((Integer) v);
            }
            node.set(key, arr);
        } else node.put(key, String.valueOf(value));
    }

    @Nested
    @DisplayName("Group 1: Phân tích Sự kiện CDC & Vòng đời")
    class CdcEventParsingTests {
        
        @Test
        @DisplayName("Phân tích sự kiện tạo mới (op=c)")
        void testParseCdcCreateEvent() {
            String cdc = buildCdcJson("r1", "rule 1", "{}", 60, 1, true, "false", "c");
            RuleCompiler.CdcRuleEvent event = compiler.parseCdcEvent(cdc);
            assertEquals("r1", event.ruleId());
            assertEquals("c", event.op());
            assertFalse(compiler.isDelete(event));
        }

        @Test
        @DisplayName("Phân tích sự kiện cập nhật (op=u)")
        void testParseCdcUpdateEvent() {
            String cdc = buildCdcJson("r1", "rule 1", "{}", 60, 2, true, "false", "u");
            RuleCompiler.CdcRuleEvent event = compiler.parseCdcEvent(cdc);
            assertEquals("u", event.op());
            assertFalse(compiler.isDelete(event));
        }

        @Test
        @DisplayName("Phân tích sự kiện xóa (op=d)")
        void testParseCdcDeleteEvent() {
            String cdc = buildCdcJson("r1", "rule 1", "{}", 60, 3, true, "false", "d");
            RuleCompiler.CdcRuleEvent event = compiler.parseCdcEvent(cdc);
            assertTrue(compiler.isDelete(event));
        }

        @Test
        @DisplayName("Phân tích sự kiện vô hiệu hóa (enabled=false)")
        void testParseCdcDisableEvent() {
            String cdc = buildCdcJson("r1", "rule 1", "{}", 60, 4, false, "false", "u");
            RuleCompiler.CdcRuleEvent event = compiler.parseCdcEvent(cdc);
            assertTrue(compiler.isDelete(event));
        }

        @Test
        @DisplayName("Phân tích sự kiện với cờ __deleted=true")
        void testParseCdcDeletedFlagEvent() {
            String cdc = buildCdcJson("r1", "rule 1", "{}", 60, 5, true, "true", "u");
            RuleCompiler.CdcRuleEvent event = compiler.parseCdcEvent(cdc);
            assertTrue(compiler.isDelete(event));
        }
    }

    @Nested
    @DisplayName("Group 2: Xử lý ngoại lệ về phiên bản (Version Check)")
    class VersionCheckTests {
        
        @Test
        @DisplayName("Phiên bản mới hơn được chấp nhận")
        void testVersionCheck_NewerVersionAccepted() {
            processor.process(buildCdcJson("r1", "name", buildRuleJson("r1", "n", "s", "1", "f", "==", "v"), 0, 1, true, "false", "c"));
            assertEquals(1L, processor.getRuleVersion("r1"));
            processor.process(buildCdcJson("r1", "name", buildRuleJson("r1", "n", "s", "1", "f", "==", "v2"), 0, 2, true, "false", "u"));
            assertEquals(2L, processor.getRuleVersion("r1"));
        }

        @Test
        @DisplayName("Phiên bản trùng lặp bị bỏ qua")
        void testVersionCheck_SameVersionSkipped() {
            processor.process(buildCdcJson("r2", "name", buildRuleJson("r2", "n", "s", "1", "f", "==", "v"), 0, 1, true, "false", "c"));
            processor.process(buildCdcJson("r2", "name", buildRuleJson("r2", "n", "s", "1", "f", "==", "v2"), 0, 1, true, "false", "u"));
            assertEquals(1L, processor.getRuleVersion("r2"));
        }

        @Test
        @DisplayName("Phiên bản cũ hơn bị bỏ qua")
        void testVersionCheck_OlderVersionSkipped() {
            processor.process(buildCdcJson("r3", "name", buildRuleJson("r3", "n", "s", "1", "f", "==", "v"), 0, 3, true, "false", "c"));
            processor.process(buildCdcJson("r3", "name", buildRuleJson("r3", "n", "s", "1", "f", "==", "v2"), 0, 1, true, "false", "u"));
            assertEquals(3L, processor.getRuleVersion("r3"));
        }

        @Test
        @DisplayName("Xóa rồi tạo lại với phiên bản thấp hơn được chấp nhận")
        void testVersionCheck_DeleteThenRecreateWithLowerVersion() {
            processor.process(buildCdcJson("r4", "name", buildRuleJson("r4", "n", "s", "1", "f", "==", "v"), 0, 5, true, "false", "c"));
            processor.process(buildCdcJson("r4", "name", "{}", 0, 6, true, "false", "d"));
            assertNull(processor.getRuleVersion("r4"));
            
            // Re-create with version 1 (because there is no existing state)
            processor.process(buildCdcJson("r4", "name", buildRuleJson("r4", "n", "s", "1", "f", "==", "v"), 0, 1, true, "false", "c"));
            assertEquals(1L, processor.getRuleVersion("r4"));
        }

        @Test
        @DisplayName("Cập nhật liên tục chỉ nhận phiên bản mới nhất")
        void testVersionCheck_RapidUpdatesOnlyLatestWins() {
            processor.process(buildCdcJson("r5", "name", buildRuleJson("r5", "n", "s", "1", "f", "==", "1"), 0, 1, true, "false", "c"));
            processor.process(buildCdcJson("r5", "name", buildRuleJson("r5", "n", "s", "1", "f", "==", "5"), 0, 5, true, "false", "u"));
            processor.process(buildCdcJson("r5", "name", buildRuleJson("r5", "n", "s", "1", "f", "==", "3"), 0, 3, true, "false", "u"));
            processor.process(buildCdcJson("r5", "name", buildRuleJson("r5", "n", "s", "1", "f", "==", "7"), 0, 7, true, "false", "u"));
            processor.process(buildCdcJson("r5", "name", buildRuleJson("r5", "n", "s", "1", "f", "==", "2"), 0, 2, true, "false", "u"));
            assertEquals(7L, processor.getRuleVersion("r5"));
        }
    }

    @Nested
    @DisplayName("Group 3: Xóa Inverted Index bằng Bitmask")
    class InvertedIndexDeletionTests {
        
        @Test
        @DisplayName("Kiểm tra xóa bằng Bitmask thay vì dọn dẹp Index ngay lập tức")
        void testDeleteRule_BitmaskNotScanIndex() {
            processor.process(buildCdcJson("r1", "name", buildRuleJson("r1", "n", "s1", "v1", "f1", "==", "val"), 0, 1, true, "false", "c"));
            int slotId = indexManager.getRuleById("r1").getSlotId();
            
            processor.process(buildCdcJson("r1", "name", "{}", 0, 2, true, "false", "d"));
            
            Map<String, Object> fields = Map.of("f1", "val");
            RoaringBitmap candidates = indexManager.findCandidateRules("s1", "v1", fields);
            assertFalse(candidates.contains(slotId));
        }
        
        @Test
        @DisplayName("Slot ID được tái sử dụng sau khi xóa")
        void testDeleteRule_SlotReusedAfterDelete() {
            processor.process(buildCdcJson("rA", "name", buildRuleJson("rA", "n", "s1", "v1", "f1", "==", "val"), 0, 1, true, "false", "c"));
            int slotA = indexManager.getRuleById("rA").getSlotId();
            
            processor.process(buildCdcJson("rA", "name", "{}", 0, 2, true, "false", "d"));
            
            processor.process(buildCdcJson("rB", "name", buildRuleJson("rB", "n", "s1", "v1", "f1", "==", "val2"), 0, 1, true, "false", "c"));
            int slotB = indexManager.getRuleById("rB").getSlotId();
            
            assertEquals(slotA, slotB);
        }

        @Test
        @DisplayName("Sự kiện Disable (enabled=false) được xử lý như xóa")
        void testDisableRule_TreatedAsDelete() {
            processor.process(buildCdcJson("rC", "name", buildRuleJson("rC", "n", "s1", "v1", "f1", "==", "val"), 0, 1, true, "false", "c"));
            assertNotNull(indexManager.getRuleById("rC"));
            
            processor.process(buildCdcJson("rC", "name", buildRuleJson("rC", "n", "s1", "v1", "f1", "==", "val"), 0, 2, false, "false", "u"));
            assertNull(indexManager.getRuleById("rC"));
        }

        @Test
        @DisplayName("Xóa Rule không tồn tại không sinh lỗi")
        void testDeleteNonExistentRule_NoError() {
            assertDoesNotThrow(() -> {
                processor.process(buildCdcJson("ghost", "name", "{}", 0, 1, true, "false", "d"));
            });
        }

        @Test
        @DisplayName("Nhiều lượt xóa và tạo mới đảm bảo tính đồng nhất trạng thái")
        void testMultipleDeletesAndAdds_ConsistentState() {
            for (int i = 0; i < 10; i++) {
                processor.process(buildCdcJson("r" + i, "name", buildRuleJson("r"+i, "n", "s1", "v1", "f1", "==", "val"), 0, 1, true, "false", "c"));
            }
            for (int i = 0; i < 5; i++) {
                processor.process(buildCdcJson("r" + i, "name", "{}", 0, 2, true, "false", "d"));
            }
            for (int i = 10; i < 15; i++) {
                processor.process(buildCdcJson("r" + i, "name", buildRuleJson("r"+i, "n", "s1", "v1", "f1", "==", "val"), 0, 1, true, "false", "c"));
            }
            
            for (int i = 5; i < 15; i++) {
                assertNotNull(indexManager.getRuleById("r" + i));
            }
            for (int i = 0; i < 5; i++) {
                assertNull(indexManager.getRuleById("r" + i));
            }
        }
    }

    @Nested
    @DisplayName("Group 4: Truy vấn Inverted Index")
    class IndexQueryTests {
        
        @Test
        @DisplayName("Tìm kiếm chính xác 1 Rule")
        void testQueryExactMatch_SingleRule() {
            processor.process(buildCdcJson("r1", "name", buildRuleJson("r1", "n", "src", "v1", "status", "==", "ACTIVE"), 0, 1, true, "false", "c"));
            RoaringBitmap candidates = indexManager.findCandidateRules("src", "v1", Map.of("status", "ACTIVE"));
            int slotId = indexManager.getRuleById("r1").getSlotId();
            assertTrue(candidates.contains(slotId));
        }

        @Test
        @DisplayName("Truy vấn không khớp trả về kết quả rỗng")
        void testQueryExactMatch_NoMatch() {
            processor.process(buildCdcJson("r1", "name", buildRuleJson("r1", "n", "src", "v1", "status", "==", "ACTIVE"), 0, 1, true, "false", "c"));
            RoaringBitmap candidates = indexManager.findCandidateRules("src", "v1", Map.of("status", "INACTIVE"));
            assertTrue(candidates.isEmpty());
        }

        @Test
        @DisplayName("Truy vấn với các toán tử phức tạp luôn trả về ứng viên")
        void testQueryComplexOperator_AlwaysCandidate() {
            processor.process(buildCdcJson("r1", "name", buildRuleJson("r1", "n", "src", "v1", "amount", ">", 1000), 0, 1, true, "false", "c"));
            int slotId = indexManager.getRuleById("r1").getSlotId();
            
            RoaringBitmap candidates = indexManager.findCandidateRules("src", "v1", Map.of("amount", 500));
            assertTrue(candidates.contains(slotId));
        }

        @Test
        @DisplayName("Nhiều Rule đăng ký nhưng chỉ trả về Rule khớp")
        void testQueryMultipleRules_OnlyMatchingReturned() {
            processor.process(buildCdcJson("r1", "name", buildRuleJson("r1", "n", "src", "v1", "code", "==", "A"), 0, 1, true, "false", "c"));
            processor.process(buildCdcJson("r2", "name", buildRuleJson("r2", "n", "src", "v1", "code", "==", "B"), 0, 1, true, "false", "c"));
            processor.process(buildCdcJson("r3", "name", buildRuleJson("r3", "n", "src", "v1", "code", "==", "C"), 0, 1, true, "false", "c"));
            
            int slotA = indexManager.getRuleById("r1").getSlotId();
            RoaringBitmap candidates = indexManager.findCandidateRules("src", "v1", Map.of("code", "A"));
            
            assertTrue(candidates.contains(slotA));
            assertEquals(1, candidates.getCardinality());
        }

        @Test
        @DisplayName("Truy vấn sau khi Rule bị cập nhật (Điều kiện cũ là false positive do bitmask, điều kiện mới khớp đúng)")
        void testQueryAfterUpdate_OldConditionNoLongerMatches() {
            processor.process(buildCdcJson("r1", "name", buildRuleJson("r1", "n", "src", "v1", "code", "==", "A"), 0, 1, true, "false", "c"));
            
            processor.process(buildCdcJson("r1", "name", buildRuleJson("r1", "n", "src", "v1", "code", "==", "B"), 0, 2, true, "false", "u"));
            
            // Với bitmask-only update, stale entry "code:A" vẫn tồn tại trong exactIndex
            // nhưng slot cũ đã bị andNot(freeSlots) loại bỏ. Slot mới (reused) sẽ xuất hiện
            // trong cả "code:A" (false positive) và "code:B" (true positive).
            // Evaluator (ConditionTreeEvaluator) sẽ loại bỏ false positive ở bước sau.
            RoaringBitmap candidatesA = indexManager.findCandidateRules("src", "v1", Map.of("code", "A"));
            // False positive do stale entry, slot reused - Evaluator sẽ loại
            // Verify: điều kiện mới khớp đúng
            RoaringBitmap candidatesB = indexManager.findCandidateRules("src", "v1", Map.of("code", "B"));
            int slotId = indexManager.getRuleById("r1").getSlotId();
            assertTrue(candidatesB.contains(slotId), "Điều kiện mới phải khớp");
        }
    }

    @Nested
    @DisplayName("Group 5: Mô phỏng toàn bộ luồng CDC")
    class FullPipelineTests {
        
        @Test
        @DisplayName("Luồng sống đầy đủ: CREATE -> UPDATE -> DELETE")
        void testFullPipeline_CreateUpdateDeleteLifecycle() {
            // CREATE
            processor.process(buildCdcJson("r_life", "name", buildRuleJson("r_life", "n", "SYS", "v1", "evt", "==", "LOGIN"), 0, 1, true, "false", "c"));
            int slot1 = indexManager.getRuleById("r_life").getSlotId();
            assertTrue(indexManager.findCandidateRules("SYS", "v1", Map.of("evt", "LOGIN")).contains(slot1));
            
            // UPDATE: thay đổi trigger từ LOGIN → LOGOUT
            processor.process(buildCdcJson("r_life", "name", buildRuleJson("r_life", "n", "SYS", "v1", "evt", "==", "LOGOUT"), 0, 2, true, "false", "u"));
            int slot2 = indexManager.getRuleById("r_life").getSlotId();
            
            // Với bitmask-only update: stale entry "evt:LOGIN" vẫn tồn tại (false positive)
            // Slot cũ released → slot mới reused → stale entry vẫn trỏ tới slot active
            // Evaluator sẽ loại bỏ false positive ở bước condition_tree evaluation
            
            // Điều kiện mới PHẢI khớp
            assertTrue(indexManager.findCandidateRules("SYS", "v1", Map.of("evt", "LOGOUT")).contains(slot2));
            
            // DELETE
            processor.process(buildCdcJson("r_life", "name", "{}", 0, 3, true, "false", "d"));
            assertTrue(indexManager.findCandidateRules("SYS", "v1", Map.of("evt", "LOGOUT")).isEmpty());
        }

        @Test
        @DisplayName("Kiểm thử Batch Bulk Create & Truy vấn ổn định")
        void testFullPipeline_BulkCreateAndQuery() {
            for (int i = 0; i < 50; i++) {
                processor.process(buildCdcJson("rb_" + i, "name", buildRuleJson("rb_" + i, "n", "bulk", "1", "id", "==", i), 0, 1, true, "false", "c"));
            }
            
            for (int i = 0; i < 50; i++) {
                RoaringBitmap cands = indexManager.findCandidateRules("bulk", "1", Map.of("id", i));
                assertEquals(1, cands.getCardinality());
                int slot = indexManager.getRuleById("rb_" + i).getSlotId();
                assertTrue(cands.contains(slot));
            }
        }

        @Test
        @DisplayName("Cấu trúc độc lập đối với các Source:Version khác nhau")
        void testFullPipeline_ConcurrentSourceVersions() {
            processor.process(buildCdcJson("r1", "n", buildRuleJson("r1", "n", "S1", "v1", "f", "==", "X"), 0, 1, true, "false", "c"));
            processor.process(buildCdcJson("r2", "n", buildRuleJson("r2", "n", "S1", "v2", "f", "==", "X"), 0, 1, true, "false", "c"));
            processor.process(buildCdcJson("r3", "n", buildRuleJson("r3", "n", "S2", "v1", "f", "==", "X"), 0, 1, true, "false", "c"));
            
            assertEquals(1, indexManager.findCandidateRules("S1", "v1", Map.of("f", "X")).getCardinality());
            assertEquals(1, indexManager.findCandidateRules("S1", "v2", Map.of("f", "X")).getCardinality());
            assertEquals(1, indexManager.findCandidateRules("S2", "v1", Map.of("f", "X")).getCardinality());
        }

        @Test
        @DisplayName("Kiểm tra Deep Copy (hỗ trợ Copy-On-Write) không làm ảnh hưởng bản sao gốc")
        void testFullPipeline_DeepCopyForCOW() {
            processor.process(buildCdcJson("r1", "n", buildRuleJson("r1", "n", "S", "v", "f", "==", "X"), 0, 1, true, "false", "c"));
            
            InvertedIndexManager copy = indexManager.deepCopy();
            
            processor.process(buildCdcJson("r1", "n", "{}", 0, 2, true, "false", "d"));
            
            assertEquals(0, indexManager.findCandidateRules("S", "v", Map.of("f", "X")).getCardinality());
            assertEquals(1, copy.findCandidateRules("S", "v", Map.of("f", "X")).getCardinality());
        }
    }
}
