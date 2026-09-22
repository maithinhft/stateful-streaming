package com.vdf.streaming.rule;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Sinh rule JSON ngẫu nhiên theo schema 04_RULE_SCHEMA.md.
 * Hỗ trợ đa dạng operators: so sánh số, chuỗi, boolean, timestamp,
 * IN, NOT IN, BETWEEN, STARTS_WITH, CONTAINS, ENDS_WITH, IS_NULL, IS_NOT_NULL, LENGTH.
 */
public class RuleGenerator {

    private final ObjectMapper mapper;
    private final List<String> sources;
    private final List<String> versions;
    private final int maxTreeHeight;
    private final int maxSourcesPerTrigger;
    private final Random random;

    // ---- Pool dữ liệu mẫu cho sinh ngẫu nhiên ----
    private static final List<String> RULE_NAME_POOL = List.of(
            "fraud_detection", "anomaly_check", "velocity_limit", "geo_fence_alert",
            "high_value_txn", "dormant_reactivation", "loyalty_upgrade", "risk_scoring",
            "channel_mismatch", "device_fingerprint", "session_timeout", "balance_threshold",
            "cross_border_check", "kyc_expiry", "otp_brute_force", "merchant_category_block",
            "refund_abuse", "account_takeover", "suspicious_ip", "spending_pattern"
    );

    private static final List<String> KEY_FIELDS = List.of("msisdn", "phone_number", "customer_id", "user_id", "account_id");
    private static final List<String> EVENT_TIME_FIELDS = List.of("timestamp", "event_time", "processing_time", "created_at");
    private static final List<String> USER_IDS = List.of("user_001", "user_002", "user_003", "admin_01", "system", "ops_team");

    // Các trường mẫu theo kiểu dữ liệu
    private static final List<String> INT_FIELDS = List.of("age", "login_attempts_count", "mcc_code", "retry_count", "session_count", "page_views");
    private static final List<String> DOUBLE_FIELDS = List.of("fraud_probability_score", "daily_spend_total_vnd", "transaction_amount", "balance_vnd", "monthly_income_vnd", "response_latency_ms");
    private static final List<String> STRING_FIELDS = List.of("login_channel", "device_type", "user_status", "province", "transaction_type", "payment_method", "msg_content", "end_point", "imei");
    private static final List<String> BOOLEAN_FIELDS = List.of("is_vip", "is_suspicious_ip", "is_2fa_enabled", "is_biometric_enabled", "is_first_login");
    private static final List<String> TIMESTAMP_FIELDS = List.of("account_created_date", "last_login_time", "last_transaction_time");

    // Giá trị mẫu cho STRING
    private static final List<String> STRING_VALUES = List.of(
            "MOBILE_APP", "WEB", "TABLET", "DESKTOP", "ACTIVE", "INACTIVE", "BLOCKED",
            "TRANSFER", "TOPUP", "PAYMENT", "CARD", "EWALLET", "BANK_TRANSFER"
    );

    private static final DateTimeFormatter ISO_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSXXX")
            .withZone(ZoneId.of("Asia/Ho_Chi_Minh"));

    public RuleGenerator(List<String> sources, List<String> versions, int maxTreeHeight, int maxSourcesPerTrigger) {
        this.mapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
        this.sources = sources;
        this.versions = versions;
        this.maxTreeHeight = maxTreeHeight;
        this.maxSourcesPerTrigger = maxSourcesPerTrigger;
        this.random = ThreadLocalRandom.current();
    }

    public ObjectMapper getMapper() {
        return mapper;
    }

    /**
     * Sinh 1 rule JSON hoàn chỉnh.
     *
     * @param index         chỉ số rule
     * @param baseTimestamp  timestamp gốc (epoch seconds), file name = baseTimestamp + index
     * @return ObjectNode chứa toàn bộ rule
     */
    public ObjectNode generateRule(int index, long baseTimestamp) {
        ObjectNode rule = mapper.createObjectNode();

        String ruleId = "rule_GEN_" + index;
        String ruleName = randomFrom(RULE_NAME_POOL) + "_" + index;
        String ruleVersion = String.valueOf(random.nextInt(1, 10));

        rule.put("rule_id", ruleId);
        rule.put("rule_name", ruleName);
        rule.put("rule_version", ruleVersion);

        // metadata
        ObjectNode metadata = mapper.createObjectNode();
        metadata.put("event_time", ISO_FORMATTER.format(Instant.ofEpochSecond(baseTimestamp + index)));
        metadata.put("user_id", randomFrom(USER_IDS));
        rule.set("metadata", metadata);

        // trigger_criteria
        rule.set("trigger_criteria", generateTriggerCriteria());

        // condition_tree
        rule.set("condition_tree", generateConditionTree(maxTreeHeight));

        return rule;
    }

    // =========================================================================
    // Trigger Criteria
    // =========================================================================

    private ArrayNode generateTriggerCriteria() {
        ArrayNode triggers = mapper.createArrayNode();

        // Random 1 -> maxSourcesPerTrigger sources (không lặp)
        int numSources = Math.min(random.nextInt(1, maxSourcesPerTrigger + 1), sources.size());
        List<String> shuffled = new ArrayList<>(sources);
        Collections.shuffle(shuffled, random);
        List<String> selectedSources = shuffled.subList(0, numSources);

        for (String source : selectedSources) {
            ObjectNode trigger = mapper.createObjectNode();
            trigger.put("source", source);
            trigger.put("schema_version", randomFrom(versions));
            trigger.put("key_field", randomFrom(KEY_FIELDS));
            trigger.put("event_time_field", randomFrom(EVENT_TIME_FIELDS));

            // conditions (DNF): 1-3 OR groups
            ArrayNode conditions = mapper.createArrayNode();
            int numOrGroups = random.nextInt(1, 4);
            for (int i = 0; i < numOrGroups; i++) {
                ArrayNode andGroup = mapper.createArrayNode();
                int numAndConds = random.nextInt(1, 4);
                for (int j = 0; j < numAndConds; j++) {
                    andGroup.add(generateSimpleTriggerCondition());
                }
                conditions.add(andGroup);
            }
            trigger.set("conditions", conditions);

            triggers.add(trigger);
        }

        return triggers;
    }

    /**
     * Sinh 1 condition đơn giản cho trigger (chỉ ==, !=, IN, NOT IN).
     */
    private ObjectNode generateSimpleTriggerCondition() {
        ObjectNode cond = mapper.createObjectNode();
        int choice = random.nextInt(4);
        switch (choice) {
            case 0 -> { // String ==
                cond.put("field", randomFrom(STRING_FIELDS));
                cond.put("op", "==");
                cond.put("value", randomFrom(STRING_VALUES));
            }
            case 1 -> { // Boolean ==
                cond.put("field", randomFrom(BOOLEAN_FIELDS));
                cond.put("op", "==");
                cond.put("value", random.nextBoolean());
            }
            case 2 -> { // String IN
                cond.put("field", randomFrom(STRING_FIELDS));
                cond.put("op", "IN");
                ArrayNode values = mapper.createArrayNode();
                int count = random.nextInt(2, 6);
                Set<String> picked = new HashSet<>();
                for (int i = 0; i < count; i++) {
                    String v = randomFrom(STRING_VALUES);
                    if (picked.add(v)) values.add(v);
                }
                cond.set("value", values);
            }
            case 3 -> { // Int ==
                cond.put("field", randomFrom(INT_FIELDS));
                cond.put("op", "==");
                cond.put("value", random.nextInt(1, 10000));
            }
        }
        return cond;
    }

    // =========================================================================
    // Condition Tree (đệ quy)
    // =========================================================================

    private ObjectNode generateConditionTree(int remainingHeight) {
        // Nếu đạt lá hoặc random quyết định dừng
        if (remainingHeight <= 1 || (remainingHeight < maxTreeHeight && random.nextDouble() < 0.4)) {
            return generateLeafCondition();
        }

        // Node nhánh: AND / OR
        ObjectNode node = mapper.createObjectNode();
        node.put("type", random.nextBoolean() ? "AND" : "OR");

        ArrayNode children = mapper.createArrayNode();
        int numChildren = random.nextInt(2, 4); // 2-3 children
        for (int i = 0; i < numChildren; i++) {
            children.add(generateConditionTree(remainingHeight - 1));
        }
        node.set("children", children);

        return node;
    }

    /**
     * Sinh node lá CONDITION với expression ngẫu nhiên.
     * Bao gồm đa dạng operators theo spec.
     */
    private ObjectNode generateLeafCondition() {
        ObjectNode node = mapper.createObjectNode();
        node.put("type", "CONDITION");

        ObjectNode expr = mapper.createObjectNode();

        // Chọn random loại expression
        int exprType = random.nextInt(10);
        switch (exprType) {
            case 0, 1 -> generateIntComparison(expr);
            case 2, 3 -> generateDoubleComparison(expr);
            case 4, 5 -> generateStringComparison(expr);
            case 6 -> generateBooleanComparison(expr);
            case 7 -> generateTimestampComparison(expr);
            case 8 -> generateBetweenExpression(expr);
            case 9 -> generateInExpression(expr);
        }

        node.set("expression", expr);
        return node;
    }

    // ---- Các hàm sinh expression theo kiểu dữ liệu ----

    private void generateIntComparison(ObjectNode expr) {
        // Prefix field với random source.version (ví dụ: CRM.v2.age)
        String qualifiedField = qualifyField(randomFrom(INT_FIELDS));
        String[] ops = {"==", "!=", ">", "<", ">=", "<="};
        String op = ops[random.nextInt(ops.length)];

        // Random: field vs value, field vs right_field, hoặc expr
        int variant = random.nextInt(3);
        switch (variant) {
            case 0 -> { // field op value
                expr.put("field", qualifiedField);
                expr.put("op", op);
                expr.put("value", random.nextInt(0, 100000));
            }
            case 1 -> { // field op right_field
                expr.put("field", qualifiedField);
                expr.put("op", op);
                expr.put("right_field", qualifyField(randomFrom(INT_FIELDS)));
            }
            case 2 -> { // expr op value
                String f1 = randomFrom(INT_FIELDS);
                String f2 = randomFrom(INT_FIELDS);
                String[] arithOps = {"+", "-", "*"};
                String arithOp = arithOps[random.nextInt(arithOps.length)];
                expr.put("expr", f1 + " " + arithOp + " " + f2);
                expr.put("op", op);
                expr.put("value", random.nextInt(0, 10000));
            }
        }
    }

    private void generateDoubleComparison(ObjectNode expr) {
        String qualifiedField = qualifyField(randomFrom(DOUBLE_FIELDS));
        String[] ops = {"==", "!=", ">", "<", ">=", "<="};
        String op = ops[random.nextInt(ops.length)];

        int variant = random.nextInt(3);
        switch (variant) {
            case 0 -> {
                expr.put("field", qualifiedField);
                expr.put("op", op);
                expr.put("value", roundDouble(random.nextDouble() * 10000000));
            }
            case 1 -> {
                expr.put("field", qualifiedField);
                expr.put("op", op);
                expr.put("right_field", qualifyField(randomFrom(DOUBLE_FIELDS)));
            }
            case 2 -> {
                String f1 = randomFrom(DOUBLE_FIELDS);
                String[] arithOps = {"+", "-", "*", "/"};
                double coeff = roundDouble(random.nextDouble() * 2);
                expr.put("expr", f1 + " * " + coeff);
                expr.put("op", op);
                expr.put("value", roundDouble(random.nextDouble() * 5000000));
            }
        }
    }

    private void generateStringComparison(ObjectNode expr) {
        String qualifiedField = qualifyField(randomFrom(STRING_FIELDS));

        int variant = random.nextInt(7);
        switch (variant) {
            case 0 -> { // ==
                expr.put("field", qualifiedField);
                expr.put("op", "==");
                expr.put("value", randomFrom(STRING_VALUES));
            }
            case 1 -> { // !=
                expr.put("field", qualifiedField);
                expr.put("op", "!=");
                expr.put("value", randomFrom(STRING_VALUES));
            }
            case 2 -> { // STARTS_WITH
                expr.put("field", qualifiedField);
                expr.put("op", "STARTS_WITH");
                expr.put("value", randomFrom(STRING_VALUES).substring(0, Math.min(3, randomFrom(STRING_VALUES).length())));
            }
            case 3 -> { // ENDS_WITH
                expr.put("field", qualifiedField);
                expr.put("op", "ENDS_WITH");
                String sv = randomFrom(STRING_VALUES);
                expr.put("value", sv.substring(Math.max(0, sv.length() - 4)));
            }
            case 4 -> { // CONTAINS
                expr.put("field", qualifiedField);
                expr.put("op", "CONTAINS");
                expr.put("value", randomFrom(STRING_VALUES).substring(0, Math.min(4, randomFrom(STRING_VALUES).length())));
            }
            case 5 -> { // IS_NULL / IS_NOT_NULL
                expr.put("field", qualifiedField);
                expr.put("op", random.nextBoolean() ? "IS_NULL" : "IS_NOT_NULL");
            }
            case 6 -> { // LENGTH
                expr.put("field", qualifiedField);
                expr.put("function", "LENGTH");
                String[] ops = {"==", ">", "<", ">=", "<="};
                expr.put("op", ops[random.nextInt(ops.length)]);
                expr.put("value", random.nextInt(1, 50));
            }
        }
    }

    private void generateBooleanComparison(ObjectNode expr) {
        String qualifiedField = qualifyField(randomFrom(BOOLEAN_FIELDS));
        expr.put("field", qualifiedField);
        expr.put("op", random.nextBoolean() ? "==" : "!=");
        expr.put("value", random.nextBoolean());
    }

    private void generateTimestampComparison(ObjectNode expr) {
        String qualifiedField = qualifyField(randomFrom(TIMESTAMP_FIELDS));
        String[] ops = {"==", "!=", ">", "<", ">=", "<="};
        String op = ops[random.nextInt(ops.length)];

        int variant = random.nextInt(2);
        if (variant == 0) {
            // field op literal timestamp
            expr.put("field", qualifiedField);
            expr.put("op", op);
            long ts = Instant.now().getEpochSecond() - random.nextLong(0, 365L * 24 * 3600);
            expr.put("value", Instant.ofEpochSecond(ts).toString());
        } else {
            // field op right_field
            expr.put("field", qualifiedField);
            expr.put("op", op);
            expr.put("right_field", qualifyField(randomFrom(TIMESTAMP_FIELDS)));
        }
    }

    private void generateBetweenExpression(ObjectNode expr) {
        // Random giữa INT/DOUBLE/TIMESTAMP BETWEEN
        int subType = random.nextInt(3);
        switch (subType) {
            case 0 -> { // INT BETWEEN
                expr.put("field", qualifyField(randomFrom(INT_FIELDS)));
                expr.put("op", "BETWEEN");
                int min = random.nextInt(0, 5000);
                int max = min + random.nextInt(1, 5000);
                ArrayNode range = mapper.createArrayNode();
                range.add(min);
                range.add(max);
                expr.set("value", range);
            }
            case 1 -> { // DOUBLE BETWEEN
                expr.put("field", qualifyField(randomFrom(DOUBLE_FIELDS)));
                expr.put("op", "BETWEEN");
                double min = roundDouble(random.nextDouble() * 1000000);
                double max = roundDouble(min + random.nextDouble() * 1000000);
                ArrayNode range = mapper.createArrayNode();
                range.add(min);
                range.add(max);
                expr.set("value", range);
            }
            case 2 -> { // TIMESTAMP BETWEEN
                expr.put("field", qualifyField(randomFrom(TIMESTAMP_FIELDS)));
                expr.put("op", "BETWEEN");
                long now = Instant.now().getEpochSecond();
                long from = now - random.nextLong(30L * 24 * 3600, 365L * 24 * 3600);
                long to = from + random.nextLong(1L * 24 * 3600, 60L * 24 * 3600);
                ArrayNode range = mapper.createArrayNode();
                range.add(Instant.ofEpochSecond(from).toString());
                range.add(Instant.ofEpochSecond(to).toString());
                expr.set("value", range);
            }
        }
    }

    private void generateInExpression(ObjectNode expr) {
        // Random giữa INT IN và STRING IN
        if (random.nextBoolean()) {
            // INT IN
            expr.put("field", qualifyField(randomFrom(INT_FIELDS)));
            expr.put("op", random.nextBoolean() ? "IN" : "NOT IN");
            ArrayNode values = mapper.createArrayNode();
            int count = random.nextInt(2, 8);
            for (int i = 0; i < count; i++) values.add(random.nextInt(1, 10000));
            expr.set("value", values);
        } else {
            // STRING IN
            expr.put("field", qualifyField(randomFrom(STRING_FIELDS)));
            expr.put("op", random.nextBoolean() ? "IN" : "NOT IN");
            ArrayNode values = mapper.createArrayNode();
            int count = random.nextInt(2, 6);
            Set<String> picked = new HashSet<>();
            for (int i = 0; i < count; i++) {
                String v = randomFrom(STRING_VALUES);
                if (picked.add(v)) values.add(v);
            }
            expr.set("value", values);
        }
    }

    // =========================================================================
    // Utility
    // =========================================================================

    /**
     * Qualify field với prefix source.version (ví dụ: CRM.v2.age).
     * Dùng trong condition_tree theo spec.
     */
    private String qualifyField(String rawField) {
        return randomFrom(sources) + "." + randomFrom(versions) + "." + rawField;
    }

    private <T> T randomFrom(List<T> list) {
        return list.get(random.nextInt(list.size()));
    }

    private double roundDouble(double val) {
        return Math.round(val * 100.0) / 100.0;
    }
}
