package com.vdf.streaming.compiler;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.vdf.streaming.models.*;

import java.util.ArrayList;
import java.util.List;

/**
 * Lớp dùng để biên dịch các sự kiện CDC từ topic rule_definitions thành CompiledRuleEnvelope.
 */
public class RuleCompiler {

    private final ObjectMapper objectMapper;

    public RuleCompiler() {
        this.objectMapper = new ObjectMapper();
    }

    /**
     * Sự kiện CDC từ topic rule_definitions.
     */
    public record CdcRuleEvent(
            String ruleId,
            String name,
            String ruleJson,
            long cooldownSeconds,
            long version,
            boolean enabled,
            String deleted,
            String op
    ) {}

    /**
     * Parse chuỗi JSON từ Kafka (CDC envelope) thành đối tượng CdcRuleEvent.
     *
     * @param cdcJson Chuỗi JSON của sự kiện CDC.
     * @return Đối tượng CdcRuleEvent.
     * @throws RuntimeException Nếu không thể parse JSON.
     */
    public CdcRuleEvent parseCdcEvent(String cdcJson) {
        try {
            JsonNode root = objectMapper.readTree(cdcJson);
            return new CdcRuleEvent(
                    root.path("rule_id").asText(null),
                    root.path("name").asText(null),
                    root.path("rule_json").asText(null),
                    root.path("cooldown_seconds").asLong(0),
                    root.path("version").asLong(0),
                    root.path("enabled").asBoolean(false),
                    root.path("__deleted").asText("false"),
                    root.path("__op").asText(null)
            );
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Lỗi khi parse sự kiện CDC", e);
        }
    }

    /**
     * Kiểm tra xem sự kiện có phải là một sự kiện xóa (DELETE) hoặc vô hiệu hóa rule hay không.
     *
     * @param event Đối tượng CdcRuleEvent cần kiểm tra.
     * @return true nếu là sự kiện xóa/vô hiệu hóa, ngược lại false.
     */
    public boolean isDelete(CdcRuleEvent event) {
        return "true".equalsIgnoreCase(event.deleted()) ||
                "d".equalsIgnoreCase(event.op()) ||
                !event.enabled();
    }

    /**
     * Biên dịch CdcRuleEvent thành CompiledRuleEnvelope.
     *
     * @param event  Sự kiện CdcRuleEvent chứa định nghĩa luật.
     * @param slotId ID của slot.
     * @return Đối tượng CompiledRuleEnvelope sau khi biên dịch.
     * @throws RuntimeException Nếu không thể biên dịch.
     */
    public CompiledRuleEnvelope compile(CdcRuleEvent event, int slotId) {
        try {
            JsonNode ruleJsonNode = objectMapper.readTree(event.ruleJson());

            String ruleId = ruleJsonNode.path("rule_id").asText();
            String ruleName = ruleJsonNode.path("rule_name").asText();
            String ruleVersion = ruleJsonNode.path("rule_version").asText();

            // Phân tích trigger criteria
            List<CompiledTriggerCriteria> triggers = new ArrayList<>();
            JsonNode triggerCriteriaNode = ruleJsonNode.path("trigger_criteria");
            if (triggerCriteriaNode.isArray()) {
                for (JsonNode triggerNode : triggerCriteriaNode) {
                    String source = triggerNode.path("source").asText();
                    String schemaVersion = triggerNode.path("schema_version").asText();
                    String keyField = triggerNode.path("key_field").asText();
                    String eventTimeField = triggerNode.path("event_time_field").asText();

                    List<List<TriggerCondition>> conditions = new ArrayList<>();
                    JsonNode conditionsNode = triggerNode.path("conditions");
                    if (conditionsNode.isArray()) {
                        for (JsonNode dnfNode : conditionsNode) {
                            List<TriggerCondition> andConditions = new ArrayList<>();
                            if (dnfNode.isArray()) {
                                for (JsonNode condNode : dnfNode) {
                                    String op = condNode.path("op").asText(null);
                                    String value = condNode.path("value").asText(null);
                                    String datasetId = condNode.path("dataset_id").asText(null);
                                    String datasetVersion = condNode.path("dataset_version").asText(null);

                                    List<String> fields = new ArrayList<>();
                                    if (condNode.has("fields") && condNode.path("fields").isArray()) {
                                        for (JsonNode fNode : condNode.path("fields")) {
                                            fields.add(fNode.asText());
                                        }
                                    } else if (condNode.has("field")) {
                                        fields.add(condNode.path("field").asText());
                                    }

                                    andConditions.add(new TriggerCondition(fields, op, value, datasetId, datasetVersion));
                                }
                            }
                            conditions.add(andConditions);
                        }
                    }
                    triggers.add(new CompiledTriggerCriteria(source, schemaVersion, keyField, eventTimeField, conditions));
                }
            }

            // Phân tích condition tree
            JsonNode conditionTreeNode = ruleJsonNode.path("condition_tree");
            ConditionNode conditionTree = parseConditionNode(conditionTreeNode);

            // Phân loại RuleType
            RuleType ruleType = classifyRuleType(conditionTree);

            return new CompiledRuleEnvelope(
                    ruleId,
                    ruleName,
                    ruleVersion,
                    event.cooldownSeconds(),
                    triggers,
                    conditionTree,
                    ruleType,
                    slotId
            );

        } catch (JsonProcessingException e) {
            throw new RuntimeException("Lỗi khi parse rule_json", e);
        }
    }

    private ConditionNode parseConditionNode(JsonNode node) {
        if (node == null || node.isMissingNode() || node.isNull()) {
            return null;
        }

        String type = node.path("type").asText();

        return switch (type) {
            case "AND", "OR" -> {
                List<ConditionNode> children = new ArrayList<>();
                JsonNode childrenNode = node.path("children");
                if (childrenNode.isArray()) {
                    for (JsonNode childNode : childrenNode) {
                        children.add(parseConditionNode(childNode));
                    }
                }
                yield new LogicalNode(type, children);
            }
            case "CONDITION" -> {
                JsonNode exprNode = node.path("expression");
                Expression expression = null;
                if (!exprNode.isMissingNode() && !exprNode.isNull()) {
                    String field = exprNode.path("field").asText(null);
                    String op = exprNode.path("op").asText(null);
                    String value = exprNode.path("value").asText(null);

                    // Phân tích multi-field cho IN_DATASET nếu có
                    List<String> fields = new ArrayList<>();
                    if (exprNode.has("fields") && exprNode.path("fields").isArray()) {
                        for (JsonNode fNode : exprNode.path("fields")) {
                            fields.add(fNode.asText());
                        }
                    } else if (field != null) {
                        fields.add(field);
                    }

                    String datasetId = exprNode.path("dataset_id").asText(null);
                    String datasetVersion = exprNode.path("dataset_version").asText(null);

                    expression = new Expression(fields, op, value, datasetId, datasetVersion);
                }
                yield new ConditionLeafNode(expression);
            }
            case "SEQUENCE" -> {
                String pattern = node.path("pattern").asText(null);
                long minTime = node.path("min_time").asLong(0);
                long maxTime = node.path("max_time").asLong(0);
                String timeUnit = node.path("time_unit").asText(null);

                List<String> joinKeys = new ArrayList<>();
                JsonNode joinKeysNode = node.path("join_keys");
                if (joinKeysNode.isArray()) {
                    for (JsonNode kNode : joinKeysNode) {
                        joinKeys.add(kNode.asText());
                    }
                }

                ConditionNode first = parseConditionNode(node.path("first"));
                ConditionNode second = parseConditionNode(node.path("second"));

                yield new SequenceNode(pattern, minTime, maxTime, timeUnit, joinKeys, first, second);
            }
            default -> throw new IllegalArgumentException("Loại ConditionNode không hợp lệ: " + type);
        };
    }

    private RuleType classifyRuleType(ConditionNode node) {
        if (node == null) {
            return RuleType.STATELESS;
        }

        if (hasCepFeatures(node)) {
            return RuleType.CEP;
        }

        if (hasStatefulFeatures(node)) {
            return RuleType.STATEFUL;
        }

        return RuleType.STATELESS;
    }

    private boolean hasCepFeatures(ConditionNode node) {
        if (node instanceof SequenceNode) {
            return true;
        }

        if (node instanceof ConditionLeafNode leaf) {
            Expression expr = leaf.expression();
            if (expr != null && "IS_FIRST_ARRIVAL".equalsIgnoreCase(expr.op())) {
                return true;
            }
        }

        if (node instanceof LogicalNode logical) {
            for (ConditionNode child : logical.children()) {
                if (hasCepFeatures(child)) {
                    return true;
                }
            }
        }

        return false;
    }

    private boolean hasStatefulFeatures(ConditionNode node) {
        if (node instanceof ConditionLeafNode leaf) {
            Expression expr = leaf.expression();
            if (expr != null) {
                if ("IN_DATASET".equalsIgnoreCase(expr.op())) {
                    return true;
                }
                if (expr.fields() != null) {
                    for (String field : expr.fields()) {
                        // Kiểm tra nếu field chứa dấu chấm (.) biểu thị việc truy xuất dữ liệu từ các source/dataset khác
                        if (field != null && field.contains(".")) {
                            return true;
                        }
                    }
                }
            }
        }

        if (node instanceof LogicalNode logical) {
            for (ConditionNode child : logical.children()) {
                if (hasStatefulFeatures(child)) {
                    return true;
                }
            }
        }

        if (node instanceof SequenceNode seq) {
            return hasStatefulFeatures(seq.first()) || hasStatefulFeatures(seq.second());
        }

        return false;
    }
}
