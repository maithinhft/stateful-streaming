package com.vdf.streaming.models;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * Bao đóng của một luật đã được biên dịch.
 */
public class CompiledRuleEnvelope implements Serializable {
    private String ruleId;
    private String ruleName;
    private String ruleVersion;
    private int slotId;
    private RuleType ruleType;
    private List<CompiledTriggerCriteria> triggers;
    private ConditionNode conditionTree;
    private long cooldownSeconds;
    private Map<String, Object> metadata;

    public CompiledRuleEnvelope() {}

    private CompiledRuleEnvelope(Builder builder) {
        this.ruleId = builder.ruleId;
        this.ruleName = builder.ruleName;
        this.ruleVersion = builder.ruleVersion;
        this.slotId = builder.slotId;
        this.ruleType = builder.ruleType;
        this.triggers = builder.triggers;
        this.conditionTree = builder.conditionTree;
        this.cooldownSeconds = builder.cooldownSeconds;
        this.metadata = builder.metadata;
    }

    public String getRuleId() { return ruleId; }
    public void setRuleId(String ruleId) { this.ruleId = ruleId; }

    public String getRuleName() { return ruleName; }
    public void setRuleName(String ruleName) { this.ruleName = ruleName; }

    public String getRuleVersion() { return ruleVersion; }
    public void setRuleVersion(String ruleVersion) { this.ruleVersion = ruleVersion; }

    public int getSlotId() { return slotId; }
    public void setSlotId(int slotId) { this.slotId = slotId; }

    public RuleType getRuleType() { return ruleType; }
    public void setRuleType(RuleType ruleType) { this.ruleType = ruleType; }

    public List<CompiledTriggerCriteria> getTriggers() { return triggers; }
    public void setTriggers(List<CompiledTriggerCriteria> triggers) { this.triggers = triggers; }

    public ConditionNode getConditionTree() { return conditionTree; }
    public void setConditionTree(ConditionNode conditionTree) { this.conditionTree = conditionTree; }

    public long getCooldownSeconds() { return cooldownSeconds; }
    public void setCooldownSeconds(long cooldownSeconds) { this.cooldownSeconds = cooldownSeconds; }

    public Map<String, Object> getMetadata() { return metadata; }
    public void setMetadata(Map<String, Object> metadata) { this.metadata = metadata; }

    @Override
    public String toString() {
        return "CompiledRuleEnvelope{" +
                "ruleId='" + ruleId + '\'' +
                ", ruleName='" + ruleName + '\'' +
                ", ruleVersion='" + ruleVersion + '\'' +
                ", slotId=" + slotId +
                ", ruleType=" + ruleType +
                ", triggers=" + triggers +
                ", conditionTree=" + conditionTree +
                ", cooldownSeconds=" + cooldownSeconds +
                ", metadata=" + metadata +
                '}';
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String ruleId;
        private String ruleName;
        private String ruleVersion;
        private int slotId;
        private RuleType ruleType;
        private List<CompiledTriggerCriteria> triggers;
        private ConditionNode conditionTree;
        private long cooldownSeconds;
        private Map<String, Object> metadata;

        public Builder ruleId(String ruleId) {
            this.ruleId = ruleId;
            return this;
        }

        public Builder ruleName(String ruleName) {
            this.ruleName = ruleName;
            return this;
        }

        public Builder ruleVersion(String ruleVersion) {
            this.ruleVersion = ruleVersion;
            return this;
        }

        public Builder slotId(int slotId) {
            this.slotId = slotId;
            return this;
        }

        public Builder ruleType(RuleType ruleType) {
            this.ruleType = ruleType;
            return this;
        }

        public Builder triggers(List<CompiledTriggerCriteria> triggers) {
            this.triggers = triggers;
            return this;
        }

        public Builder conditionTree(ConditionNode conditionTree) {
            this.conditionTree = conditionTree;
            return this;
        }

        public Builder cooldownSeconds(long cooldownSeconds) {
            this.cooldownSeconds = cooldownSeconds;
            return this;
        }

        public Builder metadata(Map<String, Object> metadata) {
            this.metadata = metadata;
            return this;
        }

        public CompiledRuleEnvelope build() {
            return new CompiledRuleEnvelope(this);
        }
    }
}
