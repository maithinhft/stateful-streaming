package com.vdf.streaming.models;

import java.io.Serializable;
import java.util.List;

/**
 * Tiêu chí trigger cho một nguồn dữ liệu cụ thể.
 */
public class CompiledTriggerCriteria implements Serializable {
    private String source;
    private String schemaVersion;
    private String keyField;
    private String eventTimeField;
    private List<List<TriggerCondition>> dnfConditions;

    public CompiledTriggerCriteria() {}

    public CompiledTriggerCriteria(String source, String schemaVersion, String keyField, String eventTimeField, List<List<TriggerCondition>> dnfConditions) {
        this.source = source;
        this.schemaVersion = schemaVersion;
        this.keyField = keyField;
        this.eventTimeField = eventTimeField;
        this.dnfConditions = dnfConditions;
    }

    public String getSource() { return source; }
    public void setSource(String source) { this.source = source; }

    public String getSchemaVersion() { return schemaVersion; }
    public void setSchemaVersion(String schemaVersion) { this.schemaVersion = schemaVersion; }

    public String getKeyField() { return keyField; }
    public void setKeyField(String keyField) { this.keyField = keyField; }

    public String getEventTimeField() { return eventTimeField; }
    public void setEventTimeField(String eventTimeField) { this.eventTimeField = eventTimeField; }

    public List<List<TriggerCondition>> getDnfConditions() { return dnfConditions; }
    public void setDnfConditions(List<List<TriggerCondition>> dnfConditions) { this.dnfConditions = dnfConditions; }

    @Override
    public String toString() {
        return "CompiledTriggerCriteria{" +
                "source='" + source + '\'' +
                ", schemaVersion='" + schemaVersion + '\'' +
                ", keyField='" + keyField + '\'' +
                ", eventTimeField='" + eventTimeField + '\'' +
                ", dnfConditions=" + dnfConditions +
                '}';
    }
}
