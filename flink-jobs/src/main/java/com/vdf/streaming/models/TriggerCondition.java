package com.vdf.streaming.models;

import java.io.Serializable;
import java.util.List;

/**
 * Điều kiện trigger (thành phần của DNF).
 */
public class TriggerCondition implements Serializable {
    private String field;
    private List<String> fields;
    private String op;
    private Object value;
    private String datasetId;
    private String datasetVersion;

    public TriggerCondition() {}

    public TriggerCondition(String field, List<String> fields, String op, Object value, String datasetId, String datasetVersion) {
        this.field = field;
        this.fields = fields;
        this.op = op;
        this.value = value;
        this.datasetId = datasetId;
        this.datasetVersion = datasetVersion;
    }

    public String getField() { return field; }
    public void setField(String field) { this.field = field; }

    public List<String> getFields() { return fields; }
    public void setFields(List<String> fields) { this.fields = fields; }

    public String getOp() { return op; }
    public void setOp(String op) { this.op = op; }

    public Object getValue() { return value; }
    public void setValue(Object value) { this.value = value; }

    public String getDatasetId() { return datasetId; }
    public void setDatasetId(String datasetId) { this.datasetId = datasetId; }

    public String getDatasetVersion() { return datasetVersion; }
    public void setDatasetVersion(String datasetVersion) { this.datasetVersion = datasetVersion; }

    @Override
    public String toString() {
        return "TriggerCondition{" +
                "field='" + field + '\'' +
                ", fields=" + fields +
                ", op='" + op + '\'' +
                ", value=" + value +
                ", datasetId='" + datasetId + '\'' +
                ", datasetVersion='" + datasetVersion + '\'' +
                '}';
    }
}
