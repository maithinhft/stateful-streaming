package com.vdf.streaming.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.Serializable;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Objects;

/**
 * Model POJO đại diện cho bản tin Batch Event sau khi đã vượt qua toàn bộ 5 tầng thẩm định
 * theo quy định tại 07_BATCH_EVENT_SCHEMA.md.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ValidatedBatchEvent implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @JsonProperty("dataset_name")
    private String datasetName;

    @JsonProperty("pipeline_id")
    private String pipelineId;

    @JsonProperty("schema_version")
    private String schemaVersion = "v1";

    @JsonProperty("batch_id")
    private String batchId;

    @JsonProperty("snapshot_time")
    private String snapshotTime;

    @JsonProperty("sync_mode")
    private String syncMode;

    @JsonProperty("key_field")
    private String keyField = "msisdn";

    @JsonProperty("key_value")
    private String keyValue;

    @JsonProperty("data")
    private JsonNode data;

    public ValidatedBatchEvent() {}

    public ValidatedBatchEvent(String datasetName, String pipelineId, String schemaVersion,
                               String batchId, String snapshotTime, String syncMode,
                               String keyField, String keyValue, JsonNode data) {
        this.datasetName = datasetName;
        this.pipelineId = pipelineId;
        this.schemaVersion = schemaVersion;
        this.batchId = batchId;
        this.snapshotTime = snapshotTime;
        this.syncMode = syncMode;
        this.keyField = keyField;
        this.keyValue = keyValue;
        this.data = data;
    }

    /**
     * Chuyển đổi snapshot_time (ISO-8601) thành epoch milliseconds.
     */
    public long getSnapshotTimestampMillis() {
        if (snapshotTime == null || snapshotTime.isEmpty()) {
            return System.currentTimeMillis();
        }
        try {
            return OffsetDateTime.parse(snapshotTime, DateTimeFormatter.ISO_DATE_TIME).toInstant().toEpochMilli();
        } catch (Exception e1) {
            try {
                return java.time.Instant.parse(snapshotTime).toEpochMilli();
            } catch (Exception e2) {
                return System.currentTimeMillis();
            }
        }
    }

    /**
     * Xuất ra chuỗi JSON envelope chuẩn.
     */
    public String toJsonString() {
        try {
            return MAPPER.writeValueAsString(this);
        } catch (Exception e) {
            return "{}";
        }
    }

    public static ValidatedBatchEvent fromJson(String json) throws Exception {
        return MAPPER.readValue(json, ValidatedBatchEvent.class);
    }

    public String getDatasetName() {
        return datasetName;
    }

    public void setDatasetName(String datasetName) {
        this.datasetName = datasetName;
    }

    public String getPipelineId() {
        return pipelineId;
    }

    public void setPipelineId(String pipelineId) {
        this.pipelineId = pipelineId;
    }

    public String getSchemaVersion() {
        return schemaVersion;
    }

    public void setSchemaVersion(String schemaVersion) {
        this.schemaVersion = schemaVersion;
    }

    public String getBatchId() {
        return batchId;
    }

    public void setBatchId(String batchId) {
        this.batchId = batchId;
    }

    public String getSnapshotTime() {
        return snapshotTime;
    }

    public void setSnapshotTime(String snapshotTime) {
        this.snapshotTime = snapshotTime;
    }

    public String getSyncMode() {
        return syncMode;
    }

    public void setSyncMode(String syncMode) {
        this.syncMode = syncMode;
    }

    public String getKeyField() {
        return keyField;
    }

    public void setKeyField(String keyField) {
        this.keyField = keyField;
    }

    public String getKeyValue() {
        return keyValue;
    }

    public void setKeyValue(String keyValue) {
        this.keyValue = keyValue;
    }

    public JsonNode getData() {
        return data;
    }

    public void setData(JsonNode data) {
        this.data = data;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ValidatedBatchEvent that = (ValidatedBatchEvent) o;
        return Objects.equals(datasetName, that.datasetName) &&
                Objects.equals(batchId, that.batchId) &&
                Objects.equals(keyValue, that.keyValue);
    }

    @Override
    public int hashCode() {
        return Objects.hash(datasetName, batchId, keyValue);
    }

    @Override
    public String toString() {
        return toJsonString();
    }
}
