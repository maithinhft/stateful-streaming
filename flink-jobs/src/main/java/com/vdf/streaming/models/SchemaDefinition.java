package com.vdf.streaming.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

@JsonIgnoreProperties(ignoreUnknown = true)
public class SchemaDefinition implements Serializable {

    private static final long serialVersionUID = 1L;

    @JsonProperty("schema_id")
    private String schemaId;

    @JsonProperty("schema_type")
    private String schemaType; // STREAM or BATCH

    @JsonProperty("dataset_name")
    private String datasetName;

    @JsonProperty("source")
    private String source;

    @JsonProperty("topic")
    private String topic;

    @JsonProperty("version")
    private String version = "v1";

    @JsonProperty("description")
    private String description;

    @JsonProperty("ttl_seconds")
    private Long ttlSeconds;

    @JsonProperty("key_definition")
    private KeyDefinition keyDefinition = new KeyDefinition();

    @JsonProperty("allowed_sync_modes")
    private List<String> allowedSyncModes = new ArrayList<>();

    @JsonProperty("fields")
    private Map<String, FieldDefinition> fields = Collections.emptyMap();

    public SchemaDefinition() {}

    public boolean isStream() {
        return "STREAM".equalsIgnoreCase(schemaType);
    }

    public boolean isBatch() {
        return "BATCH".equalsIgnoreCase(schemaType);
    }

    /**
     * Thẩm định khối dữ liệu dataNode theo danh mục ràng buộc fields của Schema.
     *
     * @param dataNode      Khối data cần kiểm tra
     * @param syncMode      Chế độ đồng bộ (FULL_SNAPSHOT, UPSERT, DELETE hoặc null với stream)
     * @param errorMessages Danh sách chứa thông điệp lỗi nếu vi phạm
     * @return true nếu thỏa mãn toàn bộ ràng buộc
     */
    public boolean validateFields(JsonNode dataNode, String syncMode, List<String> errorMessages) {
        if ("DELETE".equalsIgnoreCase(syncMode)) {
            return true; // Khối data rỗng hoặc null khi DELETE là hợp lệ
        }

        if (dataNode == null || !dataNode.isObject()) {
            errorMessages.add("ERR_DATA_NOT_OBJECT: Data payload must be a JSON object");
            return false;
        }

        boolean isValid = true;
        boolean isFullSnapshot = "FULL_SNAPSHOT".equalsIgnoreCase(syncMode);
        boolean isStream = (syncMode == null);

        for (Map.Entry<String, FieldDefinition> entry : fields.entrySet()) {
            String fieldName = entry.getKey();
            FieldDefinition def = entry.getValue();

            boolean exists = dataNode.has(fieldName);

            // 1. Kiểm tra trường bắt buộc (required)
            if (def.isRequired() && (isFullSnapshot || isStream)) {
                if (!exists) {
                    errorMessages.add("ERR_MISSING_REQUIRED_FIELD: Missing field '" + fieldName + "'");
                    isValid = false;
                    continue;
                }
            }

            if (!exists) {
                continue;
            }

            JsonNode val = dataNode.get(fieldName);

            // 2. Kiểm tra Nullable
            if (val == null || val.isNull()) {
                if (!def.isNullable()) {
                    errorMessages.add("ERR_NULL_NOT_ALLOWED: Field '" + fieldName + "' cannot be null");
                    isValid = false;
                }
                continue;
            }

            // 3. Kiểm tra kiểu dữ liệu
            String expectedType = def.getType() != null ? def.getType().toUpperCase() : "STRING";
            boolean typeValid = checkType(val, expectedType);
            if (!typeValid) {
                errorMessages.add("ERR_TYPE_MISMATCH: Field '" + fieldName + "' expected type " + expectedType + ", got " + val.getNodeType());
                isValid = false;
                continue;
            }

            // 4. Kiểm tra Allowed Values (Enum / Whitelist)
            if (def.getAllowedValues() != null && !def.getAllowedValues().isEmpty()) {
                boolean matchAllowed = false;
                String valText = val.asText();
                for (Object allowed : def.getAllowedValues()) {
                    if (allowed != null && allowed.toString().equals(valText)) {
                        matchAllowed = true;
                        break;
                    }
                }
                if (!matchAllowed) {
                    errorMessages.add("ERR_ENUM_VIOLATION: Field '" + fieldName + "' with value '" + valText + "' not in allowed_values: " + def.getAllowedValues());
                    isValid = false;
                }
            }

            // 5. Kiểm tra min / max cho số học
            if (val.isNumber()) {
                double num = val.asDouble();
                if (def.getMin() != null && num < def.getMin()) {
                    errorMessages.add("ERR_OUT_OF_RANGE: Field '" + fieldName + "' value " + num + " < min " + def.getMin());
                    isValid = false;
                }
                if (def.getMax() != null && num > def.getMax()) {
                    errorMessages.add("ERR_OUT_OF_RANGE: Field '" + fieldName + "' value " + num + " > max " + def.getMax());
                    isValid = false;
                }
            }

            // 6. Kiểm tra độ dài chuỗi
            if (val.isTextual()) {
                String text = val.asText();
                if (def.getMinLength() != null && text.length() < def.getMinLength()) {
                    errorMessages.add("ERR_STRING_TOO_SHORT: Field '" + fieldName + "' length " + text.length() + " < min_length " + def.getMinLength());
                    isValid = false;
                }
                if (def.getMaxLength() != null && text.length() > def.getMaxLength()) {
                    errorMessages.add("ERR_STRING_TOO_LONG: Field '" + fieldName + "' length " + text.length() + " > max_length " + def.getMaxLength());
                    isValid = false;
                }
                if (def.getRegexPattern() != null && !def.getRegexPattern().isEmpty()) {
                    try {
                        if (!Pattern.compile(def.getRegexPattern()).matcher(text).matches()) {
                            errorMessages.add("ERR_PATTERN_MISMATCH: Field '" + fieldName + "' does not match pattern " + def.getRegexPattern());
                            isValid = false;
                        }
                    } catch (Exception ignored) {
                    }
                }
            }
        }

        return isValid;
    }

    private boolean checkType(JsonNode val, String type) {
        return switch (type) {
            case "INT", "INTEGER" -> val.isInt() || val.canConvertToInt() || (val.isTextual() && isInteger(val.asText()));
            case "LONG" -> val.isLong() || val.canConvertToLong() || (val.isTextual() && isLong(val.asText()));
            case "DOUBLE", "FLOAT" -> val.isNumber() || (val.isTextual() && isDouble(val.asText()));
            case "BOOLEAN" -> val.isBoolean() || (val.isTextual() && ("true".equalsIgnoreCase(val.asText()) || "false".equalsIgnoreCase(val.asText())));
            case "OBJECT" -> val.isObject();
            case "ARRAY" -> val.isArray();
            case "STRING", "TIMESTAMP" -> val.isValueNode();
            default -> true;
        };
    }

    private boolean isInteger(String s) {
        try { Integer.parseInt(s); return true; } catch (Exception e) { return false; }
    }

    private boolean isLong(String s) {
        try { Long.parseLong(s); return true; } catch (Exception e) { return false; }
    }

    private boolean isDouble(String s) {
        try { Double.parseDouble(s); return true; } catch (Exception e) { return false; }
    }

    // Getters and Setters

    public String getSchemaId() {
        return schemaId;
    }

    public void setSchemaId(String schemaId) {
        this.schemaId = schemaId;
    }

    public String getSchemaType() {
        return schemaType;
    }

    public void setSchemaType(String schemaType) {
        this.schemaType = schemaType;
    }

    public String getDatasetName() {
        return datasetName;
    }

    public void setDatasetName(String datasetName) {
        this.datasetName = datasetName;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public Long getTtlSeconds() {
        return ttlSeconds;
    }

    public void setTtlSeconds(Long ttlSeconds) {
        this.ttlSeconds = ttlSeconds;
    }

    public KeyDefinition getKeyDefinition() {
        return keyDefinition;
    }

    public void setKeyDefinition(KeyDefinition keyDefinition) {
        this.keyDefinition = keyDefinition;
    }

    public List<String> getAllowedSyncModes() {
        return allowedSyncModes;
    }

    public void setAllowedSyncModes(List<String> allowedSyncModes) {
        this.allowedSyncModes = allowedSyncModes;
    }

    public Map<String, FieldDefinition> getFields() {
        return fields;
    }

    public void setFields(Map<String, FieldDefinition> fields) {
        this.fields = fields;
    }
}
