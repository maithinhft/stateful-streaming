package com.vdf.streaming.event.error;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.vdf.streaming.event.model.EventRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Bộ tạo dữ liệu bẩn / lỗi (Dirty Data / Error Injector) cho cả Stream và Batch Event.
 * Giúp kiểm thử khả năng chịu lỗi, Dead Letter Queue (DLQ) và các bộ lọc thẩm định Schema của Flink.
 */
public class DataQualityInjector {
    private static final Logger log = LoggerFactory.getLogger(DataQualityInjector.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    /**
     * Tiêm lỗi vào bản tin Stream Event.
     * Tạo ra các vi phạm thực tế theo đặc tả Schema Validation (IMPLEMENTATION_BLUEPRINT.md).
     */
    public static EventRecord injectStreamError(EventRecord original) {
        if (original == null || original.getPayloadJson() == null) return original;
        Random rand = ThreadLocalRandom.current();
        int errorType = rand.nextInt(5);

        try {
            switch (errorType) {
                case 0: {
                    // Type 1: Lỗi chuẩn hóa khóa điện thoại E.164 (ERR_STREAM_INVALID_KEY_FORMAT)
                    JsonNode root = MAPPER.readTree(original.getPayloadJson());
                    if (root.isObject()) {
                        ObjectNode obj = (ObjectNode) root;
                        String invalidPhone = "NOT_A_PHONE_" + rand.nextInt(1000);
                        if (obj.has("msisdn")) obj.put("msisdn", invalidPhone);
                        if (obj.has("custMobileNo")) obj.put("custMobileNo", invalidPhone);
                        return new EventRecord(original.getTopic(), original.getClusterType(), invalidPhone, obj.toString(), original.getContext());
                    }
                    break;
                }
                case 1: {
                    // Type 2: Thiếu trường bắt buộc (ERR_STREAM_VALIDATION_FAILED: missing required field)
                    JsonNode root = MAPPER.readTree(original.getPayloadJson());
                    if (root.isObject()) {
                        ObjectNode obj = (ObjectNode) root;
                        obj.remove("msisdn");
                        obj.remove("custMobileNo");
                        obj.remove("requestId");
                        obj.remove("processCode");
                        return new EventRecord(original.getTopic(), original.getClusterType(), original.getKey(), obj.toString(), original.getContext());
                    }
                    break;
                }
                case 2: {
                    // Type 3: Sai kiểu dữ liệu Type Mismatch (ERR_STREAM_VALIDATION_FAILED: expected INT/LONG, got STRING)
                    JsonNode root = MAPPER.readTree(original.getPayloadJson());
                    if (root.isObject()) {
                        ObjectNode obj = (ObjectNode) root;
                        obj.put("transAmount", "STRING_NOT_NUMBER");
                        obj.put("requestId", "ABC_XYZ_REQ_ID");
                        return new EventRecord(original.getTopic(), original.getClusterType(), original.getKey(), obj.toString(), original.getContext());
                    }
                    break;
                }
                case 3: {
                    // Type 4: JSON Malformed (ERR_STREAM_JSON_MALFORMED)
                    String malformed = original.getPayloadJson();
                    if (malformed.length() > 10) {
                        malformed = malformed.substring(0, malformed.length() / 2) + "...{BROKEN_JSON";
                    } else {
                        malformed = "{malformed_json_unclosed: ";
                    }
                    return new EventRecord(original.getTopic(), original.getClusterType(), original.getKey(), malformed, original.getContext());
                }
                case 4: {
                    // Type 5: Vi phạm Whitelist / Enum / Giá trị không hợp lệ
                    JsonNode root = MAPPER.readTree(original.getPayloadJson());
                    if (root.isObject()) {
                        ObjectNode obj = (ObjectNode) root;
                        obj.put("gender", "UNKNOWN_GENDER_CODE");
                        obj.put("status", -999);
                        return new EventRecord(original.getTopic(), original.getClusterType(), original.getKey(), obj.toString(), original.getContext());
                    }
                    break;
                }
            }
        } catch (Exception e) {
            log.debug("Lỗi khi inject stream error: {}", e.getMessage());
        }
        return original;
    }

    /**
     * Tiêm lỗi vào bản tin Batch Event.
     * Tạo ra các vi phạm tương ứng 5 tầng thẩm định của BatchSchemaValidationFunction (07_BATCH_EVENT_SCHEMA.md):
     * Level 1: Protocol / Missing envelope / Invalid sync_mode / Malformed JSON
     * Level 2: Partition Key Normalization (Invalid key_value)
     * Level 3: Schema Registry Lookup (Unknown dataset_name / sync_mode not allowed)
     * Level 4: Anti-Stale (Stale snapshot_time từ quá khứ xa)
     * Level 5: Semantic & Field Constraints (sai allowed_values, thiếu trường data...)
     */
    public static EventRecord injectBatchError(EventRecord original) {
        if (original == null || original.getPayloadJson() == null) return original;
        Random rand = ThreadLocalRandom.current();
        int errorLevel = 1 + rand.nextInt(5); // 1..5

        try {
            switch (errorLevel) {
                case 1: {
                    // Level 1: Protocol & Envelope (ERR_BATCH_MISSING_ENVELOPE hoặc ERR_BATCH_INVALID_SYNC_MODE hoặc ERR_BATCH_JSON_MALFORMED)
                    if (rand.nextBoolean()) {
                        // JSON Malformed
                        String broken = "{\"dataset_name\": \"corrupted_batch_event... [MALFORMED_JSON]";
                        return new EventRecord(original.getTopic(), original.getClusterType(), original.getKey(), broken, original.getContext());
                    } else {
                        JsonNode root = MAPPER.readTree(original.getPayloadJson());
                        if (root.isObject()) {
                            ObjectNode obj = (ObjectNode) root;
                            if (rand.nextBoolean()) {
                                obj.remove("dataset_name");
                                obj.remove("pipeline_id");
                            } else {
                                obj.put("sync_mode", "INVALID_SYNC_ACTION_123");
                            }
                            return new EventRecord(original.getTopic(), original.getClusterType(), original.getKey(), obj.toString(), original.getContext());
                        }
                    }
                    break;
                }
                case 2: {
                    // Level 2: Partition Key Normalization (ERR_BATCH_INVALID_KEY_FORMAT)
                    JsonNode root = MAPPER.readTree(original.getPayloadJson());
                    if (root.isObject()) {
                        ObjectNode obj = (ObjectNode) root;
                        String dirtyKey = "CORRUPTED_MSISDN_" + rand.nextInt(9999);
                        obj.put("key_value", dirtyKey);
                        return new EventRecord(original.getTopic(), original.getClusterType(), dirtyKey, obj.toString(), original.getContext());
                    }
                    break;
                }
                case 3: {
                    // Level 3: Schema Lookup (ERR_BATCH_SCHEMA_NOT_FOUND)
                    JsonNode root = MAPPER.readTree(original.getPayloadJson());
                    if (root.isObject()) {
                        ObjectNode obj = (ObjectNode) root;
                        obj.put("dataset_name", "non_existent_dataset_v99");
                        obj.put("schema_version", "v999");
                        return new EventRecord(original.getTopic(), original.getClusterType(), original.getKey(), obj.toString(), original.getContext());
                    }
                    break;
                }
                case 4: {
                    // Level 4: Anti-Stale (ERR_BATCH_STALE_SNAPSHOT)
                    JsonNode root = MAPPER.readTree(original.getPayloadJson());
                    if (root.isObject()) {
                        ObjectNode obj = (ObjectNode) root;
                        obj.put("snapshot_time", "2018-01-01T00:00:00.000+07:00");
                        return new EventRecord(original.getTopic(), original.getClusterType(), original.getKey(), obj.toString(), original.getContext());
                    }
                    break;
                }
                case 5: {
                    // Level 5: Semantic & Field Constraints (ERR_BATCH_FIELD_CONSTRAINTS_VIOLATED hoặc ERR_BATCH_MISSING_DATA)
                    JsonNode root = MAPPER.readTree(original.getPayloadJson());
                    if (root.isObject()) {
                        ObjectNode obj = (ObjectNode) root;
                        if (obj.has("data") && obj.get("data").isObject()) {
                            ObjectNode dataObj = (ObjectNode) obj.get("data");
                            dataObj.put("sub_code", "INVALID_SUB_CODE_9999");
                            dataObj.put("status", "NOT_A_VALID_INT_STATUS");
                        } else {
                            obj.remove("data");
                        }
                        return new EventRecord(original.getTopic(), original.getClusterType(), original.getKey(), obj.toString(), original.getContext());
                    }
                    break;
                }
            }
        } catch (Exception e) {
            log.debug("Lỗi khi inject batch error: {}", e.getMessage());
        }
        return original;
    }
}
