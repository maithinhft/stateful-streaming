package com.vdf.streaming.operators;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.vdf.streaming.dynamic.model.KafkaEventRecord;
import com.vdf.streaming.models.KeyDefinition;
import com.vdf.streaming.models.SchemaDefinition;
import com.vdf.streaming.utils.KeyNormalizer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Hàm thẩm định bản tin luồng Realtime Stream Event (theo format tại IMPLEMENTATION_BLUEPRINT.md).
 *
 * <p>Quy trình:
 * <ol>
 *   <li>Parse JSON payload.</li>
 *   <li>Tra cứu Schema định nghĩa từ {@link SchemaRegistry} theo Topic hoặc Source.</li>
 *   <li>Chuẩn hóa các trường số điện thoại (msisdn, custMobileNo) sang chuẩn ITU-T E.164.</li>
 *   <li>Thẩm định các ràng buộc trường (required, nullable, kiểu dữ liệu, whitelist, min/max).</li>
 *   <li>Phát hành bản tin JSON hợp lệ sang luồng downstream; ghi log WARN cho bản tin vi phạm.</li>
 * </ol>
 */
public class StreamSchemaValidationFunction extends ProcessFunction<KafkaEventRecord, String> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(StreamSchemaValidationFunction.class);

    private final String pgUrl;
    private final String pgUser;
    private final String pgPassword;

    private SchemaRegistry schemaRegistry;
    private transient ObjectMapper objectMapper;

    public StreamSchemaValidationFunction(String pgUrl, String pgUser, String pgPassword) {
        this.pgUrl = pgUrl;
        this.pgUser = pgUser;
        this.pgPassword = pgPassword;
    }

    public StreamSchemaValidationFunction(SchemaRegistry customRegistry) {
        this.pgUrl = null;
        this.pgUser = null;
        this.pgPassword = null;
        this.schemaRegistry = customRegistry;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.objectMapper = new ObjectMapper();

        if (this.schemaRegistry == null) {
            this.schemaRegistry = new SchemaRegistry();
            if (pgUrl != null && !pgUrl.isEmpty()) {
                this.schemaRegistry.loadFromPostgres(pgUrl, pgUser, pgPassword);
            }
        }
    }

    @Override
    public void processElement(KafkaEventRecord record, Context ctx, Collector<String> out) throws Exception {
        if (record == null || record.getPayload() == null || record.getPayload().trim().isEmpty()) {
            LOG.warn("ERR_STREAM_EMPTY_PAYLOAD: Dropping null/empty record from topic '{}'", record != null ? record.getTopic() : "UNKNOWN");
            return;
        }

        // 1. Parse JSON
        JsonNode rootNode;
        try {
            rootNode = objectMapper.readTree(record.getPayload());
        } catch (Exception e) {
            LOG.warn("ERR_STREAM_JSON_MALFORMED: Failed to parse JSON from topic '{}': {}", record.getTopic(), e.getMessage());
            return;
        }

        if (!rootNode.isObject()) {
            LOG.warn("ERR_STREAM_PAYLOAD_NOT_OBJECT: Record payload from topic '{}' is not a JSON object", record.getTopic());
            return;
        }

        // 2. Tra cứu Schema
        SchemaDefinition schema = null;
        if (schemaRegistry != null) {
            schema = schemaRegistry.getStreamSchema(record.getTopic());
            if (schema == null && rootNode.has("topic")) {
                schema = schemaRegistry.getStreamSchema(rootNode.get("topic").asText());
            }
            if (schema == null && rootNode.has("source")) {
                schema = schemaRegistry.getStreamSchema(rootNode.get("source").asText());
            }
        }

        if (schema == null) {
            LOG.warn("ERR_STREAM_SCHEMA_NOT_FOUND: No schema registered for topic '{}'", record.getTopic());
            return;
        }

        // 3. Chuẩn hóa Khóa E.164 (Key Normalization)
        KeyDefinition keyDef = schema.getKeyDefinition();
        if (keyDef != null && keyDef.isAutoNormalize()) {
            String keyField = (keyDef.getField() != null && !keyDef.getField().isEmpty()) ? keyDef.getField() : "msisdn";
            if (rootNode.has(keyField)) {
                String rawKey = rootNode.get(keyField).asText();
                String normalized = KeyNormalizer.normalize(rawKey, keyDef.getDefaultCountryCode());
                if (normalized != null) {
                    ((ObjectNode) rootNode).put(keyField, normalized);
                } else if (!keyDef.isAllowNull()) {
                    LOG.warn("ERR_STREAM_INVALID_KEY_FORMAT: Invalid phone key '{}' in field '{}' for topic '{}'",
                            rawKey, keyField, record.getTopic());
                    return;
                }
            }

            // Chuẩn hóa bổ sung trường custMobileNo nếu có
            if (rootNode.has("custMobileNo")) {
                String normalized = KeyNormalizer.normalize(rootNode.get("custMobileNo").asText(), keyDef.getDefaultCountryCode());
                if (normalized != null) {
                    ((ObjectNode) rootNode).put("custMobileNo", normalized);
                }
            }
        }

        // 4. Thẩm định các trường ràng buộc
        List<String> errors = new ArrayList<>();
        boolean isValid = schema.validateFields(rootNode, null, errors);
        if (!isValid) {
            LOG.warn("ERR_STREAM_VALIDATION_FAILED: Record from topic '{}' failed schema validation: {}",
                    record.getTopic(), errors);
            return;
        }

        // 5. Phát hành bản tin đã thẩm định
        LOG.debug("Stream event passed validation for topic '{}'", record.getTopic());
        out.collect(rootNode.toString());
    }

    public SchemaRegistry getSchemaRegistry() {
        return schemaRegistry;
    }
}
