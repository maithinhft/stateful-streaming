package com.vdf.streaming.validation;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.vdf.streaming.dynamic.model.KafkaEventRecord;
import com.vdf.streaming.validation.model.KeyDefinition;
import com.vdf.streaming.validation.model.SchemaDefinition;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Hàm thẩm định bản tin luồng Batch Event theo Quy trình 5 tầng (5-Level Validation Pipeline)
 * quy định tại đặc tả 07_BATCH_EVENT_SCHEMA.md.
 *
 * <ol>
 *   <li>Level 1: Protocol &amp; Envelope Validation (dataset_name, pipeline_id, batch_id, sync_mode...).</li>
 *   <li>Level 2: Partition Key Normalization (chuẩn hóa E.164 cho số điện thoại).</li>
 *   <li>Level 3: Schema Registry Lookup (khớp batch_{dataset_name}_{version}).</li>
 *   <li>Level 4: Anti-Stale &amp; Snapshot Ordering (loại bỏ snapshot lỗi thời để chống ghi đè dữ liệu cũ).</li>
 *   <li>Level 5: Semantic &amp; Field Constraints (kiểm tra type, required, nullable, allowed_values...).</li>
 * </ol>
 */
public class BatchSchemaValidationProcessFunction extends ProcessFunction<KafkaEventRecord, String> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(BatchSchemaValidationProcessFunction.class);

    private final String pgUrl;
    private final String pgUser;
    private final String pgPassword;

    private SchemaRegistry schemaRegistry;
    private transient ObjectMapper objectMapper;
    private transient Map<String, Long> lastSnapshotTimes;

    public BatchSchemaValidationProcessFunction(String pgUrl, String pgUser, String pgPassword) {
        this.pgUrl = pgUrl;
        this.pgUser = pgUser;
        this.pgPassword = pgPassword;
    }

    public BatchSchemaValidationProcessFunction(SchemaRegistry customRegistry) {
        this.pgUrl = null;
        this.pgUser = null;
        this.pgPassword = null;
        this.schemaRegistry = customRegistry;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.objectMapper = new ObjectMapper();
        this.lastSnapshotTimes = new ConcurrentHashMap<>();

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
            LOG.warn("ERR_BATCH_EMPTY_PAYLOAD: Dropping null/empty record from topic '{}'", record != null ? record.getTopic() : "UNKNOWN");
            return;
        }

        // ====================================================================
        // LEVEL 1: Protocol & Envelope Validation
        // ====================================================================
        JsonNode rootNode;
        try {
            rootNode = objectMapper.readTree(record.getPayload());
        } catch (Exception e) {
            LOG.warn("ERR_BATCH_JSON_MALFORMED: Failed to parse batch JSON from topic '{}': {}", record.getTopic(), e.getMessage());
            return;
        }

        if (!rootNode.isObject()) {
            LOG.warn("ERR_BATCH_PAYLOAD_NOT_OBJECT: Batch payload from topic '{}' is not a JSON object", record.getTopic());
            return;
        }

        String datasetName = rootNode.hasNonNull("dataset_name") ? rootNode.get("dataset_name").asText() : null;
        String pipelineId = rootNode.hasNonNull("pipeline_id") ? rootNode.get("pipeline_id").asText() : null;
        String schemaVersion = rootNode.hasNonNull("schema_version") ? rootNode.get("schema_version").asText() : "v1";
        String batchId = rootNode.hasNonNull("batch_id") ? rootNode.get("batch_id").asText() : null;
        String snapshotTimeStr = rootNode.hasNonNull("snapshot_time") ? rootNode.get("snapshot_time").asText() : null;
        String syncMode = rootNode.hasNonNull("sync_mode") ? rootNode.get("sync_mode").asText().toUpperCase() : null;
        String rawKeyValue = rootNode.hasNonNull("key_value") ? rootNode.get("key_value").asText() : null;

        if (datasetName == null || pipelineId == null || batchId == null || snapshotTimeStr == null || syncMode == null || rawKeyValue == null) {
            LOG.warn("ERR_BATCH_MISSING_ENVELOPE: Missing required envelope fields in record from topic '{}': dataset={}, pipeline={}, batch={}, snapshot={}, sync={}, key={}",
                    record.getTopic(), datasetName, pipelineId, batchId, snapshotTimeStr, syncMode, rawKeyValue);
            return;
        }

        if (!syncMode.equals("FULL_SNAPSHOT") && !syncMode.equals("UPSERT") && !syncMode.equals("DELETE")) {
            LOG.warn("ERR_BATCH_INVALID_SYNC_MODE: Unsupported sync_mode '{}' for dataset '{}'", syncMode, datasetName);
            return;
        }

        // ====================================================================
        // LEVEL 2: Partition Key Normalization (ITU-T E.164)
        // ====================================================================
        String normalizedKey = KeyNormalizer.normalize(rawKeyValue, "84");
        if (normalizedKey == null) {
            LOG.warn("ERR_BATCH_INVALID_KEY_FORMAT: Key value '{}' cannot be normalized to E.164 for dataset '{}'", rawKeyValue, datasetName);
            return;
        }
        ((ObjectNode) rootNode).put("key_value", normalizedKey);

        // ====================================================================
        // LEVEL 3: Schema Registry Lookup
        // ====================================================================
        SchemaDefinition schema = null;
        if (schemaRegistry != null) {
            schema = schemaRegistry.getBatchSchema(datasetName, schemaVersion);
        }

        if (schema == null) {
            LOG.warn("ERR_BATCH_SCHEMA_NOT_FOUND: No schema found for dataset '{}', version '{}'", datasetName, schemaVersion);
            return;
        }

        if (schema.getAllowedSyncModes() != null && !schema.getAllowedSyncModes().isEmpty()) {
            boolean modeAllowed = false;
            for (String allowed : schema.getAllowedSyncModes()) {
                if (syncMode.equalsIgnoreCase(allowed)) {
                    modeAllowed = true;
                    break;
                }
            }
            if (!modeAllowed) {
                LOG.warn("ERR_BATCH_SYNC_MODE_NOT_ALLOWED: sync_mode '{}' not in allowed_sync_modes {} for dataset '{}'",
                        syncMode, schema.getAllowedSyncModes(), datasetName);
                return;
            }
        }

        // ====================================================================
        // LEVEL 4: Anti-Stale & Snapshot Ordering Check
        // ====================================================================
        long snapshotMillis = parseSnapshotTime(snapshotTimeStr);
        Long lastSnapshot = lastSnapshotTimes.get(datasetName);
        if (lastSnapshot != null && snapshotMillis < lastSnapshot) {
            LOG.warn("ERR_BATCH_STALE_DATA: Snapshot time {} ({} ms) is older than latest snapshot {} ms for dataset '{}'. Dropping to avoid overwriting state.",
                    snapshotTimeStr, snapshotMillis, lastSnapshot, datasetName);
            return;
        }
        lastSnapshotTimes.put(datasetName, snapshotMillis);

        // ====================================================================
        // LEVEL 5: Semantic & Field Constraints Validation
        // ====================================================================
        if (!syncMode.equals("DELETE")) {
            JsonNode dataNode = rootNode.get("data");
            if (dataNode == null || !dataNode.isObject()) {
                LOG.warn("ERR_BATCH_MISSING_DATA: Missing or non-object 'data' field for dataset '{}' in sync_mode '{}'", datasetName, syncMode);
                return;
            }

            List<String> errors = new ArrayList<>();
            boolean valid = schema.validateFields(dataNode, syncMode, errors);
            if (!valid) {
                LOG.warn("ERR_BATCH_FIELD_CONSTRAINTS_VIOLATED: Dataset '{}' batch_id '{}' failed field constraints: {}",
                        datasetName, batchId, errors);
                return;
            }
        }

        // Bản tin vượt qua toàn bộ 5 tầng thẩm định
        LOG.debug("Batch event passed 5-level validation: dataset={}, batch_id={}, key={}", datasetName, batchId, normalizedKey);
        out.collect(rootNode.toString());
    }

    private long parseSnapshotTime(String snapshotTimeStr) {
        try {
            return OffsetDateTime.parse(snapshotTimeStr, DateTimeFormatter.ISO_DATE_TIME).toInstant().toEpochMilli();
        } catch (Exception e1) {
            try {
                return java.time.Instant.parse(snapshotTimeStr).toEpochMilli();
            } catch (Exception e2) {
                return System.currentTimeMillis();
            }
        }
    }

    public SchemaRegistry getSchemaRegistry() {
        return schemaRegistry;
    }
}
