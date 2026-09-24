package com.vdf.streaming.event.batch;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Utility đóng gói Batch Event Envelope chuẩn theo đặc tả 07_BATCH_EVENT_SCHEMA.md.
 *
 * <p>Cấu trúc Envelope bao gồm:
 * <ul>
 *   <li>dataset_name — Nhóm đặc trưng nghiệp vụ</li>
 *   <li>pipeline_id — Mã tác vụ ETL</li>
 *   <li>schema_version — Phiên bản schema</li>
 *   <li>batch_id — Mã mẻ chạy batch</li>
 *   <li>snapshot_time — Mốc thời gian snapshot (ISO-8601)</li>
 *   <li>sync_mode — FULL_SNAPSHOT / UPSERT / DELETE</li>
 *   <li>key_field — Tên trường khóa (mặc định "msisdn")</li>
 *   <li>key_value — Giá trị khóa (chuẩn E.164)</li>
 *   <li>data — Payload dữ liệu đặc trưng (có thể rỗng {} cho tập thuần khóa)</li>
 * </ul>
 */
public class BatchEnvelopeBuilder {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    /**
     * Đóng gói một bản tin Batch Event hoàn chỉnh.
     *
     * @param datasetName   Tên dataset (namespace trong State)
     * @param pipelineId    Mã tác vụ ETL
     * @param schemaVersion Phiên bản schema (VD: "v1")
     * @param batchId       Mã mẻ chạy batch
     * @param snapshotTime  Mốc thời gian snapshot ISO-8601
     * @param syncMode      Chế độ đồng bộ: FULL_SNAPSHOT / UPSERT / DELETE
     * @param keyField      Tên trường khóa (mặc định "msisdn")
     * @param keyValue      Giá trị khóa (chuẩn E.164)
     * @param data          Payload dữ liệu đặc trưng (có thể rỗng)
     * @return ObjectNode JSON hoàn chỉnh
     */
    public static ObjectNode build(String datasetName, String pipelineId,
                                   String schemaVersion, String batchId,
                                   String snapshotTime, String syncMode,
                                   String keyField, String keyValue,
                                   ObjectNode data) {
        ObjectNode envelope = MAPPER.createObjectNode();
        envelope.put("dataset_name", datasetName);
        envelope.put("pipeline_id", pipelineId);
        envelope.put("schema_version", schemaVersion);
        envelope.put("batch_id", batchId);
        envelope.put("snapshot_time", snapshotTime);
        envelope.put("sync_mode", syncMode);
        envelope.put("key_field", keyField);
        envelope.put("key_value", keyValue);
        envelope.set("data", data);
        return envelope;
    }

    /**
     * Shortcut: Tạo Envelope với key_field mặc định là "msisdn" và sync_mode là "FULL_SNAPSHOT".
     */
    public static ObjectNode buildFullSnapshot(String datasetName, String pipelineId,
                                               String batchId, String snapshotTime,
                                               String msisdn, ObjectNode data) {
        return build(datasetName, pipelineId, "v1", batchId, snapshotTime,
                "FULL_SNAPSHOT", "msisdn", msisdn, data);
    }

    /**
     * Tạo ObjectNode rỗng {} cho các dataset thuần khóa (B4, B5).
     */
    public static ObjectNode emptyData() {
        return MAPPER.createObjectNode();
    }

    /**
     * Tạo ObjectNode với 1 trường sub_code (B1, B2).
     */
    public static ObjectNode dataWithSubCode(String subCode) {
        ObjectNode data = MAPPER.createObjectNode();
        data.put("sub_code", subCode);
        return data;
    }
}
