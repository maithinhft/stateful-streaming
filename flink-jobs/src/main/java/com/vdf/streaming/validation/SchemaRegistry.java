package com.vdf.streaming.validation;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.vdf.streaming.validation.model.SchemaDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Kho quản lý và tra cứu Schema động cho cả hai luồng Stream và Batch.
 * Tự động nạp toàn bộ cấu hình từ bảng `schema_definitions` của PostgreSQL.
 */
public class SchemaRegistry implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(SchemaRegistry.class);

    private final Map<String, SchemaDefinition> schemasById = new ConcurrentHashMap<>();
    private final Map<String, SchemaDefinition> streamSchemasByTopic = new ConcurrentHashMap<>();
    private final Map<String, SchemaDefinition> streamSchemasBySource = new ConcurrentHashMap<>();
    private final Map<String, SchemaDefinition> batchSchemasByDatasetAndVersion = new ConcurrentHashMap<>();

    private transient ObjectMapper objectMapper;

    public SchemaRegistry() {
        initMapper();
    }

    private ObjectMapper getMapper() {
        if (objectMapper == null) {
            initMapper();
        }
        return objectMapper;
    }

    private void initMapper() {
        this.objectMapper = new ObjectMapper();
        this.objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    /**
     * Nạp toàn bộ định nghĩa Schema từ bảng PostgreSQL schema_definitions.
     *
     * @param dbUrl      JDBC URL (ví dụ: jdbc:postgresql://postgres:5432/realtime_core)
     * @param dbUser     Tên người dùng
     * @param dbPassword Mật khẩu
     */
    public synchronized void loadFromPostgres(String dbUrl, String dbUser, String dbPassword) {
        String query = "SELECT schema_id, schema_payload FROM schema_definitions";
        LOG.info("Connecting to PostgreSQL to load schemas: {}", dbUrl);

        try (Connection conn = DriverManager.getConnection(dbUrl, dbUser, dbPassword);
             PreparedStatement stmt = conn.prepareStatement(query);
             ResultSet rs = stmt.executeQuery()) {

            int count = 0;
            while (rs.next()) {
                String schemaId = rs.getString("schema_id");
                String payloadJson = rs.getString("schema_payload");
                try {
                    SchemaDefinition schema = getMapper().readValue(payloadJson, SchemaDefinition.class);
                    if (schema.getSchemaId() == null || schema.getSchemaId().isEmpty()) {
                        schema.setSchemaId(schemaId);
                    }
                    registerSchema(schema);
                    count++;
                } catch (Exception e) {
                    LOG.warn("Failed to parse schema payload for schema_id='{}': {}", schemaId, e.getMessage());
                }
            }
            LOG.info("Successfully loaded and registered {} schemas from PostgreSQL schema_definitions", count);
        } catch (SQLException e) {
            LOG.error("Failed to query schema_definitions from PostgreSQL ({}): {}", dbUrl, e.getMessage());
        }
    }

    /**
     * Đăng ký một schema vào registry và cập nhật các chỉ mục tra cứu nhanh.
     */
    public void registerSchema(SchemaDefinition schema) {
        if (schema == null || schema.getSchemaId() == null) {
            return;
        }

        schemasById.put(schema.getSchemaId(), schema);

        if (schema.isStream()) {
            if (schema.getTopic() != null && !schema.getTopic().isEmpty()) {
                streamSchemasByTopic.put(schema.getTopic().toUpperCase(), schema);
            }
            if (schema.getSource() != null && !schema.getSource().isEmpty()) {
                streamSchemasBySource.put(schema.getSource().toUpperCase(), schema);
            }
        } else if (schema.isBatch()) {
            String dataset = schema.getDatasetName();
            String version = schema.getVersion() != null ? schema.getVersion() : "v1";
            if (dataset != null && !dataset.isEmpty()) {
                batchSchemasByDatasetAndVersion.put((dataset + "_" + version).toLowerCase(), schema);
                // Mặc định cho version mới nhất nếu không chỉ định version
                batchSchemasByDatasetAndVersion.put(dataset.toLowerCase(), schema);
            }
        }
    }

    /**
     * Tra cứu schema theo schema_id chính xác.
     */
    public SchemaDefinition getSchemaById(String schemaId) {
        if (schemaId == null) return null;
        return schemasById.get(schemaId);
    }

    /**
     * Tra cứu schema cho luồng Realtime Stream theo tên Topic hoặc Source.
     */
    public SchemaDefinition getStreamSchema(String topicOrSource) {
        if (topicOrSource == null) return null;
        String key = topicOrSource.trim().toUpperCase();

        // 1. Thử theo topic
        SchemaDefinition schema = streamSchemasByTopic.get(key);
        if (schema != null) return schema;

        // 2. Thử theo source
        schema = streamSchemasBySource.get(key);
        if (schema != null) return schema;

        // 3. Thử theo schema_id trực tiếp
        schema = schemasById.get("stream_" + topicOrSource + "_v1");
        if (schema != null) return schema;

        return schemasById.get(topicOrSource);
    }

    /**
     * Tra cứu schema cho luồng Batch Event theo dataset_name và version.
     */
    public SchemaDefinition getBatchSchema(String datasetName, String version) {
        if (datasetName == null) return null;
        String v = (version != null && !version.isEmpty()) ? version : "v1";
        String key = (datasetName + "_" + v).toLowerCase();

        SchemaDefinition schema = batchSchemasByDatasetAndVersion.get(key);
        if (schema != null) return schema;

        // Fallback: tra theo schema_id
        schema = schemasById.get("batch_" + key);
        if (schema != null) return schema;

        return batchSchemasByDatasetAndVersion.get(datasetName.toLowerCase());
    }

    public Collection<SchemaDefinition> getAllSchemas() {
        return Collections.unmodifiableCollection(schemasById.values());
    }

    public int size() {
        return schemasById.size();
    }
}
