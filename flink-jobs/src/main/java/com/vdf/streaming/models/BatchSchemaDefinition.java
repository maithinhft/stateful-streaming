package com.vdf.streaming.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * Định nghĩa Schema cho tập đặc trưng Batch Event (theo đặc tả 07_BATCH_EVENT_SCHEMA.md).
 * Đại diện cho các schema có schema_type = 'BATCH' trong bảng schema_definitions.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class BatchSchemaDefinition extends SchemaDefinition {

    private static final long serialVersionUID = 1L;

    public BatchSchemaDefinition() {
        super();
        setSchemaType("BATCH");
    }
}
