package com.vdf.streaming.validation;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.vdf.streaming.dynamic.model.KafkaEventRecord;
import com.vdf.streaming.validation.model.FieldDefinition;
import com.vdf.streaming.validation.model.KeyDefinition;
import com.vdf.streaming.validation.model.SchemaDefinition;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class SchemaValidationTest {

    private SchemaRegistry registry;
    private ObjectMapper mapper;

    @BeforeEach
    void setUp() {
        registry = new SchemaRegistry();
        mapper = new ObjectMapper();

        // 1. Đăng ký Stream Schema mẫu: stream_PMT-TRANSACTION-SYNC-CMD_v1
        SchemaDefinition streamSchema = new SchemaDefinition();
        streamSchema.setSchemaId("stream_PMT-TRANSACTION-SYNC-CMD_v1");
        streamSchema.setSchemaType("STREAM");
        streamSchema.setTopic("PMT-TRANSACTION-SYNC-CMD");
        streamSchema.setSource("CPM");
        streamSchema.setVersion("v1");

        KeyDefinition keyDef = new KeyDefinition();
        keyDef.setField("msisdn");
        keyDef.setAutoNormalize(true);
        streamSchema.setKeyDefinition(keyDef);

        Map<String, FieldDefinition> streamFields = new HashMap<>();

        FieldDefinition fTransId = new FieldDefinition();
        fTransId.setType("LONG");
        fTransId.setRequired(true);
        streamFields.put("transDailyHisFinanceId", fTransId);

        FieldDefinition fMsisdn = new FieldDefinition();
        fMsisdn.setType("STRING");
        fMsisdn.setRequired(true);
        streamFields.put("msisdn", fMsisdn);

        FieldDefinition fAmount = new FieldDefinition();
        fAmount.setType("DOUBLE");
        fAmount.setRequired(true);
        fAmount.setMin(0.0);
        streamFields.put("transAmount", fAmount);

        streamSchema.setFields(streamFields);
        registry.registerSchema(streamSchema);

        // 2. Đăng ký Batch Schema mẫu: batch_trial_0d_registered_v1
        SchemaDefinition batchSchema = new SchemaDefinition();
        batchSchema.setSchemaId("batch_trial_0d_registered_v1");
        batchSchema.setSchemaType("BATCH");
        batchSchema.setDatasetName("trial_0d_registered");
        batchSchema.setTopic("batch_trial_0d_registered");
        batchSchema.setVersion("v1");
        batchSchema.setAllowedSyncModes(List.of("FULL_SNAPSHOT", "UPSERT", "DELETE"));

        Map<String, FieldDefinition> batchFields = new HashMap<>();
        FieldDefinition fSubCode = new FieldDefinition();
        fSubCode.setType("STRING");
        fSubCode.setRequired(true);
        fSubCode.setNullable(false);
        fSubCode.setAllowedValues(List.of("VTM1", "VTM2", "VTM4", "VTM5", "VTM6"));
        batchFields.put("sub_code", fSubCode);

        batchSchema.setFields(batchFields);
        registry.registerSchema(batchSchema);
    }

    private static class TestCollector implements Collector<String> {
        final List<String> collected = new ArrayList<>();

        @Override
        public void collect(String record) {
            collected.add(record);
        }

        @Override
        public void close() {}
    }

    @Test
    @DisplayName("Stream Validator: Bản tin hợp lệ, tự động chuẩn hóa msisdn sang E.164")
    void testStreamValidationSuccess() throws Exception {
        StreamSchemaValidationProcessFunction function = new StreamSchemaValidationProcessFunction(registry);
        function.open(new Configuration());

        String json = "{"
                + "\"transDailyHisFinanceId\": 987654321,"
                + "\"msisdn\": \"0981234567\","
                + "\"transAmount\": 50000.0"
                + "}";

        KafkaEventRecord record = new KafkaEventRecord("PMT-TRANSACTION-SYNC-CMD", "0981234567", json, System.currentTimeMillis());
        TestCollector collector = new TestCollector();

        function.processElement(record, null, collector);

        assertEquals(1, collector.collected.size());
        String resultJson = collector.collected.get(0);
        assertTrue(resultJson.contains("\"msisdn\":\"+84981234567\""), "msisdn phải được chuẩn hóa về E.164");
    }

    @Test
    @DisplayName("Stream Validator: Thiếu trường bắt buộc required=true bị loại bỏ")
    void testStreamValidationMissingRequired() throws Exception {
        StreamSchemaValidationProcessFunction function = new StreamSchemaValidationProcessFunction(registry);
        function.open(new Configuration());

        // Thiếu transAmount
        String json = "{"
                + "\"transDailyHisFinanceId\": 987654321,"
                + "\"msisdn\": \"0981234567\""
                + "}";

        KafkaEventRecord record = new KafkaEventRecord("PMT-TRANSACTION-SYNC-CMD", "0981234567", json, System.currentTimeMillis());
        TestCollector collector = new TestCollector();

        function.processElement(record, null, collector);
        assertEquals(0, collector.collected.size(), "Bản tin thiếu required field phải bị loại");
    }

    @Test
    @DisplayName("Batch Validator: Bản tin hợp lệ đầy đủ 5 tầng")
    void testBatchValidationSuccess() throws Exception {
        BatchSchemaValidationProcessFunction function = new BatchSchemaValidationProcessFunction(registry);
        function.open(new Configuration());

        String batchJson = "{"
                + "\"dataset_name\": \"trial_0d_registered\","
                + "\"pipeline_id\": \"HIVE_ETL_TRIAL_0D_REGISTRATION_DAILY\","
                + "\"schema_version\": \"v1\","
                + "\"batch_id\": \"BATCH_20260925_001\","
                + "\"snapshot_time\": \"2026-09-25T02:00:00.000+07:00\","
                + "\"sync_mode\": \"FULL_SNAPSHOT\","
                + "\"key_field\": \"msisdn\","
                + "\"key_value\": \"0981234567\","
                + "\"data\": {\"sub_code\": \"VTM6\"}"
                + "}";

        KafkaEventRecord record = new KafkaEventRecord("batch_trial_0d_registered", "0981234567", batchJson, System.currentTimeMillis());
        TestCollector collector = new TestCollector();

        function.processElement(record, null, collector);

        assertEquals(1, collector.collected.size());
        String out = collector.collected.get(0);
        assertTrue(out.contains("\"key_value\":\"+84981234567\""), "Khóa key_value phải được chuẩn hóa sang E.164");
    }

    @Test
    @DisplayName("Batch Validator: Vi phạm enum allowed_values bị loại bỏ")
    void testBatchValidationEnumViolation() throws Exception {
        BatchSchemaValidationProcessFunction function = new BatchSchemaValidationProcessFunction(registry);
        function.open(new Configuration());

        String batchJson = "{"
                + "\"dataset_name\": \"trial_0d_registered\","
                + "\"pipeline_id\": \"HIVE_ETL_TRIAL_0D_REGISTRATION_DAILY\","
                + "\"schema_version\": \"v1\","
                + "\"batch_id\": \"BATCH_20260925_001\","
                + "\"snapshot_time\": \"2026-09-25T02:00:00.000+07:00\","
                + "\"sync_mode\": \"FULL_SNAPSHOT\","
                + "\"key_field\": \"msisdn\","
                + "\"key_value\": \"0981234567\","
                + "\"data\": {\"sub_code\": \"INVALID_CODE\"}"
                + "}";

        KafkaEventRecord record = new KafkaEventRecord("batch_trial_0d_registered", "0981234567", batchJson, System.currentTimeMillis());
        TestCollector collector = new TestCollector();

        function.processElement(record, null, collector);
        assertEquals(0, collector.collected.size(), "Bản tin vi phạm allowed_values phải bị loại bỏ");
    }

    @Test
    @DisplayName("Batch Validator: Anti-Stale check loại bỏ snapshot cũ hơn")
    void testBatchValidationAntiStale() throws Exception {
        BatchSchemaValidationProcessFunction function = new BatchSchemaValidationProcessFunction(registry);
        function.open(new Configuration());

        // Snapshot 1 lúc 03:00 (Mới hơn)
        String batchJson1 = "{"
                + "\"dataset_name\": \"trial_0d_registered\","
                + "\"pipeline_id\": \"HIVE_ETL_TRIAL_0D_REGISTRATION_DAILY\","
                + "\"schema_version\": \"v1\","
                + "\"batch_id\": \"BATCH_2\","
                + "\"snapshot_time\": \"2026-09-25T03:00:00.000+07:00\","
                + "\"sync_mode\": \"FULL_SNAPSHOT\","
                + "\"key_field\": \"msisdn\","
                + "\"key_value\": \"0981234567\","
                + "\"data\": {\"sub_code\": \"VTM6\"}"
                + "}";

        // Snapshot 2 lúc 01:00 (Cũ hơn -> Stale)
        String batchJson2 = "{"
                + "\"dataset_name\": \"trial_0d_registered\","
                + "\"pipeline_id\": \"HIVE_ETL_TRIAL_0D_REGISTRATION_DAILY\","
                + "\"schema_version\": \"v1\","
                + "\"batch_id\": \"BATCH_1\","
                + "\"snapshot_time\": \"2026-09-25T01:00:00.000+07:00\","
                + "\"sync_mode\": \"FULL_SNAPSHOT\","
                + "\"key_field\": \"msisdn\","
                + "\"key_value\": \"0981234567\","
                + "\"data\": {\"sub_code\": \"VTM6\"}"
                + "}";

        TestCollector collector = new TestCollector();

        // Gửi batch 1 (snapshot mới)
        function.processElement(new KafkaEventRecord("batch_trial_0d_registered", "0981234567", batchJson1, 1), null, collector);
        assertEquals(1, collector.collected.size());

        // Gửi batch 2 (snapshot cũ hơn đến sau)
        function.processElement(new KafkaEventRecord("batch_trial_0d_registered", "0981234567", batchJson2, 2), null, collector);
        assertEquals(1, collector.collected.size(), "Bản tin snapshot cũ hơn phải bị Anti-Stale chặn lại");
    }

    public static void main(String[] args) throws Exception {
        System.out.println("=== Running SchemaValidation Tests ===");
        SchemaValidationTest test = new SchemaValidationTest();

        test.setUp();
        test.testStreamValidationSuccess();

        test.setUp();
        test.testStreamValidationMissingRequired();

        test.setUp();
        test.testBatchValidationSuccess();

        test.setUp();
        test.testBatchValidationEnumViolation();

        test.setUp();
        test.testBatchValidationAntiStale();

        System.out.println(">>> ALL 5 SchemaValidation tests PASSED! <<<");
    }
}
