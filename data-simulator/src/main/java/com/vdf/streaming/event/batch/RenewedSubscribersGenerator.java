package com.vdf.streaming.event.batch;

import com.vdf.streaming.event.kafka.KafkaClusterType;
import com.vdf.streaming.event.model.Customer;
import com.vdf.streaming.event.model.EventRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Generator B1 — Dieu_Kien_3: Tập KH đã từng gia hạn (dùng để loại trừ).
 *
 * <p>SQL nguồn: Giống Dieu_Kien_2 nhưng JOIN thêm ${L1_SUB_MNGT_SUB_ORDER_REQUEST}
 * với requestType in (1,2), status = 1,
 * date_format(CREATED_AT,'yyyyMMdd') từ 20250410 đến $[PARTITION_DATE]
 * → dùng để loại tập đã gia hạn, chỉ giữ gia hạn lần đầu.
 *
 * <p>Kết quả SELECT chỉ có 2 cột: msisdn, sub_code
 * → data payload chỉ chứa {"sub_code": "VTMx"}
 */
public class RenewedSubscribersGenerator implements BatchDatasetGenerator {

    public static final String DATASET_NAME = "renewed_subscribers";
    public static final String TOPIC = "batch_renewed_subscribers";
    public static final String PIPELINE_ID = "HIVE_ETL_RENEWAL_HISTORY_DAILY";

    private static final String[] TRIAL_SUB_CODES = {"VTM6", "VTM2", "VTM4", "VTM5"};

    @Override
    public String getDatasetName() { return DATASET_NAME; }

    @Override
    public String getTargetTopic() { return TOPIC; }

    @Override
    public String getPipelineId() { return PIPELINE_ID; }

    @Override
    public List<EventRecord> generateBatch(List<Customer> targetCustomers, String batchId, String snapshotTime) {
        List<EventRecord> records = new ArrayList<>();
        ThreadLocalRandom rand = ThreadLocalRandom.current();

        for (Customer cust : targetCustomers) {
            String subCode = TRIAL_SUB_CODES[rand.nextInt(TRIAL_SUB_CODES.length)];
            String msisdnE164 = BatchCustomerPool.normalizeE164(cust.getMsisdn());

            String json = BatchEnvelopeBuilder.buildFullSnapshot(
                    DATASET_NAME, PIPELINE_ID, batchId, snapshotTime,
                    msisdnE164, BatchEnvelopeBuilder.dataWithSubCode(subCode)
            ).toString();

            records.add(new EventRecord(TOPIC, KafkaClusterType.PLAIN, msisdnE164, json, null));
        }
        return records;
    }
}
