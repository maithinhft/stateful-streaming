package com.vdf.streaming.event.batch;

import com.vdf.streaming.event.kafka.KafkaClusterType;
import com.vdf.streaming.event.model.Customer;
import com.vdf.streaming.event.model.EventRecord;

import java.util.ArrayList;
import java.util.List;

/**
 * Generator B4 — Blacklist QTRR: Bản ghi mới nhất mỗi msisdn.
 *
 * <p>SQL nguồn:
 * <pre>
 * SELECT msisdn FROM (
 *   SELECT msisdn, status,
 *          row_number() OVER (PARTITION BY msisdn ORDER BY updated_date DESC) AS rn
 *   FROM ${L1_BLACKLIST_QTRR}
 * ) WHERE rn = 1 AND status = 1
 * </pre>
 *
 * <p>Kết quả SELECT chỉ có 1 cột: msisdn
 * → Tập thuần khóa (Set Membership). data payload là {} (rỗng).
 * → Rule B4 dùng IN_DATASET(msisdn) ∈ dataset_blacklist_qtrr để LOẠI TRỪ.
 */
public class BlacklistQtrrGenerator implements BatchDatasetGenerator {

    public static final String DATASET_NAME = "blacklist_qtrr";
    public static final String TOPIC = "batch_blacklist_qtrr";
    public static final String PIPELINE_ID = "HIVE_ETL_BLACKLIST_QTRR_DAILY";

    @Override
    public String getDatasetName() { return DATASET_NAME; }

    @Override
    public String getTargetTopic() { return TOPIC; }

    @Override
    public String getPipelineId() { return PIPELINE_ID; }

    @Override
    public List<EventRecord> generateBatch(List<Customer> targetCustomers, String batchId, String snapshotTime) {
        List<EventRecord> records = new ArrayList<>();

        for (Customer cust : targetCustomers) {
            String msisdnE164 = BatchCustomerPool.normalizeE164(cust.getMsisdn());

            String json = BatchEnvelopeBuilder.buildFullSnapshot(
                    DATASET_NAME, PIPELINE_ID, batchId, snapshotTime,
                    msisdnE164, BatchEnvelopeBuilder.emptyData()
            ).toString();

            records.add(new EventRecord(TOPIC, KafkaClusterType.PLAIN, msisdnE164, json, null));
        }
        return records;
    }
}
