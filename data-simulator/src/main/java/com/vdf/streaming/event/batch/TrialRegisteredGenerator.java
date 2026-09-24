package com.vdf.streaming.event.batch;

import com.vdf.streaming.event.kafka.KafkaClusterType;
import com.vdf.streaming.event.model.Customer;
import com.vdf.streaming.event.model.EventRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Generator B1 — Dieu_Kien_2: Tập KH đã đăng ký gói trial 0đ.
 *
 * <p>SQL nguồn:
 * <pre>
 * SELECT DISTINCT cms.ISDN AS msisdn, sub_code
 * FROM ${L1_SUB_MNGT_CUST_MAP_SUBS} cms
 * JOIN ${L1_SUB_MNGT_SUB} s         ON s.ID  = cms.SUB_ID
 * JOIN promotion_policy_mapping ppm ON ppm.CUST_MAP_SUB_ID = cms.ID
 * JOIN promotion_policy pp          ON pp.ID = ppm.PROMOTION_POLICY_ID
 * JOIN promotion p                  ON p.ID  = pp.PROMOTION_ID
 * WHERE cms.STATUS = 1 AND cms.IS_DELETE = 0
 *   AND ppm.STATUS = 1
 *   AND s.SUB_CODE = :subCode
 *   AND p.ID       = :promotionId
 *   AND pp.ID      = :promotionPolicyId
 * </pre>
 *
 * <p>Kết quả SELECT chỉ có 2 cột: msisdn, sub_code
 * → data payload chỉ chứa {"sub_code": "VTMx"}
 */
public class TrialRegisteredGenerator implements BatchDatasetGenerator {

    public static final String DATASET_NAME = "trial_0d_registered";
    public static final String TOPIC = "batch_trial_0d_registered";
    public static final String PIPELINE_ID = "HIVE_ETL_TRIAL_0D_REGISTRATION_DAILY";

    /** Các gói trial 0đ theo bảng segment B1 */
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
