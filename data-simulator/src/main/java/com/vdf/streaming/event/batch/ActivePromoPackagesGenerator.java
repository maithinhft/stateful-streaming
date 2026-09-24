package com.vdf.streaming.event.batch;

import com.vdf.streaming.event.kafka.KafkaClusterType;
import com.vdf.streaming.event.model.Customer;
import com.vdf.streaming.event.model.EventRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Generator B2: TB đang có gói ưu đãi hoạt động tới ngày n-1.
 *
 * <p>SQL nguồn:
 * <pre>
 * SELECT cms.ISDN AS msisdn, sub_code
 * FROM ${L1_SUB_MNGT_SUB_ORDER_REQUEST} sor
 * JOIN ${L1_SUB_MNGT_CUST_MAP_SUBS} cms ON sor.CUST_MAP_SUB_ID = cms.id
 * JOIN ${L1_SUB_MNGT_SUB} s             ON cms.SUB_ID = s.id
 * WHERE cms.status = 1 AND cms.IS_DELETE = 0
 *   AND s.SUB_CODE IN ('VTM1','VTM2','VTM3','VTM4')
 *   AND date_format(cms.START_DATE,'yyyyMMdd') <= $[PARTITION_DATE]
 *   AND date_format(cms.END_DATE,  'yyyyMMdd') >= $[PARTITION_DATE]
 * </pre>
 *
 * <p>Kết quả SELECT chỉ có 2 cột: msisdn, sub_code
 * → data payload chỉ chứa {"sub_code": "VTMx"}
 */
public class ActivePromoPackagesGenerator implements BatchDatasetGenerator {

    public static final String DATASET_NAME = "active_promo_packages";
    public static final String TOPIC = "batch_active_promo_packages";
    public static final String PIPELINE_ID = "HIVE_ETL_ACTIVE_PROMO_PACKAGES_DAILY";

    /** Các gói ưu đãi VTM theo điều kiện WHERE s.SUB_CODE IN ('VTM1','VTM2','VTM3','VTM4') */
    private static final String[] PROMO_SUB_CODES = {"VTM1", "VTM2", "VTM3", "VTM4"};

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
            String subCode = PROMO_SUB_CODES[rand.nextInt(PROMO_SUB_CODES.length)];
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
