package com.vdf.streaming.event.batch;

import com.vdf.streaming.event.kafka.KafkaClusterType;
import com.vdf.streaming.event.model.Customer;
import com.vdf.streaming.event.model.EventRecord;

import java.util.ArrayList;
import java.util.List;

/**
 * Generator B4 — Simfarm 3 trạm BTS.
 *
 * <p>SQL nguồn:
 * <pre>
 * SELECT msisdn FROM ${L2_TELCO_BLACKLIST_SIMFARM_3}
 * WHERE partition_month = (
 *   SELECT max(partition_month) FROM ${L2_TELCO_BLACKLIST_SIMFARM_3}
 *   WHERE partition_month <= date_format(to_date('$[PARTITION_DATE]','yyyyMMdd'),'yyyyMM')
 * )
 * </pre>
 *
 * <p>Kết quả SELECT chỉ có 1 cột: msisdn
 * → Tập thuần khóa (Set Membership). data payload là {} (rỗng).
 * → Rule B4 dùng IN_DATASET(msisdn) ∈ dataset_simfarm_3_tram để LOẠI TRỪ.
 */
public class SimfarmGenerator implements BatchDatasetGenerator {

    public static final String DATASET_NAME = "simfarm_3_tram";
    public static final String TOPIC = "batch_simfarm_3_tram";
    public static final String PIPELINE_ID = "SPARK_ETL_SIMFARM_3_TRAM_DAILY";

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
