package com.vdf.streaming.event.batch;

import com.vdf.streaming.event.kafka.KafkaClusterType;
import com.vdf.streaming.event.model.Customer;
import com.vdf.streaming.event.model.EventRecord;

import java.util.ArrayList;
import java.util.List;

/**
 * Generator B4 — ${29923_CEP}: Tập msisdn đã đẩy sang hệ thống CEP trước đó.
 *
 * <p>Nguồn: Bảng lưu trữ các msisdn đã được đẩy sang CEP trong các chiến dịch
 * trước đó (CTKM ePass hoàn tiền 50%). Mục đích: chống gửi trùng — mỗi KH chỉ
 * được đẩy CEP 1 lần duy nhất.
 *
 * <p>Dữ liệu: Chỉ là tập msisdn thuần khóa (Pure Set Membership).
 * → data payload là {} (rỗng).
 * → Rule B4 dùng IN_DATASET(msisdn) ∈ cep_pushed_msisdn để LOẠI TRỪ.
 */
public class CepPushedMsisdnGenerator implements BatchDatasetGenerator {

    public static final String DATASET_NAME = "cep_pushed_msisdn";
    public static final String TOPIC = "batch_cep_pushed_msisdn";
    public static final String PIPELINE_ID = "HIVE_ETL_29923_CEP_PUSHED_DAILY";

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
