package com.vdf.streaming.event.batch;

import com.vdf.streaming.event.kafka.KafkaClusterType;
import com.vdf.streaming.event.model.Customer;
import com.vdf.streaming.event.model.EventRecord;

import java.util.ArrayList;
import java.util.List;

/**
 * Generator B5 — tap_kh_vi_the_v2: Danh sách KH vị thế (VIP).
 *
 * <p>Nguồn: Bảng tap_kh_vi_the_v2 tại DWH, chứa danh sách KH có vị thế cao
 * (Diamond, Gold, Silver). Dùng để lọc giao dịch thất bại trên 12 nghiệp vụ
 * Viettel Money — chỉ giữ giao dịch của tập KH VIP này đẩy lên dashboard CSKH.
 *
 * <p>Dữ liệu: Chỉ là tập msisdn thuần khóa (Pure Set Membership).
 * → data payload là {} (rỗng).
 * → Rule B5 dùng IN_DATASET(msisdn) ∈ vip_customer_list để lọc giữ lại KH VIP.
 */
public class VipCustomerListGenerator implements BatchDatasetGenerator {

    public static final String DATASET_NAME = "vip_customer_list";
    public static final String TOPIC = "batch_vip_customer_list";
    public static final String PIPELINE_ID = "HIVE_ETL_TAP_KH_VI_THE_V2_DAILY";

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
