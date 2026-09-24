package com.vdf.streaming.event.batch;

import com.vdf.streaming.event.model.Customer;
import com.vdf.streaming.event.model.EventRecord;

import java.util.List;

/**
 * Interface cho các generator sinh dữ liệu luồng Batch Event.
 *
 * <p>Khác với {@link com.vdf.streaming.event.generator.EventGenerator} (sinh sự kiện tương quan
 * từ 1 giao dịch nghiệp vụ), BatchDatasetGenerator sinh một tập bản ghi snapshot trạng thái
 * cho một nhóm khách hàng, tuân thủ Batch Event Envelope schema.
 *
 * <p>Mỗi generator tương ứng với 1 câu truy vấn SQL ETL từ DWH/Hive/Spark,
 * đẩy vào 1 Kafka topic riêng biệt trên cụm kafka-plain.
 */
public interface BatchDatasetGenerator {

    /**
     * Tên nhóm đặc trưng nghiệp vụ (dataset_name trong Envelope).
     * Ví dụ: "trial_0d_registered", "blacklist_qtrr"
     */
    String getDatasetName();

    /**
     * Kafka topic đích để đẩy bản tin batch.
     * Quy ước: "batch_" + dataset_name
     */
    String getTargetTopic();

    /**
     * Mã định danh tác vụ ETL tại DWH/Hive/Spark (pipeline_id trong Envelope).
     */
    String getPipelineId();

    /**
     * Sinh danh sách EventRecord cho tập khách hàng được chỉ định.
     *
     * @param targetCustomers Danh sách khách hàng thuộc tập con của dataset này
     * @param batchId         Mã mẻ chạy batch (VD: "BATCH_20260924")
     * @param snapshotTime    Mốc thời gian snapshot ISO-8601 (VD: "2026-09-24T02:00:00.000+07:00")
     * @return Danh sách bản ghi sẵn sàng đẩy vào Kafka
     */
    List<EventRecord> generateBatch(List<Customer> targetCustomers, String batchId, String snapshotTime);
}
