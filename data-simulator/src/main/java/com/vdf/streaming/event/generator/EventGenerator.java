package com.vdf.streaming.event.generator;

import com.vdf.streaming.event.model.EventRecord;
import com.vdf.streaming.event.model.TransactionContext;

import java.util.List;

/**
 * Interface cho các generator sinh sự kiện của từng nguồn cụ thể.
 */
public interface EventGenerator {
    /**
     * Tên nguồn / Topic tương ứng trên Kafka
     */
    String getSourceTopic();

    /**
     * Sinh danh sách các EventRecord từ 1 TransactionContext
     */
    List<EventRecord> generate(TransactionContext ctx);
}

