package com.vdf.streaming.event.generator;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.vdf.streaming.event.kafka.KafkaClusterType;
import com.vdf.streaming.event.model.Customer;
import com.vdf.streaming.event.model.EventRecord;
import com.vdf.streaming.event.model.TransactionContext;

import java.util.List;

/**
 * Generator cho nguồn: history_service_insert_hbase_product -> kafka-plain
 * Lịch sử giao dịch dịch vụ sản phẩm ghi vào HBase.
 */
public class HistoryServiceInsertHbaseProductGenerator implements EventGenerator {
    public static final String TOPIC = "history_service_insert_hbase_product";
    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public String getSourceTopic() {
        return TOPIC;
    }

    @Override
    public List<EventRecord> generate(TransactionContext ctx) {
        Customer cust = ctx.getCustomer();
        ObjectNode node = mapper.createObjectNode();

        node.put("tableName", "history_product");
        node.put("identifyValue", cust.getMsisdn());
        node.put("originalRequestId", String.valueOf(ctx.getRequestIdInt()));
        node.put("transDate", ctx.getRequestDate());
        node.put("content", "{\"service\":\"" + ctx.getServiceCode() + "\",\"orderId\":\"" + ctx.getOrderId() + "\",\"amount\":" + ctx.getFinalAmount() + "}");
        node.put("logTransaction", true);
        node.put("serviceName", ctx.getServiceCode());
        node.put("eventName", "INSERT_PRODUCT_HISTORY");

        EventRecord record = new EventRecord(TOPIC, KafkaClusterType.PLAIN, cust.getMsisdn(), node.toString(), ctx);
        return List.of(record);
    }
}

