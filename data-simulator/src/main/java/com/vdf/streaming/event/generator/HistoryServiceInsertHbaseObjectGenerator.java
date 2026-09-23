package com.vdf.streaming.event.generator;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.vdf.streaming.event.kafka.KafkaClusterType;
import com.vdf.streaming.event.model.Customer;
import com.vdf.streaming.event.model.EventRecord;
import com.vdf.streaming.event.model.TransactionContext;

import java.util.List;

/**
 * Generator cho nguồn: HISTORY_SERVICE_INSERT_HBASE_OBJECT -> kafka-plain
 * Lịch sử thao tác object giao dịch ghi vào HBase.
 */
public class HistoryServiceInsertHbaseObjectGenerator implements EventGenerator {
    public static final String TOPIC = "HISTORY_SERVICE_INSERT_HBASE_OBJECT";
    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public String getSourceTopic() {
        return TOPIC;
    }

    @Override
    public List<EventRecord> generate(TransactionContext ctx) {
        Customer cust = ctx.getCustomer();
        ObjectNode node = mapper.createObjectNode();

        node.put("eventName", "INSERT_OBJECT_HISTORY");
        node.put("originalRequestId", String.valueOf(ctx.getRequestIdInt()));
        node.put("transDate", ctx.getRequestDate());
        node.put("identifyValue", cust.getMsisdn());
        node.put("logTransaction", true);
        node.put("tableName", "history_object");
        node.put("serviceName", ctx.getServiceCode());
        node.put("serviceCode", ctx.getServiceCode());
        node.put("paymentId", "PAY_" + ctx.getOrderId());
        node.put("requestId", String.valueOf(ctx.getRequestIdInt()));
        node.put("paymentDetails", "{\"orderId\":\"" + ctx.getOrderId() + "\",\"amount\":" + ctx.getFinalAmount() + ",\"status\":\"" + ctx.getErrorCode() + "\"}");
        node.put("content", "Lưu vết giao dịch chi tiết đối tượng " + ctx.getOrderId());

        EventRecord record = new EventRecord(TOPIC, KafkaClusterType.PLAIN, cust.getMsisdn(), node.toString(), ctx);
        return List.of(record);
    }
}

