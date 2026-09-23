package com.vdf.streaming.event.generator;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.vdf.streaming.event.kafka.KafkaClusterType;
import com.vdf.streaming.event.model.Customer;
import com.vdf.streaming.event.model.EventRecord;
import com.vdf.streaming.event.model.Scenario;
import com.vdf.streaming.event.model.TransactionContext;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Generator cho nguồn: P1-EVENT-TRACKING -> kafka-gssapi
 * Log clickstream tương tác từ ứng dụng di động.
 */
public class P1EventTrackingGenerator implements EventGenerator {
    public static final String TOPIC = "P1-EVENT-TRACKING";
    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public String getSourceTopic() {
        return TOPIC;
    }

    @Override
    public List<EventRecord> generate(TransactionContext ctx) {
        List<EventRecord> records = new ArrayList<>();
        Customer cust = ctx.getCustomer();

        // 1. Event 'submit' xác nhận thanh toán/chuyển tiền
        ObjectNode submitNode = createEventNode(ctx, cust, "submit", "btn_confirm_payment", "BUTTON", ctx.getEpochMillis());
        records.add(new EventRecord(TOPIC, KafkaClusterType.GSSAPI, cust.getMsisdn(), submitNode.toString(), ctx));

        // 2. Kịch bản OTP_RETRY_SUCCESS: sinh thêm 1 event retry sau 2 giây
        if (ctx.getScenario() == Scenario.OTP_RETRY_SUCCESS) {
            ObjectNode retryNode = createEventNode(ctx, cust, "submit", "btn_retry_otp", "BUTTON", ctx.getEpochMillis() + 2000L);
            retryNode.put("event_value", "{\"retry\":true,\"previous_error\":\"E01\"}");
            records.add(new EventRecord(TOPIC, KafkaClusterType.GSSAPI, cust.getMsisdn(), retryNode.toString(), ctx));
        }

        return records;
    }

    private ObjectNode createEventNode(TransactionContext ctx, Customer cust, String action,
                                       String objectName, String objectType, long timestamp) {
        ObjectNode node = mapper.createObjectNode();

        // Required fields (100% theo json_analysis_report.md)
        node.put("id", UUID.randomUUID().toString());
        node.put("action", action);
        node.put("app_name", "ViettelMoney");
        node.put("app_version", "5.2.1");
        node.put("device_session_id", UUID.randomUUID().toString());
        node.put("event_src", "APP_CLIENT");
        node.put("language", "vi");
        node.put("manufacturer", cust.getDeviceManufacturer());
        node.put("model", cust.getDeviceModel());
        node.put("object_name", objectName);
        node.put("object_type", objectType);
        node.put("os", cust.getOs());
        node.put("os_version", cust.getOsVersion());
        node.put("systemFake", "VTM_CORE");
        node.put("time_stamp", timestamp);
        node.put("time_zone", "Asia/Ho_Chi_Minh");
        node.put("universeFake", "PROD");

        // Non-required fields (tỷ lệ >90% theo report)
        node.put("identity", cust.getMsisdn());
        node.put("imei", cust.getImei());
        node.put("ip_addr", cust.getIpAddr());
        node.put("user_session_id", cust.getUserSessionId());
        node.put("event_value", "{\"orderId\":\"" + ctx.getOrderId() + "\",\"amount\":" + ctx.getFinalAmount() + "}");
        node.put("geo", "21.0285,105.8542");

        return node;
    }
}

