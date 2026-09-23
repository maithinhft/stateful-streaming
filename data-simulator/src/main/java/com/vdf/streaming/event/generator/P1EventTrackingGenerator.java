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

        // 1. Event A: KH bấm Continue (Bài toán A2: Telecom_topup_view_info_user_button_continue)
        ObjectNode continueNode = createEventNode(ctx, cust, "click",
                "Telecom_topup_view_info_user_button_continue", "BUTTON", ctx.getEpochMillis());
        records.add(new EventRecord(TOPIC, KafkaClusterType.GSSAPI, cust.getMsisdn(), continueNode.toString(), ctx));

        // Kịch bản PRODUCT_DROP_OFF (Bài toán A2 - Đứt gãy sản phẩm):
        // KH bấm nút Continue nhưng dừng lại, KHÔNG hoàn tất GD (NOT_FOLLOWED_BY trong 120s) -> Không sinh Event B
        if (ctx.getScenario() == Scenario.PRODUCT_DROP_OFF) {
            return records;
        }

        // 2. Kịch bản OTP_RETRY_SUCCESS: sinh thêm 1 event retry sau 1.5 giây
        if (ctx.getScenario() == Scenario.OTP_RETRY_SUCCESS) {
            ObjectNode retryNode = createEventNode(ctx, cust, "click", "btn_retry_otp", "BUTTON", ctx.getEpochMillis() + 1500L);
            retryNode.put("event_value", "{\"retry\":true,\"previous_error\":\"E01\"}");
            records.add(new EventRecord(TOPIC, KafkaClusterType.GSSAPI, cust.getMsisdn(), retryNode.toString(), ctx));
        }

        // 3. Event B: Kết quả giao dịch (Bài toán A2: Telecom_topup_view_transactionresult_app_view_info)
        // Cùng device_session_id với Event A để Rule A2 SEQUENCE NOT_FOLLOWED_BY có thể join
        ObjectNode resultNode = createEventNode(ctx, cust, "view",
                "Telecom_topup_view_transactionresult_app_view_info", "VIEW", ctx.getEpochMillis() + 2000L);
        records.add(new EventRecord(TOPIC, KafkaClusterType.GSSAPI, cust.getMsisdn(), resultNode.toString(), ctx));

        return records;
    }

    private ObjectNode createEventNode(TransactionContext ctx, Customer cust, String action,
                                       String objectName, String objectType, long timestamp) {
        ObjectNode node = mapper.createObjectNode();

        // Required fields
        node.put("id", UUID.randomUUID().toString());
        node.put("action", action);
        node.put("app_name", "ViettelMoney");
        node.put("app_version", "5.2.1");
        node.put("device_session_id", ctx.getDeviceSessionId());
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

