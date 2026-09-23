package com.vdf.streaming.event.generator;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.vdf.streaming.event.kafka.KafkaClusterType;
import com.vdf.streaming.event.model.Customer;
import com.vdf.streaming.event.model.EventRecord;
import com.vdf.streaming.event.model.Scenario;
import com.vdf.streaming.event.model.TransactionContext;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Generator cho nguồn: ADS-THIRD-PARTY-GIFT-DATA-RESULT-CMD -> kafka-plain
 * Callback kết quả nạp thẻ/quà tặng từ đối tác thứ 3.
 * Các trường signature, cmdDesc, paymentId luôn là null theo phân tích thực tế.
 */
public class AdsThirdPartyGiftDataResultCmdGenerator implements EventGenerator {
    public static final String TOPIC = "ADS-THIRD-PARTY-GIFT-DATA-RESULT-CMD";
    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public String getSourceTopic() {
        return TOPIC;
    }

    @Override
    public List<EventRecord> generate(TransactionContext ctx) {
        List<EventRecord> records = new ArrayList<>();
        Customer cust = ctx.getCustomer();

        // 1. Kịch bản OTP_RETRY_SUCCESS: sinh 1 callback lỗi E01 trước, rồi 1 callback thành công sau
        if (ctx.getScenario() == Scenario.OTP_RETRY_SUCCESS) {
            ObjectNode failNode = createCallbackNode(ctx, cust, "E01", "OTP không chính xác", "01", "FAILED", ctx.getTimestamp());
            records.add(new EventRecord(TOPIC, KafkaClusterType.PLAIN, cust.getMsisdn(), failNode.toString(), ctx));

            ObjectNode successNode = createCallbackNode(ctx, cust, "00", "Thành công", "00", "SUCCESS", ctx.getTimestamp().plusSeconds(2));
            records.add(new EventRecord(TOPIC, KafkaClusterType.PLAIN, cust.getMsisdn(), successNode.toString(), ctx));
        } else {
            // Kịch bản bình thường
            String status = "00".equals(ctx.getErrorCode()) ? "00" : "01";
            String statusMsg = "00".equals(ctx.getErrorCode()) ? "SUCCESS" : "FAILED";
            ObjectNode node = createCallbackNode(ctx, cust, ctx.getErrorCode(), ctx.getErrorCodeName(), status, statusMsg, ctx.getTimestamp());
            records.add(new EventRecord(TOPIC, KafkaClusterType.PLAIN, cust.getMsisdn(), node.toString(), ctx));
        }

        return records;
    }

    private ObjectNode createCallbackNode(TransactionContext ctx, Customer cust, String errCode, String errMsg,
                                          String status, String statusMsg, LocalDateTime time) {
        ObjectNode root = mapper.createObjectNode();

        // Field luôn là null theo json_analysis_report.md
        root.set("signature", NullNode.getInstance());

        ObjectNode data = mapper.createObjectNode();
        data.put("cmd", "GIFT_RESULT");
        data.set("cmdDesc", NullNode.getInstance());
        data.put("messageId", "MSG_" + UUID.randomUUID());
        data.set("paymentId", NullNode.getInstance());
        data.put("time", time.format(TransactionContext.SPACE_FMT));

        ObjectNode req = mapper.createObjectNode();
        req.put("billCode", ctx.getBillCode());
        req.put("errorCode", errCode);
        req.put("errorMessage", errMsg);
        req.put("msisdn", cust.getMsisdn());
        req.put("requestId", String.valueOf(ctx.getRequestIdInt()));
        req.put("status", status);
        req.put("statusMessage", statusMsg);
        req.put("transferMoneyRequestId", "TMR" + ctx.getRequestIdInt());

        // Cấu trúc LocalDateTime/JodaTime lồng nhau như phân tích trong context.md
        ObjectNode processDate = mapper.createObjectNode();
        processDate.put("year", time.getYear());
        processDate.put("month", time.getMonth().name());
        processDate.put("monthValue", time.getMonthValue());
        processDate.put("dayOfMonth", time.getDayOfMonth());
        processDate.put("dayOfWeek", time.getDayOfWeek().name());
        processDate.put("dayOfYear", time.getDayOfYear());
        processDate.put("hour", time.getHour());
        processDate.put("minute", time.getMinute());
        processDate.put("second", time.getSecond());
        processDate.put("nano", 0);

        ObjectNode chronology = mapper.createObjectNode();
        chronology.put("id", "ISO");
        chronology.put("calendarType", "iso8601");
        processDate.set("chronology", chronology);

        req.set("processDate", processDate);
        data.set("request", req);
        root.set("data", data);

        return root;
    }
}

