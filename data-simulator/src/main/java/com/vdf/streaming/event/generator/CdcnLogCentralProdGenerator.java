package com.vdf.streaming.event.generator;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.vdf.streaming.event.kafka.KafkaClusterType;
import com.vdf.streaming.event.model.Customer;
import com.vdf.streaming.event.model.EventRecord;
import com.vdf.streaming.event.model.TransactionContext;

import java.util.List;

/**
 * Generator cho nguồn: cdcn_log_central_prod -> kafka-plain
 * Log tập trung từ central API gateway cho các giao dịch.
 */
public class CdcnLogCentralProdGenerator implements EventGenerator {
    public static final String TOPIC = "cdcn_log_central_prod";
    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public String getSourceTopic() {
        return TOPIC;
    }

    @Override
    public List<EventRecord> generate(TransactionContext ctx) {
        Customer cust = ctx.getCustomer();
        ObjectNode node = mapper.createObjectNode();

        long duration = 120L;
        long startTime = ctx.getEpochMillis() - duration;
        long endTime = ctx.getEpochMillis();

        node.put("applicationCode", "VTM_API_GATEWAY");
        node.put("account", cust.getMsisdn());
        node.put("serviceCode", ctx.getServiceCode());
        node.put("threadID", "thread-" + Thread.currentThread().getName());
        node.put("requestID", String.valueOf(ctx.getRequestIdInt()));
        node.put("sessionID", cust.getUserSessionId());
        node.put("ipPortParentNode", "10.240.10.12:8080");
        node.put("ipPortCurrentNode", "10.240.10.18:8080");
        node.put("startTime", startTime);
        node.put("endTime", endTime);
        node.put("requestContent", "{\"msisdn\":\"" + cust.getMsisdn() + "\",\"amount\":" + ctx.getFinalAmount() + "}");
        node.put("duration", duration);
        node.put("errorCode", ctx.getErrorCode());
        node.put("errorDescription", ctx.getErrorCodeName());
        node.put("transactionStatus", "00".equals(ctx.getErrorCode()) ? "SUCCESS" : "FAILED");
        node.put("actionName", "processTransaction");
        node.put("username", "SYSTEM_VTM");
        node.put("threadName", "http-nio-8080-exec-" + (ctx.getRequestIdInt() % 10 + 1));
        node.put("sourceClass", "com.vtm.gateway.TransactionHandler");
        node.put("sourceLine", "154");
        node.put("sourceMethod", "executePayment");
        node.put("serviceProvider", "VIETTEL_DIGITAL");
        node.put("transactionID", ctx.getOrderId());
        node.put("clientRequestID", String.valueOf(ctx.getRequestIdInt()));
        node.put("clientIP", cust.getIpAddr());
        node.put("responseContent", "{\"status\":\"" + ("00".equals(ctx.getErrorCode()) ? "SUCCESS" : "FAILED") + "\"}");
        node.put("transactionType", ctx.getTransType());
        node.put("system", "VTM_CORE");
        node.put("actionType", "ONLINE_PAYMENT");
        node.put("dataType", "JSON");
        node.put("numRecord", "1");
        node.put("correlationID", ctx.getOrderId());
        node.put("imeiTel", cust.getImei());
        node.put("imei", cust.getImei());
        node.put("appVersion", "5.2.1");
        node.put("spanId", "span-" + ctx.getRequestIdInt());
        node.put("typeOS", cust.getOs());
        node.put("osVersion", cust.getOsVersion());
        node.put("userAgent", "ViettelMoney/5.2.1 (" + cust.getDeviceModel() + ")");
        node.put("client", "MOBILE_CLIENT");

        EventRecord record = new EventRecord(TOPIC, KafkaClusterType.PLAIN, cust.getMsisdn(), node.toString(), ctx);
        return List.of(record);
    }
}
