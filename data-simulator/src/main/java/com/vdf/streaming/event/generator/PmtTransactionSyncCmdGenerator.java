package com.vdf.streaming.event.generator;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.vdf.streaming.event.kafka.KafkaClusterType;
import com.vdf.streaming.event.model.Customer;
import com.vdf.streaming.event.model.EventRecord;
import com.vdf.streaming.event.model.TransactionContext;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Generator cho nguồn: PMT-TRANSACTION-SYNC-CMD -> kafka-plain
 * Nguồn CPM (Kafka Core Payment) phục vụ bài toán A1 (RT_PSGD thứ 2 - Deduplication đa nguồn).
 *
 * Cấu trúc bản tin: DONG_BO_GIAO_DICH
 * - Khóa liên kết: data.request.originalRequestId trỏ về V1-INSERT-TRANS-DAILY-HIS.requestId
 * - Định danh KH: data.request.identifyValue và content.identifyValue = msisdn
 * - Trạng thái: request.content.errorCode = "SUCCESS" khi giao dịch thành công (theo quy tắc bài toán A1)
 * - data.request.content chứa toàn bộ trường định dạng String (transAmount, transFee dạng str)
 * - data.request.detailSources chứa clientRequestId cho đối soát
 */
public class PmtTransactionSyncCmdGenerator implements EventGenerator {
    public static final String TOPIC = "PMT-TRANSACTION-SYNC-CMD";
    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public String getSourceTopic() {
        return TOPIC;
    }

    @Override
    public List<EventRecord> generate(TransactionContext ctx) {
        List<EventRecord> records = new ArrayList<>();
        Customer cust = ctx.getCustomer();

        ObjectNode root = mapper.createObjectNode();

        // 1. signature luôn null (NoneType)
        root.set("signature", NullNode.getInstance());

        // 2. data node
        ObjectNode data = mapper.createObjectNode();
        data.put("cmd", "DONG_BO_GIAO_DICH");
        data.put("messageId", "MSG_" + UUID.randomUUID());
        data.set("originalPaymentId", NullNode.getInstance());
        data.set("paymentDetailId", NullNode.getInstance());
        data.put("paymentId", "PMT" + ctx.getTransDailyHisFinanceId());
        data.put("time", ctx.getRequestDate());
        data.set("viettelRequestId", NullNode.getInstance());

        // 3. data.request node
        ObjectNode request = mapper.createObjectNode();
        request.put("billingCode", ctx.getBillCode());
        request.put("identifyValue", cust.getMsisdn());
        request.put("logTransaction", true);
        request.put("moneySourceType", "VIETTELPAY");
        request.put("originalRequestId", String.valueOf(ctx.getRequestIdInt()));
        request.put("processCode", ctx.getProcessCode());
        request.put("processName", "Dong bo giao dich " + ctx.getServiceCode());
        request.put("serviceCode", ctx.getServiceCode());
        request.put("serviceName", ctx.getMasterDetail());
        request.put("transDate", ctx.getRequestDate());
        request.put("viettelBankCode", "VTM");

        // detailSources
        ArrayNode detailSources = mapper.createArrayNode();
        ObjectNode sourceItem = mapper.createObjectNode();
        sourceItem.put("moneySourceType", "VIETTELPAY");
        sourceItem.put("sourceCode", "VTM");

        ObjectNode pmtSourceTrans = mapper.createObjectNode();
        pmtSourceTrans.put("clientRequestId", "CRQ_" + ctx.getRequestIdInt());
        pmtSourceTrans.put("amount", ctx.getTransAmount());
        pmtSourceTrans.put("transDate", ctx.getRequestDate());
        sourceItem.set("paymentSourceTransaction", pmtSourceTrans);
        detailSources.add(sourceItem);
        request.set("detailSources", detailSources);

        // 4. data.request.content node (Tất cả giá trị số đều serialize thành String theo phân tích payload)
        ObjectNode content = mapper.createObjectNode();
        content.put("accountId", cust.getAccountNo());
        content.put("accountType", "VTM");
        content.put("additionalInfo", "");
        content.put("billingCode", ctx.getBillCode());

        ThreadLocalRandom rand = ThreadLocalRandom.current();
        boolean isAtm = rand.nextInt(100) < 21; // ~21.2% giao dịch kênh ATM
        content.put("channel", isAtm ? "ATM" : "APP");

        boolean isSuccess = "00".equals(ctx.getErrorCode());
        // Bài toán A1: Lọc request.content.errorCode = SUCCESS
        String errCodeStr = isSuccess ? "SUCCESS" : ctx.getErrorCode();
        content.put("errorCode", errCodeStr);
        content.put("identifyValue", cust.getMsisdn());
        content.put("requestDate", ctx.getRequestDate());
        content.put("requestId", String.valueOf(ctx.getRequestIdInt()));
        content.put("responseDate", ctx.getResponseDate());
        content.put("serviceCode", ctx.getServiceCode());
        content.put("transAmount", String.valueOf(ctx.getTransAmount())); // string type
        content.put("transDesc", "Dong bo giao dich " + ctx.getServiceCode() + " - " + ctx.getOrderId());
        content.put("transFee", String.valueOf(ctx.getTransFee()));       // string type
        content.put("transType", ctx.getTransType());

        // Trường không bắt buộc (Non-required fields):
        // errorCodeDetail (~52.9%)
        if (rand.nextInt(100) < 53) {
            content.put("errorCodeDetail", isSuccess ? "SUCCESS" : ctx.getErrorCodeName());
        }

        // Cụm ATM (atmIdCode, telcoCode, contentDescriptionsService: ~21.2%)
        if (isAtm) {
            content.put("atmIdCode", "ATM_" + (1000 + rand.nextInt(9000)));
            content.put("telcoCode", "VTT");
            content.put("contentDescriptionsService", "Giao dich qua kenh ATM");
        }

        // additionData (~5.9%)
        if (rand.nextInt(100) < 6) {
            content.put("additionData", "{\"traceNo\":\"" + (100000 + rand.nextInt(900000)) + "\"}");
        }

        request.set("content", content);
        data.set("request", request);
        root.set("data", data);

        records.add(new EventRecord(TOPIC, KafkaClusterType.PLAIN, cust.getMsisdn(), root.toString(), ctx));
        return records;
    }
}

