package com.vdf.streaming.event.generator;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.vdf.streaming.event.kafka.KafkaClusterType;
import com.vdf.streaming.event.model.Customer;
import com.vdf.streaming.event.model.EventRecord;
import com.vdf.streaming.event.model.Scenario;
import com.vdf.streaming.event.model.TransactionContext;

import java.util.List;

/**
 * Generator cho nguồn: V1-INSERT-TRANS-DAILY-HIS -> kafka-gssapi
 * Bảng giao dịch lõi (core transaction) của hệ thống.
 */
public class V1InsertTransDailyHisGenerator implements EventGenerator {
    public static final String TOPIC = "V1-INSERT-TRANS-DAILY-HIS";
    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public String getSourceTopic() {
        return TOPIC;
    }

    @Override
    public List<EventRecord> generate(TransactionContext ctx) {
        Customer cust = ctx.getCustomer();
        ObjectNode node = mapper.createObjectNode();

        // 1. Required fields (100% theo json_analysis_report.md)
        node.put("processCode", ctx.getProcessCode());
        node.put("requestContent", "{\"service\":\"" + ctx.getServiceCode() + "\",\"orderId\":\"" + ctx.getOrderId() + "\"}");
        node.put("requestDate", ctx.getRequestDate());
        node.put("requestId", ctx.getRequestIdInt());
        node.put("requestMti", "0200");
        node.put("responseDate", ctx.getResponseDate());

        // 2. Core Business Fields
        node.put("transDailyHisFinanceId", ctx.getTransDailyHisFinanceId());
        node.put("msisdn", cust.getMsisdn());
        node.put("custId", cust.getCustId());
        node.put("custName", cust.getCustName());
        node.put("custMobileNo", cust.getMsisdn());
        node.put("idNo", cust.getIdNo());
        node.put("idType", cust.getIdType());
        node.put("idTypeName", cust.getIdTypeName());
        node.put("gender", cust.getGender());
        node.put("birthday", cust.getBirthday());
        node.put("custAddress", cust.getAddress());
        node.put("mobileId", cust.getCustId());

        // Tiền tệ & kết quả
        node.put("transAmount", ctx.getTransAmount());
        node.put("transFee", ctx.getTransFee());
        node.put("discount", ctx.getDiscount());
        node.put("finalAmount", ctx.getFinalAmount());
        node.put("errorCode", ctx.getErrorCode());
        node.put("errorCodeName", ctx.getErrorCodeName());

        // Khóa điều chỉnh (đáp ứng kịch bản NEEDS_CORRECTION 5% có correctCode='05')
        if (ctx.getScenario() == Scenario.NEEDS_CORRECTION) {
            node.put("correctCode", "05");
            node.put("correctCodeName", "Giao dịch chờ đối soát điều chỉnh");
        } else {
            node.put("correctCode", "00");
            node.put("correctCodeName", "Bình thường");
        }

        node.put("serviceCode", ctx.getServiceCode());
        node.put("transType", ctx.getTransType());
        node.put("transContent", "Giao dich " + ctx.getServiceCode() + " don hang " + ctx.getOrderId());
        node.put("viettelBankCode", "VTM");
        node.put("appId", "VTM");
        node.put("shopCode", "SHOP_ONLINE");
        node.put("shopName", "Viettel Money Online");
        node.put("staffCode", "SYSTEM");
        node.put("staffName", "Hệ thống tự động");
        node.put("accNo", cust.getAccountNo());
        node.put("accName", cust.getCustName());
        node.put("accType", "VTM");
        node.put("accTypeName", "Ví điện tử");
        node.put("telcoCode", "VTT");
        node.put("nationCode", "VNM");
        node.put("nationName", "Việt Nam");
        node.put("languageCode", "vi");
        node.put("languageName", "Tiếng Việt");
        node.put("responseMti", "0210");
        node.put("partnerRequestId", "PARTNER_" + ctx.getOrderId());
        node.put("billCode", ctx.getBillCode());

        // Trường có điều kiện theo transType
        if ("TRANSFER".equalsIgnoreCase(ctx.getTransType())) {
            node.put("benAccNo", "9876543210");
            node.put("benBankCode", "MBBANK");
            node.put("benCustName", "NGUYEN VAN B");
        }

        EventRecord record = new EventRecord(TOPIC, KafkaClusterType.GSSAPI, cust.getMsisdn(), node.toString(), ctx);
        return List.of(record);
    }
}

