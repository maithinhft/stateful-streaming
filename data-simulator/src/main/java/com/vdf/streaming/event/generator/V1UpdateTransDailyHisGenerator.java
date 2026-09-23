package com.vdf.streaming.event.generator;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.vdf.streaming.event.kafka.KafkaClusterType;
import com.vdf.streaming.event.model.Customer;
import com.vdf.streaming.event.model.EventRecord;
import com.vdf.streaming.event.model.Scenario;
import com.vdf.streaming.event.model.TransactionContext;

import java.util.Collections;
import java.util.List;

/**
 * Generator cho nguồn: V1-UPDATE-TRANS-DAILY-HIS -> kafka-gssapi
 * Tham chiếu tới transDailyHisFinanceId của V1-INSERT.
 */
public class V1UpdateTransDailyHisGenerator implements EventGenerator {
    public static final String TOPIC = "V1-UPDATE-TRANS-DAILY-HIS";
    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public String getSourceTopic() {
        return TOPIC;
    }

    @Override
    public List<EventRecord> generate(TransactionContext ctx) {
        // Chỉ sinh UPDATE khi kịch bản là NEEDS_CORRECTION (khoảng 5% tổng số giao dịch theo context.md)
        if (ctx.getScenario() != Scenario.NEEDS_CORRECTION) {
            return Collections.emptyList();
        }

        Customer cust = ctx.getCustomer();
        ObjectNode node = mapper.createObjectNode();

        // Required fields (100% theo json_analysis_report.md)
        node.put("transDailyHisFinanceId", ctx.getTransDailyHisFinanceId());
        node.put("requestId", ctx.getRequestIdInt() + 1000000);
        node.put("requestMti", "0200");
        node.put("processCode", "000001");
        node.put("processName", "Điều chỉnh giao dịch đối soát");
        node.put("serviceCode", ctx.getServiceCode());
        node.put("msisdn", cust.getMsisdn());
        node.put("mobileId", cust.getCustId());
        node.put("accNo", cust.getAccountNo());
        node.put("accTypeName", cust.getIdTypeName());
        node.put("appId", "VTM");
        node.put("auditNo", "AUD" + ctx.getTransDailyHisFinanceId());
        node.put("cardNo", "NONE");
        node.put("correctCode", "00");
        node.put("correctCodeName", "Giao dịch đã được điều chỉnh thành công");
        node.put("errorCode", "00");
        node.put("errorCodeName", "Thành công");
        node.put("insertDate", ctx.getRequestDate());
        node.put("requestDate", ctx.getRequestDate());
        node.put("responseDate", ctx.getResponseDate());
        node.put("shopCode", "SHOP_ONLINE");
        node.put("staffCode", "SYSTEM_RECON");
        node.put("transContent", "Dieu chinh giao dich tai chinh " + ctx.getTransDailyHisFinanceId());
        node.put("viettelBankCode", "VTM");

        // Optional fields phổ biến (>80% theo report)
        node.put("custId", cust.getCustId());
        node.put("custName", cust.getCustName());
        node.put("birthday", cust.getBirthday());
        node.put("gender", cust.getGender());
        node.put("idType", cust.getIdType());
        node.put("idTypeName", cust.getIdTypeName());
        node.put("idIssueDate", "2021-05-10");
        node.put("idIssuePlace", "Cuc Canh sat QLHC");
        node.put("accName", cust.getCustName());
        node.put("accType", "VTM");
        node.put("transAmount", ctx.getTransAmount());
        node.put("transFee", ctx.getTransFee());
        node.put("discount", ctx.getDiscount());
        node.put("finalAmount", ctx.getFinalAmount());
        node.put("transType", ctx.getTransType());
        node.put("telcoCode", "VTT");
        node.put("nationCode", "VNM");
        node.put("nationName", "Việt Nam");
        node.put("languageCode", "vi");
        node.put("languageName", "Tiếng Việt");
        node.put("responseMti", "0210");
        node.put("shopName", "Viettel Money Digital");
        node.put("staffName", "He thong tu dong");
        node.put("srcRequestId", ctx.getRequestIdInt());

        EventRecord record = new EventRecord(TOPIC, KafkaClusterType.GSSAPI, cust.getMsisdn(), node.toString(), ctx);
        return List.of(record);
    }
}

