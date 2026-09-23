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
 * Generator cho nguồn: core-recharge-history -> kafka-plain
 * Lịch sử nạp tiền qua liên kết ngân hàng.
 * Mô phỏng case đối soát: PARTNER_TIMEOUT thì debitStatus=SUCCESS nhưng rechargeStatus=FAILED.
 */
public class CoreRechargeHistoryGenerator implements EventGenerator {
    public static final String TOPIC = "core-recharge-history";
    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public String getSourceTopic() {
        return TOPIC;
    }

    @Override
    public List<EventRecord> generate(TransactionContext ctx) {
        Customer cust = ctx.getCustomer();
        ObjectNode node = mapper.createObjectNode();

        String debitStatus = ctx.getDebitStatus();
        String rechargeStatus = ctx.getRechargeStatus();
        String overallStatus = ("SUCCESS".equals(debitStatus) && "SUCCESS".equals(rechargeStatus)) ? "SUCCESS" : "FAILED";

        String debitErrCode = "SUCCESS".equals(debitStatus) ? "00" : ctx.getErrorCode();
        String rechargeErrCode = "SUCCESS".equals(rechargeStatus) ? "00" : (ctx.getScenario() == Scenario.PARTNER_TIMEOUT ? "01" : ctx.getErrorCode());

        // Required fields (100% theo json_analysis_report.md)
        node.put("benAccNo", cust.getAccountNo());
        node.put("benBankCode", "MBBANK");
        node.put("debitErrorCode", debitErrCode);
        node.put("debitLinkType", "DIRECT_LINK");
        node.put("debitStatus", debitStatus);
        node.put("debitTransactionId", "DEBIT_" + ctx.getOrderId());
        node.put("msisdn", cust.getMsisdn());
        node.put("orderId", ctx.getOrderId());
        node.put("rechargeErrorCode", rechargeErrCode);
        node.put("rechargeStatus", rechargeStatus);
        node.put("serviceCode", "RECHARGE_BANK");
        node.put("status", overallStatus);
        node.put("tranContent", "Nạp tiền vào ví VTM qua ngân hàng liên kết MB");
        node.put("transAmount", ctx.getTransAmount());
        node.put("transDate", ctx.getRequestDate());
        node.put("transFee", ctx.getTransFee());

        // Non-required field (81.8% theo report)
        if (!"FAILED".equals(overallStatus) || ctx.getScenario() == Scenario.PARTNER_TIMEOUT) {
            node.put("rechargeTransactionId", "RCG_" + ctx.getOrderId());
        }

        EventRecord record = new EventRecord(TOPIC, KafkaClusterType.PLAIN, cust.getMsisdn(), node.toString(), ctx);
        return List.of(record);
    }
}

