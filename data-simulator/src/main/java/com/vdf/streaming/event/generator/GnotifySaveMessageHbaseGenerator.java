package com.vdf.streaming.event.generator;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.vdf.streaming.event.kafka.KafkaClusterType;
import com.vdf.streaming.event.model.Customer;
import com.vdf.streaming.event.model.EventRecord;
import com.vdf.streaming.event.model.TransactionContext;

import java.util.List;

/**
 * Generator cho nguồn: GNOTIFY_SAVE_MESSAGE_HBASE -> kafka-plain
 * Thông báo biến động số dư, tin nhắn thông báo gửi về HBase.
 */
public class GnotifySaveMessageHbaseGenerator implements EventGenerator {
    public static final String TOPIC = "GNOTIFY_SAVE_MESSAGE_HBASE";
    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public String getSourceTopic() {
        return TOPIC;
    }

    @Override
    public List<EventRecord> generate(TransactionContext ctx) {
        Customer cust = ctx.getCustomer();
        ObjectNode node = mapper.createObjectNode();

        node.put("msisdn", cust.getMsisdn());
        node.put("clientId", "CLIENT_VTM_01");
        node.put("clientCode", "VTM_APP");
        node.put("requestId", String.valueOf(ctx.getRequestIdInt()));
        node.put("orderId", ctx.getOrderId());
        node.put("channelType", "APP_PUSH");
        node.put("msgType", "BALANCE_UPDATE");
        node.put("msgContent", "Biến động số dư giao dịch: -" + ctx.getFinalAmount() + " VND cho dịch vụ " + ctx.getServiceCode());
        node.put("templateId", "TMPL_NOTIFY_V1");
        node.put("accountId", cust.getAccountNo());
        node.put("amount", (long) ctx.getTransAmount());
        node.put("fee", (long) ctx.getTransFee());
        node.put("balance", cust.getBalance());
        node.put("bankTransId", "BT" + ctx.getTransDailyHisFinanceId());
        node.put("description", "Thong bao giao dich " + ctx.getServiceCode());
        node.put("processCode", "000001");
        node.put("serviceCode", ctx.getServiceCode());
        node.put("cardType", "DOMESTIC");
        node.put("fullAddress", cust.getAddress());
        node.put("originalBankTransId", "OBT" + ctx.getTransDailyHisFinanceId());
        node.put("sourceBankCode", "MBBANK");
        node.put("sourceAccountNumber", cust.getAccountNo());
        node.put("sourceCustomerName", cust.getCustName());
        node.put("paymentType", "WALLET");
        node.put("status", "00".equals(ctx.getErrorCode()) ? 1 : 0);
        node.put("transDetailContent", "Chi tiet thanh toan don hang " + ctx.getOrderId());
        node.put("failureCount", "00".equals(ctx.getErrorCode()) ? 0 : 1);
        node.put("pushContent", "GD thành công: -" + ctx.getFinalAmount() + "đ");
        node.put("errorCode", ctx.getErrorCode());
        node.put("errorMsg", ctx.getErrorCodeName());
        node.put("additionalInfoOne", "");
        node.put("additionalInfoTwo", "");
        node.put("iosDeeplink", "vtm://transaction/detail?orderId=" + ctx.getOrderId());
        node.put("androidDeeplink", "vtm://transaction/detail?orderId=" + ctx.getOrderId());
        node.put("transDate", ctx.getEpochMillis());
        node.put("createdDate", ctx.getEpochMillis());
        node.put("lastModifiedDate", ctx.getEpochMillis());
        node.put("segment", "RETAIL");

        EventRecord record = new EventRecord(TOPIC, KafkaClusterType.PLAIN, cust.getMsisdn(), node.toString(), ctx);
        return List.of(record);
    }
}

