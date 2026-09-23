package com.vdf.streaming.event.generator;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.vdf.streaming.event.kafka.KafkaClusterType;
import com.vdf.streaming.event.model.Customer;
import com.vdf.streaming.event.model.EventRecord;
import com.vdf.streaming.event.model.TransactionContext;

import java.util.List;
import java.util.Random;

/**
 * Generator cho nguồn: GNOTIFY_SAVE_MESSAGE_HBASE -> kafka-plain
 * Thông báo biến động số dư giao dịch cộng tiền (CREDIT) theo đặc tả RT_Luồng GNOTI:
 * Phân 3 loại product:
 * 1. nhan_tien: client_code in (NAPAS, VietQR, ViCong, MB, CITAD, BIDV) hoặc VTP + msg_content like 'GD nhan tien%' / '%CHL%' / '%Thanh toan PBH%' / '%#TattoanTK%'
 * 2. nap_tien: VTP + msg_content like '%VTT VTP_REC_%' / '%NAPTIENVIETTELPAY%' / '%NaptienVIETTELPAY%'
 * 3. tra_thuong: VTP + msg_content like '%VIETTELMONEY TRATHUONG%' / '%QUATANGVOUCHER%'
 */
public class GnotifySaveMessageHbaseGenerator implements EventGenerator {
    public static final String TOPIC = "GNOTIFY_SAVE_MESSAGE_HBASE";
    private final ObjectMapper mapper = new ObjectMapper();
    private final Random rand = new Random();

    private static final String[] BANK_CLIENT_CODES = {"NAPAS", "VietQR", "ViCong", "MB", "CITAD", "BIDV"};

    @Override
    public String getSourceTopic() {
        return TOPIC;
    }

    @Override
    public List<EventRecord> generate(TransactionContext ctx) {
        Customer cust = ctx.getCustomer();
        ObjectNode node = mapper.createObjectNode();

        long amount = ctx.getTransAmount();
        String clientCode;
        String msgContent;

        // Quyết định loại product theo tỷ lệ thực tế (nhan_tien 50%, nap_tien 35%, tra_thuong 15%)
        int pRoll = rand.nextInt(100);
        if (pRoll < 50) {
            // 1. nhan_tien
            int variant = rand.nextInt(5);
            switch (variant) {
                case 0 -> {
                    clientCode = BANK_CLIENT_CODES[rand.nextInt(BANK_CLIENT_CODES.length)];
                    msgContent = "GD nhan tien chuyen khoan tu ngan hang " + clientCode + " so tien +" + amount + " VND";
                }
                case 1 -> {
                    clientCode = "VTP";
                    msgContent = "GD nhan tien tu so dien thoai " + cust.getMsisdn() + " so tien +" + amount + " VND";
                }
                case 2 -> {
                    clientCode = "VTP";
                    msgContent = "CHL chi ho luong doanh nghiep doi tac so tien +" + amount + " VND";
                }
                case 3 -> {
                    clientCode = "VTP";
                    msgContent = "Thanh toan PBH chi tra quyen loi bao hiem so tien +" + amount + " VND";
                }
                default -> {
                    clientCode = "VTP";
                    msgContent = "#TattoanTK tiet kiem tai ngan hang so tien +" + amount + " VND";
                }
            }
        } else if (pRoll < 85) {
            // 2. nap_tien
            clientCode = "VTP";
            int variant = rand.nextInt(3);
            switch (variant) {
                case 0 -> msgContent = "VTT VTP_REC_ nap tien tu the Napas thanh cong so tien +" + amount + " VND";
                case 1 -> msgContent = "NAPTIENVIETTELPAY nap tien tu nguon ngan hang lien ket MB so tien +" + amount + " VND";
                default -> msgContent = "NaptienVIETTELPAY nap tien thanh cong vao vi so tien +" + amount + " VND";
            }
        } else {
            // 3. tra_thuong
            clientCode = "VTP";
            int variant = rand.nextInt(2);
            if (variant == 0) {
                msgContent = "VIETTELMONEY TRATHUONG chuong trinh khuyen mai Data 30% so tien +" + amount + " VND";
            } else {
                msgContent = "QUATANGVOUCHER tang voucher mua sam khuyen mai tri gia +" + amount + " VND";
            }
        }

        node.put("msisdn", cust.getMsisdn());
        node.put("clientId", "CLIENT_VTM_01");
        node.put("clientCode", clientCode);
        node.put("client_code", clientCode); // Hỗ trợ cả snake_case cho HBase mapping
        node.put("requestId", String.valueOf(ctx.getRequestIdInt()));
        node.put("orderId", ctx.getOrderId());
        node.put("channelType", "APP_PUSH");
        node.put("msgType", "BALANCE_UPDATE");
        node.put("msgContent", msgContent);
        node.put("msg_content", msgContent); // Hỗ trợ cả snake_case cho HBase mapping
        node.put("templateId", "TMPL_NOTIFY_V1");
        node.put("accountId", cust.getAccountNo());
        node.put("amount", amount);
        node.put("trans_amount", String.valueOf(amount));
        node.put("fee", (long) ctx.getTransFee());
        node.put("balance", cust.getBalance() + amount);
        node.put("bankTransId", "BT" + ctx.getTransDailyHisFinanceId());
        node.put("bank_trans_id", "BT" + ctx.getTransDailyHisFinanceId());
        node.put("description", "Thong bao giao dich cong tien " + ctx.getServiceCode());
        node.put("processCode", ctx.getProcessCode());
        node.put("serviceCode", ctx.getServiceCode());
        node.put("cardType", "DOMESTIC");
        node.put("fullAddress", cust.getAddress());
        node.put("originalBankTransId", "OBT" + ctx.getTransDailyHisFinanceId());
        node.put("sourceBankCode", "MBBANK");
        node.put("sourceAccountNumber", cust.getAccountNo());
        node.put("sourceCustomerName", cust.getCustName());

        // BẮT BUỘC: payment_type = CREDIT theo quy định bài toán A1 & RT_Luồng GNOTI
        node.put("paymentType", "CREDIT");
        node.put("payment_type", "CREDIT");

        node.put("status", "00".equals(ctx.getErrorCode()) ? 1 : 0);
        node.put("transDetailContent", "Chi tiet thanh toan don hang " + ctx.getOrderId());
        node.put("failureCount", "00".equals(ctx.getErrorCode()) ? 0 : 1);
        node.put("pushContent", "GD cong tien thanh cong: +" + amount + "đ");
        node.put("errorCode", ctx.getErrorCode());
        node.put("errorMsg", ctx.getErrorCodeName());
        node.put("additionalInfoOne", "");
        node.put("additionalInfoTwo", "");
        node.put("iosDeeplink", "vtm://transaction/detail?orderId=" + ctx.getOrderId());
        node.put("androidDeeplink", "vtm://transaction/detail?orderId=" + ctx.getOrderId());
        node.put("transDate", ctx.getEpochMillis());
        node.put("trans_date", ctx.getEpochMillis());
        node.put("createdDate", ctx.getEpochMillis());
        node.put("lastModifiedDate", ctx.getEpochMillis());
        node.put("segment", "RETAIL");

        EventRecord record = new EventRecord(TOPIC, KafkaClusterType.PLAIN, cust.getMsisdn(), node.toString(), ctx);
        return List.of(record);
    }
}

