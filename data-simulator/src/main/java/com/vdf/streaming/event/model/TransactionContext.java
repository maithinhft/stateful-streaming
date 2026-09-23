package com.vdf.streaming.event.model;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * Ngữ cảnh của 1 giao dịch xuyên suốt tất cả các nguồn sự kiện.
 * Đảm bảo tính nhất quán dữ liệu (referential integrity) giữa 9 topics.
 */
public class TransactionContext {
    public static final DateTimeFormatter ISO_FMT = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");
    public static final DateTimeFormatter SPACE_FMT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private final Customer customer;
    private final Scenario scenario;
    private final int transDailyHisFinanceId;
    private final int requestIdInt;
    private final String orderId;
    private final String billCode;
    private final String serviceCode;
    private final String transType;
    private final int transAmount;
    private final int transFee;
    private final int discount;
    private final int finalAmount;
    private final String errorCode;
    private final String errorCodeName;
    private final LocalDateTime timestamp;
    private final long epochMillis;
    private final String requestDate;
    private final String responseDate;

    // Các cờ kịch bản cụ thể
    private final String debitStatus;
    private final String rechargeStatus;

    public TransactionContext(Customer customer, Scenario scenario, int transDailyHisFinanceId,
                              int requestIdInt, String orderId, String billCode, String serviceCode,
                              String transType, int transAmount, int transFee, int discount,
                              String errorCode, String errorCodeName, LocalDateTime timestamp) {
        this.customer = customer;
        this.scenario = scenario;
        this.transDailyHisFinanceId = transDailyHisFinanceId;
        this.requestIdInt = requestIdInt;
        this.orderId = orderId;
        this.billCode = billCode;
        this.serviceCode = serviceCode;
        this.transType = transType;
        this.transAmount = transAmount;
        this.transFee = transFee;
        this.discount = discount;
        this.finalAmount = Math.max(0, transAmount - discount + transFee);
        this.errorCode = errorCode;
        this.errorCodeName = errorCodeName;
        this.timestamp = timestamp;
        this.epochMillis = timestamp.atZone(java.time.ZoneId.systemDefault()).toInstant().toEpochMilli();
        this.requestDate = timestamp.format(SPACE_FMT);
        this.responseDate = timestamp.plusSeconds(1).format(SPACE_FMT);

        // Kịch bản debit vs recharge
        if (scenario == Scenario.HAPPY_PATH || scenario == Scenario.NEEDS_CORRECTION || scenario == Scenario.OTP_RETRY_SUCCESS) {
            this.debitStatus = "SUCCESS";
            this.rechargeStatus = "SUCCESS";
        } else if (scenario == Scenario.PARTNER_TIMEOUT) {
            this.debitStatus = "SUCCESS";
            this.rechargeStatus = "FAILED";
        } else {
            this.debitStatus = "FAILED";
            this.rechargeStatus = "FAILED";
        }
    }

    public Customer getCustomer() { return customer; }
    public Scenario getScenario() { return scenario; }
    public int getTransDailyHisFinanceId() { return transDailyHisFinanceId; }
    public int getRequestIdInt() { return requestIdInt; }
    public String getOrderId() { return orderId; }
    public String getBillCode() { return billCode; }
    public String getServiceCode() { return serviceCode; }
    public String getTransType() { return transType; }
    public int getTransAmount() { return transAmount; }
    public int getTransFee() { return transFee; }
    public int getDiscount() { return discount; }
    public int getFinalAmount() { return finalAmount; }
    public String getErrorCode() { return errorCode; }
    public String getErrorCodeName() { return errorCodeName; }
    public LocalDateTime getTimestamp() { return timestamp; }
    public long getEpochMillis() { return epochMillis; }
    public String getRequestDate() { return requestDate; }
    public String getResponseDate() { return responseDate; }
    public String getDebitStatus() { return debitStatus; }
    public String getRechargeStatus() { return rechargeStatus; }
}

