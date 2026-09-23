package com.vdf.streaming.event.model;

/**
 * Entity khách hàng (Customer) trong customer pool.
 * Đóng vai trò làm trục liên kết (msisdn, custId, accountNo...) xuyên suốt tất cả các nguồn dữ liệu.
 */
public class Customer {
    private final int custId;
    private final String msisdn;
    private final String custName;
    private final String idNo;
    private final String idType;
    private final String idTypeName;
    private final String gender;
    private final String birthday;
    private final String address;
    private final String accountNo;
    private final String deviceManufacturer;
    private final String deviceModel;
    private final String os;
    private final String osVersion;
    private final String imei;
    private final String ipAddr;
    private final String userSessionId;
    private long balance;

    public Customer(int custId, String msisdn, String custName, String idNo, String idType, String idTypeName,
                    String gender, String birthday, String address, String accountNo,
                    String deviceManufacturer, String deviceModel, String os, String osVersion,
                    String imei, String ipAddr, String userSessionId, long balance) {
        this.custId = custId;
        this.msisdn = msisdn;
        this.custName = custName;
        this.idNo = idNo;
        this.idType = idType;
        this.idTypeName = idTypeName;
        this.gender = gender;
        this.birthday = birthday;
        this.address = address;
        this.accountNo = accountNo;
        this.deviceManufacturer = deviceManufacturer;
        this.deviceModel = deviceModel;
        this.os = os;
        this.osVersion = osVersion;
        this.imei = imei;
        this.ipAddr = ipAddr;
        this.userSessionId = userSessionId;
        this.balance = balance;
    }

    public int getCustId() { return custId; }
    public String getMsisdn() { return msisdn; }
    public String getCustName() { return custName; }
    public String getIdNo() { return idNo; }
    public String getIdType() { return idType; }
    public String getIdTypeName() { return idTypeName; }
    public String getGender() { return gender; }
    public String getBirthday() { return birthday; }
    public String getAddress() { return address; }
    public String getAccountNo() { return accountNo; }
    public String getDeviceManufacturer() { return deviceManufacturer; }
    public String getDeviceModel() { return deviceModel; }
    public String getOs() { return os; }
    public String getOsVersion() { return osVersion; }
    public String getImei() { return imei; }
    public String getIpAddr() { return ipAddr; }
    public String getUserSessionId() { return userSessionId; }
    public long getBalance() { return balance; }

    public synchronized void adjustBalance(long delta) {
        this.balance = Math.max(0, this.balance + delta);
    }
}

