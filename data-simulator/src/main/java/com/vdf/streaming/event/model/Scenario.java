package com.vdf.streaming.event.model;

import java.util.Random;

/**
 * 7 kịch bản giao dịch (Scenario Catalog) được định nghĩa trong context.md:
 * 1. HAPPY_PATH (70%): Giao dịch thành công ngay lần đầu.
 * 2. INSUFFICIENT_BALANCE (8%): Không đủ số dư (errorCode="02").
 * 3. OTP_RETRY_SUCCESS (6%): Nhập sai OTP lần 1 (fail E01), retry lần 2 thành công.
 * 4. PARTNER_TIMEOUT (5%): Gọi đối tác timeout (debitStatus=SUCCESS, rechargeStatus=FAILED).
 * 5. NEEDS_CORRECTION (5%): Giao dịch cần đối soát/điều chỉnh (INSERT correctCode="05" -> UPDATE correctCode="00").
 * 6. SYSTEM_ERROR (3%): Lỗi hệ thống ngẫu nhiên (errorCode="99").
 * 7. DIRTY_DATA (3%): Dữ liệu bất thường/bẩn để test data quality (msisdn sai format, amount âm...).
 */
public enum Scenario {
    HAPPY_PATH(65, "00", "Thành công"),
    INSUFFICIENT_BALANCE(8, "02", "Không đủ số dư"),
    PRODUCT_DROP_OFF(6, "00", "Đứt gãy sản phẩm (Drop-off)"),
    OTP_RETRY_SUCCESS(5, "00", "Nhập lại OTP thành công"),
    PARTNER_TIMEOUT(5, "01", "Timeout kết nối đối tác"),
    NEEDS_CORRECTION(5, "05", "Giao dịch cần điều chỉnh"),
    SYSTEM_ERROR(3, "99", "Lỗi hệ thống không xác định"),
    DIRTY_DATA(3, "00", "Dữ liệu biên/sai định dạng");

    private final int weight;
    private final String defaultErrorCode;
    private final String defaultErrorMsg;

    Scenario(int weight, String defaultErrorCode, String defaultErrorMsg) {
        this.weight = weight;
        this.defaultErrorCode = defaultErrorCode;
        this.defaultErrorMsg = defaultErrorMsg;
    }

    public int getWeight() { return weight; }
    public String getDefaultErrorCode() { return defaultErrorCode; }
    public String getDefaultErrorMsg() { return defaultErrorMsg; }

    public static Scenario pickRandom(Random rand) {
        int r = rand.nextInt(100);
        int cum = 0;
        for (Scenario s : values()) {
            cum += s.weight;
            if (r < cum) {
                return s;
            }
        }
        return HAPPY_PATH;
    }
}

