package com.vdf.streaming.validation;

import java.io.Serializable;
import java.util.regex.Pattern;

/**
 * Utility chuẩn hóa khóa định danh điện thoại theo chuẩn quốc tế ITU-T E.164.
 *
 * <p>Quy tắc:
 * <ul>
 *   <li>Làm sạch ký tự lạ (khoảng trắng, '-', '.', '(', ')').</li>
 *   <li>Đầu vào bắt đầu bằng '0' (ví dụ: 0981234567) -&gt; chuyển thành '+' + defaultCountryCode + số thuê bao (bỏ 0).</li>
 *   <li>Đầu vào bắt đầu bằng '84' không có '+' -&gt; thêm '+' phía trước (+84...).</li>
 *   <li>Đầu vào đã có '+' phía trước -&gt; giữ nguyên.</li>
 *   <li>Số nội địa không có tiền tố (ví dụ 9 số: 981234567) -&gt; thêm '+' + defaultCountryCode.</li>
 *   <li>Thẩm định định dạng đầu ra theo chuẩn Regex E.164: ^\+[1-9][0-9]{6,14}$</li>
 * </ul>
 */
public class KeyNormalizer implements Serializable {

    private static final long serialVersionUID = 1L;

    public static final String DEFAULT_COUNTRY_CODE = "84";
    public static final String E164_REGEX = "^\\+[1-9][0-9]{6,14}$";
    private static final Pattern E164_PATTERN = Pattern.compile(E164_REGEX);

    /**
     * Chuẩn hóa số điện thoại về E.164 với mã quốc gia mặc định là Việt Nam ("84").
     *
     * @param rawPhone Chuỗi số điện thoại đầu vào
     * @return Chuỗi E.164 hợp lệ (bắt đầu bằng '+'), hoặc null nếu chuỗi không hợp lệ
     */
    public static String normalize(String rawPhone) {
        return normalize(rawPhone, DEFAULT_COUNTRY_CODE);
    }

    /**
     * Chuẩn hóa số điện thoại về E.164 với mã quốc gia tùy chỉnh.
     *
     * @param rawPhone           Chuỗi số điện thoại đầu vào
     * @param defaultCountryCode Mã quốc gia mặc định (ví dụ: "84")
     * @return Chuỗi E.164 hợp lệ (bắt đầu bằng '+'), hoặc null nếu chuỗi không hợp lệ
     */
    public static String normalize(String rawPhone, String defaultCountryCode) {
        if (rawPhone == null) {
            return null;
        }

        String cleaned = rawPhone.trim().replaceAll("[\\s\\-\\.\\(\\)]", "");
        if (cleaned.isEmpty()) {
            return null;
        }

        String cc = (defaultCountryCode != null && !defaultCountryCode.trim().isEmpty())
                ? defaultCountryCode.trim().replaceAll("^\\+", "")
                : DEFAULT_COUNTRY_CODE;

        String normalized;
        if (cleaned.startsWith("+")) {
            normalized = cleaned;
        } else if (cleaned.startsWith("00")) {
            // Chuẩn quay số quốc tế dạng 0084...
            normalized = "+" + cleaned.substring(2);
        } else if (cleaned.startsWith("0")) {
            // Số nội địa 098... -> +8498...
            normalized = "+" + cc + cleaned.substring(1);
        } else if (cleaned.startsWith(cc)) {
            // Bắt đầu bằng mã quốc gia không có + (8498...)
            normalized = "+" + cleaned;
        } else if (cleaned.length() >= 7 && cleaned.length() <= 10) {
            // Số nội địa không có số 0 đầu (ví dụ: 981234567)
            normalized = "+" + cc + cleaned;
        } else {
            normalized = "+" + cleaned;
        }

        if (isValidE164(normalized)) {
            return normalized;
        }

        return null;
    }

    /**
     * Kiểm tra chuỗi có tuân thủ định dạng E.164 (^\+[1-9][0-9]{6,14}$) hay không.
     */
    public static boolean isValidE164(String phone) {
        if (phone == null || phone.isEmpty()) {
            return false;
        }
        return E164_PATTERN.matcher(phone).matches();
    }
}
