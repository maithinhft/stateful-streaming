package com.vdf.streaming.event.batch;

import com.vdf.streaming.event.coordinator.CustomerPool;
import com.vdf.streaming.event.model.Customer;

import java.util.*;

/**
 * Quản lý phân bổ khách hàng từ CustomerPool vào các tập con
 * phục vụ 7 dataset batch (B1, B2, B4, B5).
 *
 * <p>Tái sử dụng danh sách 150 KH cố định từ CustomerPool, phân tập con theo index
 * với tỷ lệ giao thoa thực tế, đảm bảo khi chạy cả Stream Simulator và Batch Simulator
 * cùng lúc, các msisdn của 2 luồng sẽ khớp nhau để kiểm thử logic Rule Engine.
 *
 * <pre>
 * Pool 150 KH (index 0→149):
 * ├── VIP (B5)                : index 0→29   (20%)
 * ├── Trial 0đ (B1 ĐK2)      : index 19→63  (30%) — giao thoa VIP
 * │   └── Đã gia hạn (B1 ĐK3): index 19→43  (subset trial)
 * ├── Gói ưu đãi (B2)        : index 49→98  (33%)
 * ├── Blacklist QTRR (B4)     : index 99→110 (8%)
 * ├── Simfarm 3 trạm (B4)     : index 109→116 (5%) — giao thoa blacklist
 * └── Đã đẩy CEP (B4)         : index 114→133 (13%)
 * </pre>
 */
public class BatchCustomerPool {

    private final List<Customer> allCustomers;

    // Các tập con phân bổ cố định
    private final List<Customer> vipCustomers;
    private final List<Customer> trialRegistered;
    private final List<Customer> renewedSubscribers;
    private final List<Customer> activePromoPackages;
    private final List<Customer> blacklistQtrr;
    private final List<Customer> simfarm3Tram;
    private final List<Customer> cepPushedMsisdn;

    public BatchCustomerPool(CustomerPool basePool) {
        this.allCustomers = basePool.getAll();
        int size = allCustomers.size(); // 150

        // Phân tập con theo index range (deterministic, reproducible)
        this.vipCustomers       = subList(0, Math.min(30, size));
        this.trialRegistered    = subList(Math.min(19, size), Math.min(64, size));
        this.renewedSubscribers = subList(Math.min(19, size), Math.min(44, size));
        this.activePromoPackages= subList(Math.min(49, size), Math.min(99, size));
        this.blacklistQtrr      = subList(Math.min(99, size), Math.min(111, size));
        this.simfarm3Tram       = subList(Math.min(109, size), Math.min(117, size));
        this.cepPushedMsisdn    = subList(Math.min(114, size), Math.min(134, size));
    }

    private List<Customer> subList(int fromIndex, int toIndex) {
        if (fromIndex >= allCustomers.size()) return Collections.emptyList();
        toIndex = Math.min(toIndex, allCustomers.size());
        return new ArrayList<>(allCustomers.subList(fromIndex, toIndex));
    }

    /**
     * Chuẩn hóa msisdn về E.164 quốc tế.
     * CustomerPool sinh "84xxx" → cần thêm "+" phía trước → "+84xxx"
     */
    public static String normalizeE164(String msisdn) {
        if (msisdn == null) return "+84000000000";
        if (msisdn.startsWith("+")) return msisdn;
        if (msisdn.startsWith("84")) return "+" + msisdn;
        if (msisdn.startsWith("0")) return "+84" + msisdn.substring(1);
        return "+" + msisdn;
    }

    // === Getters ===

    public List<Customer> getAllCustomers()          { return allCustomers; }
    public List<Customer> getVipCustomers()          { return vipCustomers; }
    public List<Customer> getTrialRegistered()       { return trialRegistered; }
    public List<Customer> getRenewedSubscribers()    { return renewedSubscribers; }
    public List<Customer> getActivePromoPackages()   { return activePromoPackages; }
    public List<Customer> getBlacklistQtrr()         { return blacklistQtrr; }
    public List<Customer> getSimfarm3Tram()          { return simfarm3Tram; }
    public List<Customer> getCepPushedMsisdn()       { return cepPushedMsisdn; }

    /**
     * Tạo danh sách khách hàng có phân bổ Skew (Hot Keys).
     *
     * @param baseList      Danh sách khách hàng gốc của dataset
     * @param targetCount   Tổng số bản ghi mong muốn sinh ra
     * @param skewRate      Tỉ lệ bản ghi dồn vào hot keys (vd: 0.8 = 80%)
     * @param hotKeyRatio   Tỉ lệ số lượng khách hàng được coi là hot keys (vd: 0.1 = 10%)
     */
    public List<Customer> generateSkewedList(List<Customer> baseList, int targetCount, double skewRate, double hotKeyRatio) {
        if (baseList == null || baseList.isEmpty()) return Collections.emptyList();
        if (targetCount <= 0) targetCount = baseList.size();

        if (skewRate <= 0.0) {
            if (targetCount == baseList.size()) return new ArrayList<>(baseList);
            List<Customer> result = new ArrayList<>(targetCount);
            Random rand = java.util.concurrent.ThreadLocalRandom.current();
            for (int i = 0; i < targetCount; i++) {
                result.add(baseList.get(rand.nextInt(baseList.size())));
            }
            return result;
        }

        int hotCount = Math.max(1, (int) Math.round(baseList.size() * Math.min(1.0, Math.max(0.01, hotKeyRatio))));
        List<Customer> hotSubset = baseList.subList(0, Math.min(hotCount, baseList.size()));
        List<Customer> nonHotSubset = (baseList.size() > hotCount)
                ? baseList.subList(hotCount, baseList.size())
                : baseList;

        List<Customer> result = new ArrayList<>(targetCount);
        Random rand = java.util.concurrent.ThreadLocalRandom.current();
        for (int i = 0; i < targetCount; i++) {
            if (rand.nextDouble() < skewRate) {
                result.add(hotSubset.get(rand.nextInt(hotSubset.size())));
            } else {
                result.add(nonHotSubset.get(rand.nextInt(nonHotSubset.size())));
            }
        }
        return result;
    }

    /**
     * Lấy danh sách số điện thoại Hot MSISDNs của 1 dataset.
     */
    public List<String> getHotMsisdns(List<Customer> baseList, double hotKeyRatio) {
        if (baseList == null || baseList.isEmpty()) return Collections.emptyList();
        int hotCount = Math.max(1, (int) Math.round(baseList.size() * Math.min(1.0, Math.max(0.01, hotKeyRatio))));
        List<String> list = new ArrayList<>();
        for (int i = 0; i < Math.min(hotCount, baseList.size()); i++) {
            list.add(normalizeE164(baseList.get(i).getMsisdn()));
        }
        return list;
    }
}
