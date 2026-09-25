package com.vdf.streaming.event.coordinator;

import com.vdf.streaming.event.generator.*;
import com.vdf.streaming.event.model.Customer;
import com.vdf.streaming.event.model.EventRecord;
import com.vdf.streaming.event.model.Scenario;
import com.vdf.streaming.event.model.TransactionContext;

import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Điều phối sinh giao dịch và sinh sự kiện trên toàn bộ 9 nguồn theo kịch bản nghiệp vụ.
 */
public class TransactionCoordinator {
    private final CustomerPool customerPool;
    private final AtomicInteger financeIdCounter = new AtomicInteger(1000);
    private final AtomicInteger requestIdCounter = new AtomicInteger(5000);

    private final List<EventGenerator> generators = new ArrayList<>();

    public record ServiceDef(String serviceCode, String processCode, String masterDetail, String transType) {}

    private static final List<ServiceDef> SERVICE_CATALOG = List.of(
            new ServiceDef("000000", "675000", "TELCOCARD", "TOPUP"),       // Topup nạp tiền ĐT (B5)
            new ServiceDef("MCS098", "610301", "DATAVT", "DATA"),            // Mua gói cước Data (B2, B5)
            new ServiceDef("EVN", "PM1001", "EVN", "PAYMENT"),                // Hóa đơn Điện EVN (B5)
            new ServiceDef("NUOC", "PM1001", "NUOC", "PAYMENT"),              // Hóa đơn Nước (B5)
            new ServiceDef("FLTSEDU", "300001", "EDU", "PAYMENT"),            // Học phí EDU (B5)
            new ServiceDef("VNPAYQR", "QR0000", "VNPAYQR", "PAYMENT"),        // Quét QR thanh toán (B5)
            new ServiceDef("VTM_TRANSFER", "000001", "TRANSFER", "TRANSFER"), // Chuyển tiền VTM (A1, B5)
            new ServiceDef("EPASS", "ETC_PAY_CONFIRM", "EPASS", "PAYMENT")    // Thu phí ePass qua trạm (B4, B5)
    );

    public TransactionCoordinator(CustomerPool customerPool, Random rand) {
        this.customerPool = customerPool;

        // Đăng ký toàn bộ 10 generator tương ứng 10 nguồn sự kiện
        generators.add(new V1InsertTransDailyHisGenerator());
        generators.add(new V1UpdateTransDailyHisGenerator());
        generators.add(new P1EventTrackingGenerator());
        generators.add(new GnotifySaveMessageHbaseGenerator());
        generators.add(new HistoryServiceInsertHbaseProductGenerator());
        generators.add(new HistoryServiceInsertHbaseObjectGenerator());
        generators.add(new CdcnLogCentralProdGenerator());
        generators.add(new AdsThirdPartyGiftDataResultCmdGenerator());
        generators.add(new CoreRechargeHistoryGenerator());
        generators.add(new PmtTransactionSyncCmdGenerator());
    }

    /**
     * Tạo 1 giao dịch mới theo kịch bản ngẫu nhiên và sinh toàn bộ sự kiện liên quan trên 10 nguồn (phân bổ đều).
     */
    public List<EventRecord> generateTransactionEvents(Set<String> filterTopics) {
        return generateTransactionEvents(filterTopics, 0.0, 0.05);
    }

    /**
     * Tạo 1 giao dịch mới có hỗ trợ điều chỉnh tỉ lệ Data Skew dồn vào các Hot MSISDNs.
     */
    public List<EventRecord> generateTransactionEvents(Set<String> filterTopics, double skewRate, double hotKeyRatio) {
        ThreadLocalRandom localRand = ThreadLocalRandom.current();
        Customer customer = (skewRate > 0)
                ? customerPool.getSkewedCustomer(skewRate, hotKeyRatio)
                : customerPool.getRandomCustomer();
        Scenario scenario = Scenario.pickRandom(localRand);

        int financeId = financeIdCounter.incrementAndGet();
        int reqId = requestIdCounter.incrementAndGet();
        String orderId = "ORD" + System.currentTimeMillis() + "_" + financeId;
        String billCode = "BILL" + (100000 + localRand.nextInt(900000));

        ServiceDef sDef = SERVICE_CATALOG.get(localRand.nextInt(SERVICE_CATALOG.size()));
        String service = sDef.serviceCode();
        String processCode = sDef.processCode();
        String masterDetail = sDef.masterDetail();
        String transType = sDef.transType();

        int[] amounts = {20000, 50000, 100000, 200000, 500000, 1000000};
        int transAmount = amounts[localRand.nextInt(amounts.length)];
        int transFee = (transAmount >= 500000) ? 2200 : 0;
        int discount = (localRand.nextInt(10) > 7) ? 5000 : 0;

        String errCode = scenario.getDefaultErrorCode();
        String errMsg = scenario.getDefaultErrorMsg();

        // Kịch bản DIRTY_DATA: cố tình đưa vào các giá trị biên/lỗi định dạng để test data quality
        if (scenario == Scenario.DIRTY_DATA) {
            transAmount = -transAmount; // Số tiền âm
        }

        TransactionContext ctx = new TransactionContext(
                customer, scenario, financeId, reqId, orderId, billCode, service,
                processCode, masterDetail, transType, transAmount, transFee, discount,
                errCode, errMsg, LocalDateTime.now()
        );

        // Trừ/cộng số dư nếu giao dịch thành công (và không phải drop-off)
        if ("00".equals(errCode) && transAmount > 0 && scenario != Scenario.PRODUCT_DROP_OFF) {
            customer.adjustBalance(-ctx.getFinalAmount());
        }

        List<EventRecord> allRecords = new ArrayList<>();
        for (EventGenerator gen : generators) {
            if (filterTopics == null || filterTopics.isEmpty() || filterTopics.contains(gen.getSourceTopic())) {
                // Với kịch bản PRODUCT_DROP_OFF (Bài toán A2): Khách hàng bấm nút Continue trên App nhưng dừng lại,
                // KHÔNG hoàn tất GD (NOT_FOLLOWED_BY trong 120s). Chỉ sinh event click Continue và log gateway,
                // KHÔNG sinh các giao dịch hoàn tất ở backend.
                if (scenario == Scenario.PRODUCT_DROP_OFF) {
                    if (!P1EventTrackingGenerator.TOPIC.equals(gen.getSourceTopic()) &&
                        !CdcnLogCentralProdGenerator.TOPIC.equals(gen.getSourceTopic())) {
                        continue;
                    }
                }

                List<EventRecord> recs = gen.generate(ctx);
                if (recs != null && !recs.isEmpty()) {
                    allRecords.addAll(recs);
                }
            }
        }

        return allRecords;
    }

    public List<EventGenerator> getGenerators() {
        return Collections.unmodifiableList(generators);
    }
}

