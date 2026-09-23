package com.vdf.streaming.event.coordinator;

import com.vdf.streaming.event.generator.*;
import com.vdf.streaming.event.model.Customer;
import com.vdf.streaming.event.model.EventRecord;
import com.vdf.streaming.event.model.Scenario;
import com.vdf.streaming.event.model.TransactionContext;

import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Điều phối sinh giao dịch và sinh sự kiện trên toàn bộ 9 nguồn theo kịch bản nghiệp vụ.
 */
public class TransactionCoordinator {
    private final CustomerPool customerPool;
    private final Random rand;
    private final AtomicInteger financeIdCounter = new AtomicInteger(1000);
    private final AtomicInteger requestIdCounter = new AtomicInteger(5000);

    private final List<EventGenerator> generators = new ArrayList<>();
    private static final String[] SERVICES = {
            "VTM_TOPUP", "VTM_BILL_ELECTRIC", "VTM_BILL_WATER",
            "VTM_TRANSFER", "VTM_DATA_3G", "VTM_PAYMENT_QR"
    };

    public TransactionCoordinator(CustomerPool customerPool, Random rand) {
        this.customerPool = customerPool;
        this.rand = rand;

        // Đăng ký toàn bộ 9 generator tương ứng 9 nguồn yêu cầu
        generators.add(new V1InsertTransDailyHisGenerator());
        generators.add(new V1UpdateTransDailyHisGenerator());
        generators.add(new P1EventTrackingGenerator());
        generators.add(new GnotifySaveMessageHbaseGenerator());
        generators.add(new HistoryServiceInsertHbaseProductGenerator());
        generators.add(new HistoryServiceInsertHbaseObjectGenerator());
        generators.add(new CdcnLogCentralProdGenerator());
        generators.add(new AdsThirdPartyGiftDataResultCmdGenerator());
        generators.add(new CoreRechargeHistoryGenerator());
    }

    /**
     * Tạo 1 giao dịch mới theo kịch bản ngẫu nhiên và sinh toàn bộ sự kiện liên quan trên 9 nguồn.
     */
    public List<EventRecord> generateTransactionEvents(Set<String> filterTopics) {
        Customer customer = customerPool.getRandomCustomer();
        Scenario scenario = Scenario.pickRandom(rand);

        int financeId = financeIdCounter.incrementAndGet();
        int reqId = requestIdCounter.incrementAndGet();
        String orderId = "ORD" + System.currentTimeMillis() + "_" + financeId;
        String billCode = "BILL" + (100000 + rand.nextInt(900000));
        String service = SERVICES[rand.nextInt(SERVICES.length)];
        String transType = service.contains("TRANSFER") ? "TRANSFER" :
                (service.contains("TOPUP") ? "TOPUP" : "PAYMENT");

        int[] amounts = {20000, 50000, 100000, 200000, 500000, 1000000};
        int transAmount = amounts[rand.nextInt(amounts.length)];
        int transFee = (transAmount >= 500000) ? 2200 : 0;
        int discount = (rand.nextInt(10) > 7) ? 5000 : 0;

        String errCode = scenario.getDefaultErrorCode();
        String errMsg = scenario.getDefaultErrorMsg();

        // Kịch bản DIRTY_DATA: cố tình đưa vào các giá trị biên/lỗi định dạng để test data quality
        if (scenario == Scenario.DIRTY_DATA) {
            transAmount = -transAmount; // Số tiền âm
        }

        TransactionContext ctx = new TransactionContext(
                customer, scenario, financeId, reqId, orderId, billCode, service,
                transType, transAmount, transFee, discount, errCode, errMsg, LocalDateTime.now()
        );

        // Trừ/cộng số dư nếu giao dịch thành công
        if ("00".equals(errCode) && transAmount > 0) {
            customer.adjustBalance(-ctx.getFinalAmount());
        }

        List<EventRecord> allRecords = new ArrayList<>();
        for (EventGenerator gen : generators) {
            if (filterTopics == null || filterTopics.isEmpty() || filterTopics.contains(gen.getSourceTopic())) {
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

