package com.vdf.streaming.event;

import com.vdf.streaming.event.batch.*;
import com.vdf.streaming.event.coordinator.CustomerPool;
import com.vdf.streaming.event.error.DataQualityInjector;
import com.vdf.streaming.event.kafka.KafkaSimulatorProducer;
import com.vdf.streaming.event.model.Customer;
import com.vdf.streaming.event.model.EventRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Ứng dụng chính sinh dữ liệu Batch Event cho 7 nguồn batch, đẩy vào cụm kafka-plain:
 *
 * <ul>
 *   <li><b>B1:</b> batch_trial_0d_registered (ĐK2 — đã đăng ký trial 0đ)</li>
 *   <li><b>B1:</b> batch_renewed_subscribers (ĐK3 — loại KH đã từng gia hạn)</li>
 *   <li><b>B2:</b> batch_active_promo_packages (TB đang có gói ưu đãi tới ngày n-1)</li>
 *   <li><b>B4:</b> batch_blacklist_qtrr (Blacklist QTRR — bản ghi mới nhất mỗi msisdn)</li>
 *   <li><b>B4:</b> batch_simfarm_3_tram (Simfarm 3 trạm BTS)</li>
 *   <li><b>B4:</b> batch_cep_pushed_msisdn (Tập msisdn đã đẩy CEP trước đó)</li>
 *   <li><b>B5:</b> batch_vip_customer_list (Danh sách KH vị thế / VIP)</li>
 * </ul>
 *
 * <p>Mỗi bản tin tuân thủ Batch Event Envelope schema (07_BATCH_EVENT_SCHEMA.md),
 * với key_value chuẩn hóa E.164 quốc tế (+84...).
 *
 * <p>Khác biệt với EventSimulatorMain (sinh sự kiện tương quan liên tục),
 * BatchSimulatorMain sinh dữ liệu trạng thái snapshot cho từng dataset một cách độc lập.
 */
public class BatchSimulatorMain {
    private static final Logger log = LoggerFactory.getLogger(BatchSimulatorMain.class);

    public static void main(String[] args) {
        boolean dryRun = false;
        String envPath = null;
        String outputDir = null;
        String batchId = null;
        String snapshotTime = null;
        double errorRate = 0.0;
        double skewRate = 0.0;
        double hotKeyRatio = 0.05;
        int recordsPerDataset = -1;
        Set<String> selectedDatasets = new LinkedHashSet<>();

        // Parse CLI args
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--datasets":
                    if (i + 1 < args.length) {
                        String[] parts = args[++i].split(",");
                        for (String p : parts) selectedDatasets.add(p.trim());
                    }
                    break;
                case "--batch-id":
                    if (i + 1 < args.length) batchId = args[++i];
                    break;
                case "--snapshot-time":
                    if (i + 1 < args.length) snapshotTime = args[++i];
                    break;
                case "--error-rate":
                case "--dirty-rate":
                    if (i + 1 < args.length) {
                        String val = args[++i].replace("%", "").trim();
                        double r = Double.parseDouble(val);
                        errorRate = (r > 1.0) ? r / 100.0 : r;
                    }
                    break;
                case "--skew-rate":
                    if (i + 1 < args.length) {
                        String val = args[++i].replace("%", "").trim();
                        double r = Double.parseDouble(val);
                        skewRate = (r > 1.0) ? r / 100.0 : r;
                    }
                    break;
                case "--hotkey-ratio":
                    if (i + 1 < args.length) {
                        String val = args[++i].replace("%", "").trim();
                        double r = Double.parseDouble(val);
                        hotKeyRatio = (r > 1.0) ? r / 100.0 : r;
                    }
                    break;
                case "--records-per-dataset":
                case "--batch-size":
                    if (i + 1 < args.length) recordsPerDataset = Integer.parseInt(args[++i]);
                    break;
                case "--dry-run":
                    dryRun = true;
                    break;
                case "--env":
                    if (i + 1 < args.length) envPath = args[++i];
                    break;
                case "--output-dir":
                    if (i + 1 < args.length) outputDir = args[++i];
                    break;
                case "-h":
                case "--help":
                    printUsage();
                    return;
            }
        }

        // Giá trị mặc định
        if (batchId == null) {
            batchId = "BATCH_" + LocalDate.now().format(DateTimeFormatter.BASIC_ISO_DATE);
        }
        if (snapshotTime == null) {
            snapshotTime = OffsetDateTime.now(ZoneId.of("Asia/Ho_Chi_Minh"))
                    .format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSXXX"));
        }

        // Banner
        System.out.println("╔════════════════════════════════════════════════════════════════════╗");
        System.out.println("║           BATCH EVENT DATA SIMULATOR (7 BATCH SOURCES)            ║");
        System.out.println("╠════════════════════════════════════════════════════════════════════╣");
        System.out.println("║ Batch ID:     " + String.format("%-52s", batchId) + "║");
        System.out.println("║ Snapshot:     " + String.format("%-52s", snapshotTime) + "║");
        System.out.println("║ Error Rate:   " + String.format("%-52s", (errorRate > 0 ? String.format("%.1f%% (Dirty data injection)", errorRate * 100) : "0% (All clean data)")) + "║");
        System.out.println("║ Data Skew:    " + String.format("%-52s", (skewRate > 0 ? String.format("%.1f%% records dồn vào top %.0f%% Hot Keys", skewRate * 100, hotKeyRatio * 100) : "0% (Uniform distribution)")) + "║");
        if (recordsPerDataset > 0) {
            System.out.println("║ Records/Set:  " + String.format("%-52s", recordsPerDataset + " bản ghi / dataset") + "║");
        }
        System.out.println("║ Dry-run:      " + String.format("%-52s", dryRun) + "║");
        System.out.println("║ Output dir:   " + String.format("%-52s", (outputDir != null ? outputDir : "None")) + "║");
        System.out.println("║ Datasets:     " + String.format("%-52s", (selectedDatasets.isEmpty() ? "ALL (7 datasets)" : String.join(", ", selectedDatasets))) + "║");
        System.out.println("╚════════════════════════════════════════════════════════════════════╝");

        // Khởi tạo Customer Pool (150 KH cố định, deterministic seed)
        Random rand = new Random(42); // Seed cố định để batch luôn sinh cùng tập KH
        CustomerPool basePool = new CustomerPool(150, rand);
        BatchCustomerPool batchPool = new BatchCustomerPool(basePool);

        // Đăng ký 7 generator + mapping sang tập KH tương ứng
        Map<BatchDatasetGenerator, List<Customer>> generatorMap = new LinkedHashMap<>();
        generatorMap.put(new TrialRegisteredGenerator(),     batchPool.getTrialRegistered());
        generatorMap.put(new RenewedSubscribersGenerator(),  batchPool.getRenewedSubscribers());
        generatorMap.put(new ActivePromoPackagesGenerator(), batchPool.getActivePromoPackages());
        generatorMap.put(new BlacklistQtrrGenerator(),       batchPool.getBlacklistQtrr());
        generatorMap.put(new SimfarmGenerator(),             batchPool.getSimfarm3Tram());
        generatorMap.put(new CepPushedMsisdnGenerator(),     batchPool.getCepPushedMsisdn());
        generatorMap.put(new VipCustomerListGenerator(),     batchPool.getVipCustomers());

        // Output dir
        final Path outPath = (outputDir != null) ? Paths.get(outputDir) : null;
        if (outPath != null) {
            try {
                Files.createDirectories(outPath);
            } catch (IOException e) {
                log.error("Không thể tạo thư mục output: {}", e.getMessage());
            }
        }

        // Thống kê
        Map<String, AtomicLong> topicStats = new ConcurrentHashMap<>();
        AtomicLong totalValidRecords = new AtomicLong(0);
        AtomicLong totalDirtyRecords = new AtomicLong(0);
        long totalRecords = 0;

        try (KafkaSimulatorProducer producer = new KafkaSimulatorProducer(dryRun, envPath)) {
            for (Map.Entry<BatchDatasetGenerator, List<Customer>> entry : generatorMap.entrySet()) {
                BatchDatasetGenerator gen = entry.getKey();
                List<Customer> customers = entry.getValue();

                // Lọc theo --datasets nếu có
                if (!selectedDatasets.isEmpty() && !selectedDatasets.contains(gen.getDatasetName())) {
                    log.info("⏭ Bỏ qua dataset: {} (không nằm trong --datasets)", gen.getDatasetName());
                    continue;
                }

                // Phân bổ danh sách khách hàng có xét tới Data Skew
                List<Customer> targetCustomers;
                if (skewRate > 0) {
                    int targetCount = (recordsPerDataset > 0) ? recordsPerDataset : Math.max(customers.size() * 2, 50);
                    targetCustomers = batchPool.generateSkewedList(customers, targetCount, skewRate, hotKeyRatio);
                    List<String> hotKeys = batchPool.getHotMsisdns(customers, hotKeyRatio);
                    log.info("🔥 [Data Skew] Dataset '{}' có {} hot keys dồn {:.0f}% bản ghi: {}",
                            gen.getDatasetName(), hotKeys.size(), skewRate * 100, hotKeys);
                } else if (recordsPerDataset > 0) {
                    targetCustomers = batchPool.generateSkewedList(customers, recordsPerDataset, 0.0, hotKeyRatio);
                } else {
                    targetCustomers = customers;
                }

                log.info("📦 Đang sinh dữ liệu cho dataset: {} ({} bản ghi) → topic: {}",
                        gen.getDatasetName(), targetCustomers.size(), gen.getTargetTopic());

                List<EventRecord> records = gen.generateBatch(targetCustomers, batchId, snapshotTime);

                for (EventRecord record : records) {
                    if (errorRate > 0 && rand.nextDouble() < errorRate) {
                        record = DataQualityInjector.injectBatchError(record);
                        totalDirtyRecords.incrementAndGet();
                    } else {
                        totalValidRecords.incrementAndGet();
                    }

                    producer.send(record);
                    topicStats.computeIfAbsent(record.getTopic(), k -> new AtomicLong(0)).incrementAndGet();
                    totalRecords++;

                    if (outPath != null) {
                        saveToFile(outPath, record);
                    }
                }

                log.info("✅ Hoàn tất dataset: {} — {} bản ghi", gen.getDatasetName(), records.size());
            }

            producer.flush();

        } catch (Exception e) {
            log.error("Lỗi trong quá trình chạy Batch Simulator: {}", e.getMessage(), e);
        }

        // In bảng tổng kết
        printSummary(batchId, snapshotTime, topicStats, totalValidRecords.get(), totalDirtyRecords.get(), totalRecords);
    }

    private static void saveToFile(Path baseDir, EventRecord record) {
        try {
            Path topicFile = baseDir.resolve(record.getTopic() + ".jsonl");
            Files.writeString(topicFile, record.getPayloadJson() + System.lineSeparator(),
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            log.warn("Không thể ghi file cho topic {}: {}", record.getTopic(), e.getMessage());
        }
    }

    private static void printSummary(String batchId, String snapshotTime,
                                     Map<String, AtomicLong> stats,
                                     long validRecords, long dirtyRecords, long totalRecords) {
        System.out.println("\n╔════════════════════════════════════════════════════════════════════╗");
        System.out.println("║                 TỔNG KẾT SINH DỮ LIỆU BATCH                      ║");
        System.out.println("╠════════════════════════════════════════════════════════════════════╣");
        System.out.println(String.format("║ Batch ID:        %-49s ║", batchId));
        System.out.println(String.format("║ Snapshot:        %-49s ║", snapshotTime));
        System.out.println(String.format("║ Bản tin hợp lệ (Valid -> result):      %-27d ║", validRecords));
        System.out.println(String.format("║ Bản tin lỗi (Dirty -> dlq_batch_events): %-25d ║", dirtyRecords));
        if (totalRecords > 0) {
            double actualRate = (dirtyRecords * 100.0) / totalRecords;
            System.out.println(String.format("║ Tỉ lệ lỗi thực tế đạt được:            %-26.2f%% ║", actualRate));
        }
        System.out.println("╟──────────────────────────────────────────┬─────────────────────────╢");
        System.out.println("║ Kafka Topic                              │ Số bản ghi              ║");
        System.out.println("╠══════════════════════════════════════════╪═════════════════════════╣");

        for (Map.Entry<String, AtomicLong> entry : stats.entrySet()) {
            System.out.println(String.format("║ %-40s │ %-23d ║", entry.getKey(), entry.getValue().get()));
        }

        System.out.println("╠══════════════════════════════════════════╪═════════════════════════╣");
        System.out.println(String.format("║ TỔNG CỘNG                                │ %-23d ║", totalRecords));
        System.out.println("╚══════════════════════════════════════════╧═════════════════════════╝\n");
    }

    private static void printUsage() {
        System.out.println("Sử dụng: java -cp ... com.vdf.streaming.event.BatchSimulatorMain [TÙY CHỌN]");
        System.out.println();
        System.out.println("Sinh dữ liệu Batch Event (7 nguồn batch) cho các bài toán B1, B2, B4, B5");
        System.out.println("và đẩy vào cụm kafka-plain. Tuân thủ Batch Event Envelope schema.");
        System.out.println();
        System.out.println("Tùy chọn:");
        System.out.println("  --datasets <list>          Danh sách dataset_name cần sinh (phân cách bởi dấu phẩy)");
        System.out.println("                             VD: --datasets blacklist_qtrr,simfarm_3_tram");
        System.out.println("                             Mặc định: tất cả 7 datasets");
        System.out.println("  --batch-id <id>            Mã mẻ batch (VD: BATCH_20260924). Mặc định: auto theo ngày");
        System.out.println("  --snapshot-time <iso>      Mốc thời gian snapshot ISO-8601. Mặc định: now()");
        System.out.println("  --error-rate <rate>        Tỉ lệ dữ liệu lỗi để test DLQ (vd: 0.05 hoặc 5 cho 5%, mặc định: 0.0)");
        System.out.println("  --skew-rate <rate>         Tỉ lệ bản ghi dồn vào các Hot MSISDNs (vd: 0.8 hoặc 80 cho 80%, mặc định: 0.0)");
        System.out.println("  --hotkey-ratio <ratio>     Tỉ lệ tập khách hàng được coi là Hot Keys (vd: 0.05 hoặc 5 cho 5%, mặc định: 0.05)");
        System.out.println("  --records-per-dataset <n>  Số lượng bản ghi sinh cho mỗi dataset (mặc định: auto)");
        System.out.println("  --dry-run                  Chỉ sinh dữ liệu và in ra log, không gửi mạng tới Kafka");
        System.out.println("  --output-dir <path>        Đường dẫn thư mục lưu các file .jsonl");
        System.out.println("  --env <path>               Đường dẫn tới file .env");
        System.out.println("  -h, --help                 Hiển thị hướng dẫn này");
        System.out.println();
        System.out.println("7 datasets khả dụng:");
        System.out.println("  - trial_0d_registered     (B1 ĐK2: Đăng ký trial 0đ)");
        System.out.println("  - renewed_subscribers     (B1 ĐK3: Đã từng gia hạn)");
        System.out.println("  - active_promo_packages   (B2: Gói cước ưu đãi)");
        System.out.println("  - blacklist_qtrr          (B4: Blacklist QTRR)");
        System.out.println("  - simfarm_3_tram          (B4: Simfarm 3 trạm)");
        System.out.println("  - cep_pushed_msisdn       (B4: Đã đẩy CEP)");
        System.out.println("  - vip_customer_list       (B5: Khách hàng VIP)");
    }
}
