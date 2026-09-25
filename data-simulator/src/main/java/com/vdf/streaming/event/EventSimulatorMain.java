package com.vdf.streaming.event;

import com.vdf.streaming.event.coordinator.CustomerPool;
import com.vdf.streaming.event.coordinator.TransactionCoordinator;
import com.vdf.streaming.event.error.DataQualityInjector;
import com.vdf.streaming.event.generator.EventGenerator;
import com.vdf.streaming.event.kafka.KafkaSimulatorProducer;
import com.vdf.streaming.event.model.EventRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

/**
 * Ứng dụng chính sinh dữ liệu realtime cho 10 nguồn sự kiện và đẩy vào 2 cụm Kafka tương ứng:
 *
 * Cụm kafka-gssapi:
 * - V1-UPDATE-TRANS-DAILY-HIS
 * - P1-EVENT-TRACKING
 * - V1-INSERT-TRANS-DAILY-HIS
 *
 * Cụm kafka-plain:
 * - GNOTIFY_SAVE_MESSAGE_HBASE
 * - history_service_insert_hbase_product
 * - HISTORY_SERVICE_INSERT_HBASE_OBJECT
 * - cdcn_log_central_prod
 * - ADS-THIRD-PARTY-GIFT-DATA-RESULT-CMD
 * - core-recharge-history
 * - PMT-TRANSACTION-SYNC-CMD
 */
public class EventSimulatorMain {
    private static final Logger log = LoggerFactory.getLogger(EventSimulatorMain.class);
    private static final Map<String, Object> FILE_LOCKS = new ConcurrentHashMap<>();

    public static void main(String[] args) {
        String mode = "stream"; // "stream" hoặc "batch"
        double ratePerSec = 2.0; // số giao dịch / giây (1 giao dịch ~8-10 sự kiện)
        long count = -1; // -1 nghĩa là chạy liên tục
        int numThreads = Math.max(2, Runtime.getRuntime().availableProcessors());
        boolean dryRun = false;
        String envPath = null;
        String outputDir = null;
        double errorRate = 0.0;
        double skewRate = 0.0;
        double hotKeyRatio = 0.05;
        Set<String> selectedSources = new HashSet<>();

        // Parse CLI args
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--mode":
                    if (i + 1 < args.length) mode = args[++i];
                    break;
                case "--rate":
                    if (i + 1 < args.length) ratePerSec = Double.parseDouble(args[++i]);
                    break;
                case "--count":
                    if (i + 1 < args.length) count = Long.parseLong(args[++i]);
                    break;
                case "--threads":
                    if (i + 1 < args.length) numThreads = Math.max(1, Integer.parseInt(args[++i]));
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
                case "--dry-run":
                    dryRun = true;
                    break;
                case "--env":
                    if (i + 1 < args.length) envPath = args[++i];
                    break;
                case "--output-dir":
                    if (i + 1 < args.length) outputDir = args[++i];
                    break;
                case "--sources":
                    if (i + 1 < args.length) {
                        String[] parts = args[++i].split(",");
                        for (String p : parts) selectedSources.add(p.trim());
                    }
                    break;
                case "-h":
                case "--help":
                    printUsage();
                    return;
            }
        }

        if ("batch".equalsIgnoreCase(mode) && count <= 0) {
            count = 50; // mặc định 50 giao dịch nếu chạy batch
        }

        String rateStr = (ratePerSec <= 0)
                ? "UNLIMITED (Max Throughput Benchmark)"
                : String.format("%.0f trans/s (~%.0f events/s)", ratePerSec, ratePerSec * 8.5);

        Random rand = new Random();
        CustomerPool customerPool = new CustomerPool(150, rand);
        TransactionCoordinator coordinator = new TransactionCoordinator(customerPool, rand);

        System.out.println("╔════════════════════════════════════════════════════════════════════╗");
        System.out.println("║          REALTIME EVENT DATA SIMULATOR (10 KAFKA SOURCES)          ║");
        System.out.println("╠════════════════════════════════════════════════════════════════════╣");
        System.out.println("║ Mode:       " + String.format("%-54s", mode.toUpperCase()) + "║");
        System.out.println("║ Threads:    " + String.format("%-54s", numThreads + " worker threads") + "║");
        System.out.println("║ Target Rate:" + String.format("%-54s", rateStr) + "║");
        System.out.println("║ Count:      " + String.format("%-54s", (count < 0 ? "Continuous (Ctrl+C to stop)" : count + " transactions")) + "║");
        System.out.println("║ Error Rate: " + String.format("%-54s", (errorRate > 0 ? String.format("%.1f%% (Dirty data injection)", errorRate * 100) : "0% (All clean data)")) + "║");
        System.out.println("║ Data Skew:  " + String.format("%-54s", (skewRate > 0 ? String.format("%.1f%% traffic dồn vào top %.0f%% Hot Keys", skewRate * 100, hotKeyRatio * 100) : "0% (Uniform distribution)")) + "║");
        if (skewRate > 0) {
            List<String> hotMsisdns = customerPool.getHotMsisdns(hotKeyRatio);
            String hotStr = hotMsisdns.size() > 4 ? hotMsisdns.subList(0, 4) + "..." : hotMsisdns.toString();
            System.out.println("║ Hot MSISDNs:" + String.format("%-54s", hotStr) + "║");
        }
        System.out.println("║ Dry-run:    " + String.format("%-54s", dryRun) + "║");
        System.out.println("║ Output dir: " + String.format("%-54s", (outputDir != null ? outputDir : "None")) + "║");
        System.out.println("╚════════════════════════════════════════════════════════════════════╝");

        AtomicBoolean running = new AtomicBoolean(true);
        Map<String, AtomicLong> eventStats = new ConcurrentHashMap<>();
        AtomicLong totalTransactions = new AtomicLong(0);
        AtomicLong totalEvents = new AtomicLong(0);
        AtomicLong totalValidEvents = new AtomicLong(0);
        AtomicLong totalDirtyEvents = new AtomicLong(0);

        for (EventGenerator gen : coordinator.getGenerators()) {
            eventStats.put(gen.getSourceTopic(), new AtomicLong(0));
        }

        final Path outPath = (outputDir != null) ? Paths.get(outputDir) : null;
        if (outPath != null) {
            try {
                Files.createDirectories(outPath);
            } catch (IOException e) {
                log.error("Không thể tạo thư mục output: {}", e.getMessage());
            }
        }

        ExecutorService workerPool = Executors.newFixedThreadPool(numThreads);
        ScheduledExecutorService reporter = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "metrics-reporter");
            t.setDaemon(true);
            return t;
        });

        final double targetRate = ratePerSec;
        final long targetCount = count;
        final double finalErrorRate = errorRate;
        final double finalSkewRate = skewRate;
        final double finalHotKeyRatio = hotKeyRatio;

        AtomicBoolean summaryPrinted = new AtomicBoolean(false);

        try (KafkaSimulatorProducer producer = new KafkaSimulatorProducer(dryRun, envPath)) {
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                log.info("Nhận tín hiệu dừng, đang hoàn tất các bản ghi dở dang...");
                running.set(false);
                workerPool.shutdown();
                try {
                    workerPool.awaitTermination(3, TimeUnit.SECONDS);
                } catch (InterruptedException ignored) {}
                reporter.shutdown();
                producer.flush();
                if (summaryPrinted.compareAndSet(false, true)) {
                    printSummary(totalTransactions.get(), totalValidEvents.get(), totalDirtyEvents.get(), eventStats);
                }
            }));

            // Bắt đầu luồng báo cáo Throughput định kỳ mỗi 1 giây
            AtomicLong lastTx = new AtomicLong(0);
            AtomicLong lastEv = new AtomicLong(0);
            reporter.scheduleAtFixedRate(() -> {
                long curTx = totalTransactions.get();
                long curEv = totalEvents.get();
                long dTx = curTx - lastTx.getAndSet(curTx);
                long dEv = curEv - lastEv.getAndSet(curEv);
                log.info("[Throughput] {} trans/s (~{} events/s) | Tổng lũy kế: {} trans, {} events ({} dirty)",
                        dTx, dEv, curTx, curEv, totalDirtyEvents.get());
            }, 1, 1, TimeUnit.SECONDS);

            log.info("Khởi chạy {} worker threads để sinh dữ liệu tải cao...", numThreads);

            // Tính chu kỳ phát sóng nano-giây trên mỗi worker (nếu có giới hạn rate)
            final long intervalNanosPerWorker = (targetRate > 0)
                    ? (long) (1_000_000_000.0 * numThreads / targetRate)
                    : 0;

            List<Future<?>> futures = new ArrayList<>();
            for (int t = 0; t < numThreads; t++) {
                futures.add(workerPool.submit(() -> {
                    long nextEmitTime = System.nanoTime();
                    while (running.get()) {
                        // Kiểm tra giới hạn tổng số giao dịch
                        long curTx = totalTransactions.incrementAndGet();
                        if (targetCount > 0 && curTx > targetCount) {
                            totalTransactions.decrementAndGet();
                            running.set(false);
                            break;
                        }

                        // Điều tiết tốc độ nano-giây (Pacing)
                        if (intervalNanosPerWorker > 0) {
                            long now = System.nanoTime();
                            if (now < nextEmitTime) {
                                long waitNanos = nextEmitTime - now;
                                if (waitNanos > 2_000_000L) { // Nếu thời gian chờ > 2ms thì park luồng
                                    LockSupport.parkNanos(waitNanos - 1_000_000L);
                                }
                                while (System.nanoTime() < nextEmitTime) {
                                    Thread.onSpinWait();
                                }
                            }
                            nextEmitTime = System.nanoTime() + intervalNanosPerWorker;
                        }

                        // Sinh sự kiện của 1 giao dịch trên 10 nguồn (có hỗ trợ Data Skew)
                        List<EventRecord> records = coordinator.generateTransactionEvents(selectedSources, finalSkewRate, finalHotKeyRatio);
                        for (EventRecord record : records) {
                            if (finalErrorRate > 0 && ThreadLocalRandom.current().nextDouble() < finalErrorRate) {
                                record = DataQualityInjector.injectStreamError(record);
                                totalDirtyEvents.incrementAndGet();
                            } else {
                                totalValidEvents.incrementAndGet();
                            }

                            producer.send(record);
                            eventStats.computeIfAbsent(record.getTopic(), k -> new AtomicLong(0)).incrementAndGet();
                            totalEvents.incrementAndGet();

                            if (outPath != null) {
                                saveToFile(outPath, record);
                            }
                        }
                    }
                }));
            }

            // Đợi tất cả workers hoàn thành (áp dụng cho batch mode hoặc khi đạt targetCount)
            for (Future<?> f : futures) {
                try {
                    f.get();
                } catch (Exception ignored) {}
            }

            workerPool.shutdown();
            workerPool.awaitTermination(5, TimeUnit.SECONDS);
            reporter.shutdown();
            producer.flush();
            if (summaryPrinted.compareAndSet(false, true)) {
                printSummary(totalTransactions.get(), totalValidEvents.get(), totalDirtyEvents.get(), eventStats);
            }

        } catch (Exception e) {
            log.error("Lỗi trong quá trình chạy Simulator: {}", e.getMessage(), e);
        } finally {
            workerPool.shutdown();
            reporter.shutdown();
        }
    }

    private static void saveToFile(Path baseDir, EventRecord record) {
        try {
            Path topicFile = baseDir.resolve(record.getTopic() + ".jsonl");
            Object lock = FILE_LOCKS.computeIfAbsent(record.getTopic(), k -> new Object());
            synchronized (lock) {
                Files.writeString(topicFile, record.getPayloadJson() + System.lineSeparator(),
                        StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            }
        } catch (IOException e) {
            log.warn("Không thể ghi file cho topic {}: {}", record.getTopic(), e.getMessage());
        }
    }

    private static void printSummary(long totalTx, long validEvents, long dirtyEvents, Map<String, AtomicLong> stats) {
        System.out.println("\n╔════════════════════════════════════════════════════════════════════╗");
        System.out.println("║                    TỔNG KẾT SINH SỰ KIỆN STREAM                    ║");
        System.out.println("╠════════════════════════════════════════════════════════════════════╣");
        System.out.println(String.format("║ Tổng số giao dịch khởi tạo:        %-31d ║", totalTx));
        System.out.println(String.format("║ Bản tin hợp lệ (Valid -> result):  %-31d ║", validEvents));
        System.out.println(String.format("║ Bản tin lỗi (Dirty -> DLQ):        %-31d ║", dirtyEvents));
        long totalEvents = validEvents + dirtyEvents;
        if (totalEvents > 0) {
            double actualErrorRate = (dirtyEvents * 100.0) / totalEvents;
            System.out.println(String.format("║ Tỉ lệ lỗi thực tế đạt được:        %-30.2f%% ║", actualErrorRate));
        }
        System.out.println("╟─────────────────────────────────────────────┬──────────────────────╢");
        System.out.println("║ Nguồn / Kafka Topic                         │ Số sự kiện đã sinh   ║");
        System.out.println("╠═════════════════════════════════════════════╪══════════════════════╣");

        for (Map.Entry<String, AtomicLong> entry : stats.entrySet()) {
            long c = entry.getValue().get();
            System.out.println(String.format("║ %-43s │ %-20d ║", entry.getKey(), c));
        }

        System.out.println("╠═════════════════════════════════════════════╪══════════════════════╣");
        System.out.println(String.format("║ TỔNG CỘNG TẤT CẢ TOPICS                     │ %-20d ║", totalEvents));
        System.out.println("╚═════════════════════════════════════════════╧══════════════════════╝\n");
    }

    private static void printUsage() {
        System.out.println("Sử dụng: java -cp ... com.vdf.streaming.event.EventSimulatorMain [TÙY CHỌN]");
        System.out.println();
        System.out.println("Tùy chọn:");
        System.out.println("  --mode <stream|batch>      Chế độ sinh (stream: liên tục, batch: số lượng cố định, mặc định: stream)");
        System.out.println("  --threads <n>              Số lượng luồng worker chạy song song (mặc định: số CPU cores, tối thiểu 2)");
        System.out.println("  --rate <n>                 Tốc độ sinh (số giao dịch / giây; mỗi giao dịch ~8-10 events; <=0: tối đa không giới hạn, mặc định: 2.0)");
        System.out.println("  --count <n>                Tổng số giao dịch cần sinh (mặc định: liên tục cho stream, 50 cho batch)");
        System.out.println("  --error-rate <rate>        Tỉ lệ tiêm dữ liệu lỗi/bẩn để test DLQ (vd: 0.05 hoặc 5 cho 5%, mặc định: 0.0)");
        System.out.println("  --skew-rate <rate>         Tỉ lệ lưu lượng dồn vào các số ĐT Hot Keys (vd: 0.8 hoặc 80 cho 80%, mặc định: 0.0)");
        System.out.println("  --hotkey-ratio <ratio>     Tỉ lệ tập khách hàng được chọn làm Hot Keys (vd: 0.05 hoặc 5 cho 5%, mặc định: 0.05)");
        System.out.println("  --dry-run                  Chỉ sinh dữ liệu và in ra log/file, không gửi mạng tới Kafka");
        System.out.println("  --output-dir <path>        Đường dẫn thư mục lưu các sự kiện dạng .jsonl");
        System.out.println("  --sources <list>           Danh sách topic cần sinh (phân cách bởi dấu phẩy, mặc định: toàn bộ 10)");
        System.out.println("  --env <path>               Đường dẫn tới file .env (mặc định: .env ở thư mục gốc)");
        System.out.println("  -h, --help                 Hiển thị hướng dẫn này");
    }
}
