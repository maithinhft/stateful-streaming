package com.vdf.streaming.event;

import com.vdf.streaming.event.coordinator.CustomerPool;
import com.vdf.streaming.event.coordinator.TransactionCoordinator;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Ứng dụng chính sinh dữ liệu realtime cho 9 nguồn sự kiện và đẩy vào 2 cụm Kafka tương ứng:
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
 */
public class EventSimulatorMain {
    private static final Logger log = LoggerFactory.getLogger(EventSimulatorMain.class);

    public static void main(String[] args) {
        String mode = "stream"; // "stream" hoặc "batch"
        double ratePerSec = 2.0; // số giao dịch / giây
        long count = -1; // -1 nghĩa là chạy liên tục
        boolean dryRun = false;
        String envPath = null;
        String outputDir = null;
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

        System.out.println("╔════════════════════════════════════════════════════════════════════╗");
        System.out.println("║          REALTIME EVENT DATA SIMULATOR (9 KAFKA SOURCES)           ║");
        System.out.println("╠════════════════════════════════════════════════════════════════════╣");
        System.out.println("║ Mode:       " + String.format("%-54s", mode.toUpperCase()) + "║");
        System.out.println("║ Rate:       " + String.format("%-54s", ratePerSec + " trans/sec") + "║");
        System.out.println("║ Count:      " + String.format("%-54s", (count < 0 ? "Continuous (Ctrl+C to stop)" : count + " transactions")) + "║");
        System.out.println("║ Dry-run:    " + String.format("%-54s", dryRun) + "║");
        System.out.println("║ Output dir: " + String.format("%-54s", (outputDir != null ? outputDir : "None")) + "║");
        System.out.println("╚════════════════════════════════════════════════════════════════════╝");

        Random rand = new Random();
        CustomerPool customerPool = new CustomerPool(150, rand);
        TransactionCoordinator coordinator = new TransactionCoordinator(customerPool, rand);

        AtomicBoolean running = new AtomicBoolean(true);
        Map<String, AtomicLong> eventStats = new ConcurrentHashMap<>();
        AtomicLong totalTransactions = new AtomicLong(0);

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

        try (KafkaSimulatorProducer producer = new KafkaSimulatorProducer(dryRun, envPath)) {
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                log.info("Nhận tín hiệu dừng, đang hoàn tất các bản ghi dở dang...");
                running.set(false);
                producer.flush();
                printSummary(totalTransactions.get(), eventStats);
            }));

            long delayMs = (ratePerSec > 0) ? (long) (1000.0 / ratePerSec) : 500;
            log.info("Bắt đầu sinh sự kiện với chu kỳ {} ms / giao dịch...", delayMs);

            while (running.get() && (count < 0 || totalTransactions.get() < count)) {
                List<EventRecord> records = coordinator.generateTransactionEvents(selectedSources);
                long currentTx = totalTransactions.incrementAndGet();

                for (EventRecord record : records) {
                    producer.send(record);
                    eventStats.computeIfAbsent(record.getTopic(), k -> new AtomicLong(0)).incrementAndGet();

                    // Ghi ra file nếu có chỉ định outputDir
                    if (outPath != null) {
                        saveToFile(outPath, record);
                    }
                }

                if (currentTx % 10 == 0 || "batch".equalsIgnoreCase(mode)) {
                    log.info("Đã sinh {} giao dịch (tổng số sự kiện: {})",
                            currentTx, eventStats.values().stream().mapToLong(AtomicLong::get).sum());
                }

                if (delayMs > 0) {
                    try {
                        Thread.sleep(delayMs);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }

            producer.flush();
            printSummary(totalTransactions.get(), eventStats);

        } catch (Exception e) {
            log.error("Lỗi trong quá trình chạy Simulator: {}", e.getMessage(), e);
        }
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

    private static void printSummary(long totalTx, Map<String, AtomicLong> stats) {
        System.out.println("\n╔════════════════════════════════════════════════════════════════════╗");
        System.out.println("║                    TỔNG KẾT SINH SỰ KIỆN                           ║");
        System.out.println("╠════════════════════════════════════════════════════════════════════╣");
        System.out.println(String.format("║ Tổng số giao dịch khởi tạo: %-38d ║", totalTx));
        System.out.println("╟─────────────────────────────────────────────┬──────────────────────╢");
        System.out.println("║ Nguồn / Kafka Topic                         │ Số sự kiện đã sinh   ║");
        System.out.println("╠═════════════════════════════════════════════╪══════════════════════╣");

        long totalEvents = 0;
        for (Map.Entry<String, AtomicLong> entry : stats.entrySet()) {
            long c = entry.getValue().get();
            totalEvents += c;
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
        System.out.println("  --rate <n>                 Tốc độ sinh (số giao dịch / giây, mặc định: 2.0)");
        System.out.println("  --count <n>                Tổng số giao dịch cần sinh (mặc định: liên tục cho stream, 50 cho batch)");
        System.out.println("  --dry-run                  Chỉ sinh dữ liệu và in ra log/file, không gửi mạng tới Kafka");
        System.out.println("  --output-dir <path>        Đường dẫn thư mục lưu các sự kiện dạng .jsonl");
        System.out.println("  --sources <list>           Danh sách topic cần sinh (phân cách bởi dấu phẩy, mặc định: toàn bộ 9)");
        System.out.println("  --env <path>               Đường dẫn tới file .env (mặc định: .env ở thư mục gốc)");
        System.out.println("  -h, --help                 Hiển thị hướng dẫn này");
    }
}

