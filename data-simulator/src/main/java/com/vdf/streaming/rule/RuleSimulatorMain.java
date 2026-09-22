package com.vdf.streaming.rule;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;

/**
 * Main sinh rule JSON ngẫu nhiên và ghi ra file.
 * CHỈ gen data, KHÔNG ghi PostgreSQL.
 *
 * Chỉnh trực tiếp các tham số đầu vào bên dưới, không cần file properties.
 */
public class RuleSimulatorMain {

    // ====================================================================
    //  CẤU HÌNH THAM SỐ ĐẦU VÀO — CHỈNH TRỰC TIẾP TẠI ĐÂY
    // ====================================================================

    /** Số lượng rule cần sinh */
    static final int NUM_RULES = 100;

    /** Chiều cao tối đa của cây condition_tree */
    static final int MAX_TREE_HEIGHT = 3;

    /** Danh sách source (mapping từ 7 bài toán A1, A2, B1-B5) */
    static final List<String> SOURCES = List.of(
            "TDH", "EVT", "GNOTI", "CPM",       // A1: RT_PSGD thứ 2
            "PMT",                                // B1: hoàn tiền trial
            "SUB_MNGT",                           // B2: kích thích mua gói
            "ADS",                                // B3: tặng gói cước data
            "TMS",                                // B4: CTKM ePass
            "CDCN", "TOPUP", "COREPAY", "SAVING"  // B5: Dashboard KH vị thế
    );

    /** Danh sách version */
    static final List<String> VERSIONS = List.of("v1", "v2", "v3");

    /** Số source tối đa có trong trigger của 1 rule */
    static final int MAX_SOURCES_PER_TRIGGER = 3;

    // ====================================================================
    //  CẤU HÌNH OUTPUT DIRECTORY
    // ====================================================================

    /** Đường dẫn gốc output (tương đối so với project root) */
    static final String OUTPUT_BASE_DIR = "local/data/rules";

    // ====================================================================

    public static void main(String[] args) throws IOException {
        System.out.println("╔════════════════════════════════════════════════╗");
        System.out.println("║         RULE DATA GENERATOR                    ║");
        System.out.println("╠════════════════════════════════════════════════╣");
        System.out.printf("║  Num Rules           : %-23d║%n", NUM_RULES);
        System.out.printf("║  Max Tree Height     : %-23d║%n", MAX_TREE_HEIGHT);
        System.out.printf("║  Sources             : %-23s║%n", SOURCES);
        System.out.printf("║  Versions            : %-23s║%n", VERSIONS);
        System.out.printf("║  Max Sources/Trigger : %-23d║%n", MAX_SOURCES_PER_TRIGGER);
        System.out.println("╚════════════════════════════════════════════════╝");

        // 1. Resolve output directory
        Path projectRoot = findProjectRoot();
        Path outputDir = projectRoot.resolve(OUTPUT_BASE_DIR).resolve(String.valueOf(NUM_RULES));
        Files.createDirectories(outputDir);
        System.out.println("\n  Output directory: " + outputDir.toAbsolutePath());

        // 2. Khởi tạo generator
        RuleGenerator generator = new RuleGenerator(SOURCES, VERSIONS, MAX_TREE_HEIGHT, MAX_SOURCES_PER_TRIGGER);

        // 3. Sinh tất cả rules vào 1 mảng JSON
        long baseTimestamp = Instant.now().getEpochSecond();
        ArrayNode allRules = generator.getMapper().createArrayNode();

        long genStart = System.currentTimeMillis();
        for (int i = 0; i < NUM_RULES; i++) {
            ObjectNode rule = generator.generateRule(i, baseTimestamp);
            allRules.add(rule);

            if ((i + 1) % 100 == 0 || i == NUM_RULES - 1) {
                System.out.printf("  [Gen] Generated %d/%d rules%n", i + 1, NUM_RULES);
            }
        }

        // 4. Ghi chung 1 file JSON — tên file = timestamp dễ đọc (yyyyMMdd_HHmmss)
        DateTimeFormatter fileNameFormatter = DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss")
                .withZone(ZoneId.of("Asia/Ho_Chi_Minh"));
        String fileName = fileNameFormatter.format(Instant.ofEpochSecond(baseTimestamp)) + ".json";
        Path filePath = outputDir.resolve(fileName);
        String json = generator.getMapper().writeValueAsString(allRules);
        Files.writeString(filePath, json);

        long genElapsed = System.currentTimeMillis() - genStart;
        System.out.printf("\n  ✓ Done: %d rules generated in %d ms%n", NUM_RULES, genElapsed);
        System.out.println("  Output: " + filePath.toAbsolutePath());
    }

    /**
     * Tìm project root bằng cách đi ngược lên cho tới khi tìm thấy folder "local/".
     * Fallback: dùng CWD.
     */
    private static Path findProjectRoot() {
        Path cwd = Paths.get(System.getProperty("user.dir"));

        Path current = cwd;
        for (int i = 0; i < 10; i++) {
            if (Files.isDirectory(current.resolve("local"))) {
                return current;
            }
            Path parent = current.getParent();
            if (parent == null) break;
            current = parent;
        }

        return cwd;
    }
}
