package com.vdf.streaming.rule;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.util.UUID;

/**
 * Main đọc 1 file JSON (chứa mảng rules) và ghi vào PostgreSQL bảng rule_definitions.
 * Sử dụng batch insert để tối ưu hiệu năng.
 *
 * Chỉnh trực tiếp các tham số đầu vào bên dưới, không cần file properties.
 */
public class RulePostgresWriter {

    // ====================================================================
    //  CẤU HÌNH THAM SỐ ĐẦU VÀO — CHỈNH TRỰC TIẾP TẠI ĐÂY
    // ====================================================================

    /** Đường dẫn tới file JSON chứa mảng rules (tương đối hoặc tuyệt đối) */
    static final String JSON_FILE = "local/data/rules/100/1790063676.json";

    /** JDBC URL tới PostgreSQL */
    static final String JDBC_URL = "jdbc:postgresql://localhost:5433/realtime_core";

    /** Username kết nối PostgreSQL */
    static final String DB_USER = "postgres";

    /** Password kết nối PostgreSQL */
    static final String DB_PASSWORD = "postgres";

    /**
     * Batch size cho PreparedStatement.
     * 500 rules/batch: mỗi rule JSON ~1-5 KB → batch ≈ 0.5-2.5 MB.
     * Đủ lớn để giảm round-trip, không quá tải memory.
     */
    static final int BATCH_SIZE = 500;

    // ====================================================================

    private static final String INSERT_SQL = """
            INSERT INTO rule_definitions (rule_id, name, rule_json, cooldown_seconds, version, enabled, user_id)
            VALUES (?::uuid, ?, ?::jsonb, ?, ?, ?, ?)
            ON CONFLICT (rule_id, version) DO UPDATE SET
                name = EXCLUDED.name,
                rule_json = EXCLUDED.rule_json,
                cooldown_seconds = EXCLUDED.cooldown_seconds,
                enabled = EXCLUDED.enabled,
                user_id = EXCLUDED.user_id,
                updated_at = CURRENT_TIMESTAMP
            """;

    // ====================================================================
    //  MAIN
    // ====================================================================

    public static void main(String[] args) throws IOException {
        // Resolve đường dẫn JSON file
        Path jsonFile = resolvePath(JSON_FILE);

        if (!Files.exists(jsonFile)) {
            System.err.println("  [ERROR] File not found: " + jsonFile.toAbsolutePath());
            return;
        }

        System.out.println("╔════════════════════════════════════════════════╗");
        System.out.println("║         RULE POSTGRES WRITER                  ║");
        System.out.println("╠════════════════════════════════════════════════╣");
        System.out.printf("║  JSON File   : %-31s║%n", truncate(jsonFile.getFileName().toString(), 31));
        System.out.printf("║  JDBC URL    : %-31s║%n", truncate(JDBC_URL, 31));
        System.out.printf("║  Batch Size  : %-31d║%n", BATCH_SIZE);
        System.out.println("╚════════════════════════════════════════════════╝");

        // 1. Đọc file JSON (mảng rules)
        ObjectMapper mapper = new ObjectMapper();
        String jsonContent = Files.readString(jsonFile);
        JsonNode rootNode = mapper.readTree(jsonContent);

        if (!rootNode.isArray()) {
            System.err.println("  [ERROR] JSON file must contain an array of rules");
            return;
        }

        int totalRules = rootNode.size();
        System.out.printf("\n  Loaded %d rules from file%n", totalRules);

        // 2. Ghi vào PostgreSQL theo batch
        long startTime = System.currentTimeMillis();
        int totalInserted = 0;

        try (Connection conn = DriverManager.getConnection(JDBC_URL, DB_USER, DB_PASSWORD)) {
            conn.setAutoCommit(false);

            try (PreparedStatement ps = conn.prepareStatement(INSERT_SQL)) {
                int batchCount = 0;

                for (int i = 0; i < totalRules; i++) {
                    JsonNode rule = rootNode.get(i);

                    String ruleId = UUID.randomUUID().toString();
                    String name = rule.get("rule_name").asText();
                    String ruleJson = mapper.writeValueAsString(rule);
                    long cooldownSeconds = (long) (Math.random() * 3600);
                    long version = Long.parseLong(rule.get("rule_version").asText());
                    boolean enabled = Math.random() < 0.9;
                    String userId = rule.has("metadata") && rule.get("metadata").has("user_id")
                            ? rule.get("metadata").get("user_id").asText()
                            : "system";

                    ps.setString(1, ruleId);
                    ps.setString(2, name);
                    ps.setString(3, ruleJson);
                    ps.setLong(4, cooldownSeconds);
                    ps.setLong(5, version);
                    ps.setBoolean(6, enabled);
                    ps.setString(7, userId);

                    ps.addBatch();
                    batchCount++;

                    if (batchCount >= BATCH_SIZE) {
                        int[] results = ps.executeBatch();
                        conn.commit();
                        totalInserted += results.length;
                        System.out.printf("  [Batch] Committed %d rules (total: %d/%d)%n",
                                results.length, totalInserted, totalRules);
                        batchCount = 0;
                    }
                }

                // Flush remaining
                if (batchCount > 0) {
                    int[] results = ps.executeBatch();
                    conn.commit();
                    totalInserted += results.length;
                    System.out.printf("  [Batch] Committed %d rules (total: %d/%d)%n",
                            results.length, totalInserted, totalRules);
                }

            } catch (Exception e) {
                conn.rollback();
                throw e;
            }

        } catch (Exception e) {
            System.err.println("\n  [ERROR] Failed to write to PostgreSQL: " + e.getMessage());
            e.printStackTrace();
            return;
        }

        long elapsed = System.currentTimeMillis() - startTime;
        System.out.println("\n  ────────────────────────────────────────");
        System.out.printf("  ✓ Done: %d rules written in %d ms (%.1f rules/sec)%n",
                totalInserted, elapsed, totalInserted * 1000.0 / Math.max(elapsed, 1));
    }

    // ====================================================================
    //  Utility
    // ====================================================================

    /**
     * Resolve đường dẫn file.
     * Nếu tuyệt đối → dùng trực tiếp.
     * Nếu tương đối → tìm project root rồi resolve.
     */
    private static Path resolvePath(String filePath) {
        Path path = Paths.get(filePath);
        if (path.isAbsolute()) {
            return path;
        }

        Path cwd = Paths.get(System.getProperty("user.dir"));
        Path current = cwd;
        for (int i = 0; i < 10; i++) {
            if (Files.isDirectory(current.resolve("local"))) {
                return current.resolve(filePath);
            }
            Path parent = current.getParent();
            if (parent == null) break;
            current = parent;
        }

        return cwd.resolve(filePath);
    }

    private static String truncate(String s, int maxLen) {
        if (s.length() <= maxLen) return s;
        return "..." + s.substring(s.length() - maxLen + 3);
    }
}
