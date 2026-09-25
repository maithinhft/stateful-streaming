package com.vdf.streaming.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Singleton config loader.
 * <p>
 * Load order:
 * <ol>
 *   <li>Tìm và nạp file {@code .env} (thư mục làm việc hoặc project root) vào System properties
 *       – chỉ những key chưa có trong môi trường thật mới được ghi đè, giúp biến môi trường
 *       thật luôn có độ ưu tiên cao nhất.</li>
 *   <li>Đọc {@code config.{APP_ENV}.yml} từ classpath (mặc định {@code APP_ENV=dev}).</li>
 *   <li>Giải nén {@code ${VAR}} và {@code ${VAR:-default}} từ tổng hợp env + system properties.</li>
 *   <li>Cache kết quả – thread-safe bằng {@code synchronized}.</li>
 * </ol>
 */
public class ConfigLoader {

    private static final Logger LOG = LoggerFactory.getLogger(ConfigLoader.class);

    // Pattern bắt ${VAR} hoặc ${VAR:-default}
    private static final Pattern PLACEHOLDER = Pattern.compile("\\$\\{(\\w+)(?::-(.*?))?\\}");

    private static Map<String, Object> configCache;

    // ----------------------------------------------------------------
    // Public API
    // ----------------------------------------------------------------

    /**
     * Nạp (hoặc trả về cache) của config map đã được parse.
     */
    public static synchronized Map<String, Object> load() {
        if (configCache != null) {
            LOG.debug("Returning cached config");
            return configCache;
        }

        // 1. Nạp .env vào system properties (ưu tiên thấp hơn biến môi trường thật)
        loadDotEnv();

        // 2. Chọn file YAML theo APP_ENV
        String env = resolveVar("APP_ENV", "dev");
        String fileName = "config." + env + ".yml";
        LOG.info("Loading config file: {} (APP_ENV={})", fileName, env);

        try {
            String rawContent;
            try (InputStream is = ConfigLoader.class.getClassLoader().getResourceAsStream(fileName)) {
                if (is == null) {
                    throw new RuntimeException("Config file not found on classpath: " + fileName);
                }
                rawContent = new String(is.readAllBytes());
            }

            // 3. Resolve ${VAR:-default} placeholders
            String resolved = resolvePlaceholders(rawContent);

            // 4. Parse YAML
            Yaml yaml = new Yaml();
            configCache = yaml.load(resolved);

            LOG.info("Config loaded from '{}' with {} top-level keys", fileName, configCache.size());
            return configCache;

        } catch (Exception e) {
            LOG.error("Failed to load config file: {}", fileName, e);
            throw new RuntimeException("Load config failed", e);
        }
    }

    /**
     * Helper: đọc một nested value từ config map theo đường dẫn chấm.
     * Ví dụ: {@code getString("kafka.plain.bootstrap_servers")}
     */
    @SuppressWarnings("unchecked")
    public static String getString(String dotPath) {
        return getString(dotPath, null);
    }

    @SuppressWarnings("unchecked")
    public static String getString(String dotPath, String defaultValue) {
        Map<String, Object> cfg = load();
        String[] parts = dotPath.split("\\.");
        Object current = cfg;
        for (String part : parts) {
            if (!(current instanceof Map)) return defaultValue;
            current = ((Map<String, Object>) current).get(part);
        }
        return current != null ? String.valueOf(current) : defaultValue;
    }

    public static int getInt(String dotPath, int defaultValue) {
        String val = getString(dotPath);
        if (val == null) return defaultValue;
        try { return Integer.parseInt(val); } catch (NumberFormatException e) { return defaultValue; }
    }

    public static long getLong(String dotPath, long defaultValue) {
        String val = getString(dotPath);
        if (val == null) return defaultValue;
        try { return Long.parseLong(val); } catch (NumberFormatException e) { return defaultValue; }
    }

    // ----------------------------------------------------------------
    // .env loader
    // ----------------------------------------------------------------

    /**
     * Tìm và nạp file {@code .env} theo thứ tự ưu tiên:
     * <ol>
     *   <li>Biến môi trường / system property {@code DOTENV_PATH} (đường dẫn tuyệt đối).</li>
     *   <li>Thư mục làm việc hiện tại: {@code ./.env}</li>
     *   <li>Thư mục cha (project root): {@code ../.env}</li>
     * </ol>
     * Chỉ các key chưa xuất hiện trong môi trường thật mới được inject vào
     * {@link System#setProperty(String, String)}.
     */
    private static void loadDotEnv() {
        String customPath = System.getenv("DOTENV_PATH");
        if (customPath == null) customPath = System.getProperty("DOTENV_PATH");

        File dotEnvFile = null;
        if (customPath != null) {
            dotEnvFile = new File(customPath);
        } else {
            // CWD khi chạy Maven là thư mục module (flink-jobs/)
            // .env nằm ở project root (một cấp trên) → "../.env"
            for (String candidate : new String[]{ ".env", "../.env" }) {
                File f = new File(candidate);
                if (f.isFile()) { dotEnvFile = f; break; }
            }
        }

        if (dotEnvFile == null || !dotEnvFile.isFile()) {
            LOG.warn(".env file not found in CWD='{}' or parent – using OS environment variables only",
                    new File(".").getAbsolutePath());
            return;
        }

        LOG.info(".env found at: {}", dotEnvFile.getAbsolutePath());
        Map<String, String> dotEnvVars = parseDotEnv(dotEnvFile);
        int injected = 0;
        for (Map.Entry<String, String> entry : dotEnvVars.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            // Biến môi trường thật luôn thắng
            if (System.getenv(key) == null && System.getProperty(key) == null) {
                System.setProperty(key, value);
                injected++;
            }
        }
        LOG.info(".env loaded: {} total entries, {} injected into system properties", dotEnvVars.size(), injected);
    }

    /**
     * Parse file .env theo định dạng KEY=VALUE (bỏ comment #, bỏ dòng trống,
     * strip dấu nháy đơn và đôi xung quanh value).
     */
    private static Map<String, String> parseDotEnv(File file) {
        Map<String, String> result = new HashMap<>();
        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
            String line;
            while ((line = br.readLine()) != null) {
                line = line.trim();
                // Bỏ qua dòng trống và comment
                if (line.isEmpty() || line.startsWith("#")) continue;
                int eqIdx = line.indexOf('=');
                if (eqIdx <= 0) continue;

                String key = line.substring(0, eqIdx).trim();
                String value = line.substring(eqIdx + 1).trim();

                // Strip dấu nháy bao quanh value (nếu có)
                if ((value.startsWith("\"") && value.endsWith("\""))
                        || (value.startsWith("'") && value.endsWith("'"))) {
                    value = value.substring(1, value.length() - 1);
                }

                // Bỏ comment inline (phần sau dấu # nằm ngoài nháy)
                int inlineComment = value.indexOf(" #");
                if (inlineComment != -1) {
                    value = value.substring(0, inlineComment).trim();
                }

                if (!key.isEmpty()) {
                    result.put(key, value);
                }
            }
        } catch (Exception e) {
            LOG.warn("Failed to parse .env file: {}", file.getAbsolutePath(), e);
        }
        return result;
    }

    // ----------------------------------------------------------------
    // Placeholder resolver
    // ----------------------------------------------------------------

    /**
     * Thay thế {@code ${VAR}} và {@code ${VAR:-default}} trong {@code content}
     * bằng giá trị từ môi trường + system properties.
     */
    private static String resolvePlaceholders(String content) {
        Matcher matcher = PLACEHOLDER.matcher(content);
        StringBuilder sb = new StringBuilder();
        while (matcher.find()) {
            String varName = matcher.group(1);
            String fallback = matcher.group(2); // null nếu không có :-default
            String val = resolveVar(varName, fallback);
            if (val == null) {
                LOG.warn("Env var '{}' not set and no default provided – keeping placeholder", varName);
                val = matcher.group(0);
            }
            matcher.appendReplacement(sb, Matcher.quoteReplacement(val));
        }
        matcher.appendTail(sb);
        return sb.toString();
    }

    /**
     * Tìm giá trị của một biến: ưu tiên env thật → system property → defaultValue.
     */
    private static String resolveVar(String name, String defaultValue) {
        String val = System.getenv(name);
        if (val == null) val = System.getProperty(name);
        return val != null ? val : defaultValue;
    }

    // ----------------------------------------------------------------
    // Self-test
    // ----------------------------------------------------------------

    /**
     * Chạy để kiểm tra ConfigLoader hoạt động đúng không.
     * <pre>
     *   mvn exec:java -Dexec.mainClass="com.vdf.streaming.config.ConfigLoader"
     *                 -Dexec.includeProvidedDependencies=true
     * </pre>
     */
    @SuppressWarnings("unchecked")
    public static void main(String[] args) {
        System.out.println("=== ConfigLoader Self-Test ===");
        System.out.println("CWD     : " + new java.io.File(".").getAbsolutePath());
        System.out.println("APP_ENV : " + System.getenv().getOrDefault("APP_ENV",
                System.getProperty("APP_ENV", "(not set -> default: dev)")));
        System.out.println();

        // Kích hoạt load (sẽ log ra console qua Logback)
        Map<String, Object> cfg = load();

        System.out.println();
        System.out.println("=== Config Map (flat) ===");
        printFlat("", cfg);
    }

    @SuppressWarnings("unchecked")
    private static void printFlat(String prefix, Object node) {
        if (node instanceof Map) {
            ((Map<String, Object>) node).forEach((k, v) ->
                    printFlat(prefix.isEmpty() ? k : prefix + "." + k, v));
        } else {
            System.out.printf("  %-55s = %s%n", prefix, node);
        }
    }
}