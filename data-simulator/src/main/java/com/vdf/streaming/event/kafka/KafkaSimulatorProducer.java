package com.vdf.streaming.event.kafka;

import com.vdf.streaming.event.model.EventRecord;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Quản lý gửi tin nhắn tới 2 cụm Kafka:
 * 1. kafka-plain (SASL_PLAINTEXT / PLAIN)
 * 2. kafka-gssapi (SASL_PLAINTEXT / GSSAPI Kerberos)
 *
 * Tự động đọc cấu hình từ file .env nếu có, kèm chế độ dry-run/fallback an toàn.
 */
public class KafkaSimulatorProducer implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(KafkaSimulatorProducer.class);

    private final boolean dryRun;
    private final AtomicLong dryRunCounter = new AtomicLong(0);
    private final AtomicLong sendErrorCounter = new AtomicLong(0);

    private KafkaProducer<String, String> plainProducer;
    private KafkaProducer<String, String> gssapiProducer;

    private String plainBootstrap;
    private String gssapiBootstrap;
    private String plainUsername;
    private String plainPassword;
    private String gssapiRealm;
    private String gssapiPrincipal;
    private String gssapiKeytab;
    private String gssapiKrb5Conf;

    public KafkaSimulatorProducer(boolean dryRun, String envFilePath) {
        this.dryRun = dryRun;
        loadEnv(envFilePath);

        if (!dryRun) {
            initPlainProducer();
            initGssapiProducer();
        } else {
            log.info("Chế độ DRY-RUN đang bật: Các sự kiện sẽ chỉ in ra log/file, không gửi lên Kafka.");
        }
    }

    private void loadEnv(String envFilePath) {
        Properties envProps = new Properties();
        Path envPath = (envFilePath != null && !envFilePath.isBlank()) ? Paths.get(envFilePath) : Paths.get(".env");
        if (!Files.exists(envPath)) {
            // Thử tìm ở thư mục cha nếu chạy từ submodule
            Path parentEnv = Paths.get("..", ".env");
            if (Files.exists(parentEnv)) {
                envPath = parentEnv;
            }
        }

        if (Files.exists(envPath)) {
            log.info("Đang đọc cấu hình Kafka từ file: {}", envPath.toAbsolutePath());
            try (FileInputStream fis = new FileInputStream(envPath.toFile())) {
                envProps.load(fis);
            } catch (IOException e) {
                log.warn("Không thể đọc file .env: {}. Sử dụng cấu hình mặc định.", e.getMessage());
            }
        }

        String serverIp = getEnvOrProp(envProps, "SERVER_IP", "localhost");
        if ("0.0.0.0".equals(serverIp)) {
            serverIp = "localhost";
        }

        String plainPort = getEnvOrProp(envProps, "KAFKA_PLAIN_PORT", getEnvOrProp(envProps, "KAFKA_PORT", "9092"));
        String gssapiPort = getEnvOrProp(envProps, "KAFKA_GSSAPI_PORT", "9094");

        this.plainBootstrap = getEnvOrProp(envProps, "KAFKA_PLAIN_BOOTSTRAP_SERVERS", serverIp + ":" + plainPort);
        this.gssapiBootstrap = getEnvOrProp(envProps, "KAFKA_GSSAPI_BOOTSTRAP_SERVERS", serverIp + ":" + gssapiPort);

        // Cấu hình Authentication cho cụm PLAIN (SASL_PLAINTEXT)
        this.plainUsername = getEnvOrProp(envProps, "KAFKA_PLAIN_USERNAME", "admin");
        this.plainPassword = getEnvOrProp(envProps, "KAFKA_PLAIN_PASSWORD", "admin-secret");

        // Cấu hình Authentication cho cụm GSSAPI (Kerberos)
        this.gssapiRealm = getEnvOrProp(envProps, "KRB5_REALM", "EXAMPLE.COM");
        this.gssapiPrincipal = getEnvOrProp(envProps, "KAFKA_GSSAPI_PRINCIPAL", "client@" + this.gssapiRealm);
        this.gssapiKeytab = getEnvOrProp(envProps, "KAFKA_GSSAPI_KEYTAB", "/var/lib/secret/client.keytab");
        this.gssapiKrb5Conf = getEnvOrProp(envProps, "KAFKA_GSSAPI_KRB5_CONF", "/var/lib/secret/krb5.conf");
    }

    private String getEnvOrProp(Properties props, String key, String defaultVal) {
        String val = System.getenv(key);
        if (val != null && !val.isBlank()) return val;
        val = props.getProperty(key);
        if (val != null && !val.isBlank()) return val;
        return defaultVal;
    }

    private void initPlainProducer() {
        try {
            Properties props = new Properties();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, plainBootstrap);
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            props.put(ProducerConfig.ACKS_CONFIG, "1");
            props.put(ProducerConfig.RETRIES_CONFIG, 3);
            props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 5000);
            props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 5000);

            // Cấu hình tối ưu High-Throughput (10.000+ events/s)
            props.put(ProducerConfig.LINGER_MS_CONFIG, 10);
            props.put(ProducerConfig.BATCH_SIZE_CONFIG, 65536); // 64 KB
            props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
            props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 67108864L); // 64 MB
            props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);

            // Cấu hình SASL_PLAINTEXT PLAIN (Username & Password)
            props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
            props.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
            String jaasConfig = String.format(
                    "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";",
                    plainUsername, plainPassword
            );
            props.put(SaslConfigs.SASL_JAAS_CONFIG, jaasConfig);

            this.plainProducer = new KafkaProducer<>(props);
            log.info("✅ Khởi tạo thành công Kafka Producer cho cụm PLAIN tại: {} (User: {})", plainBootstrap, plainUsername);
        } catch (Exception e) {
            log.error("❌ Lỗi khi khởi tạo Kafka Producer PLAIN: {}", e.getMessage(), e);
        }
    }

    private void initGssapiProducer() {
        try {
            // 1. Kiểm tra file krb5.conf
            String krb5Conf = System.getProperty("java.security.krb5.conf");
            if (krb5Conf == null || krb5Conf.isBlank() || !new File(krb5Conf).exists()) {
                if (new File(this.gssapiKrb5Conf).exists()) {
                    krb5Conf = this.gssapiKrb5Conf;
                } else if (new File("docker/krb5/krb5.conf").exists()) {
                    krb5Conf = new File("docker/krb5/krb5.conf").getAbsolutePath();
                } else if (new File("/var/lib/secret/krb5.conf").exists()) {
                    krb5Conf = "/var/lib/secret/krb5.conf";
                }
                if (krb5Conf != null && new File(krb5Conf).exists()) {
                    System.setProperty("java.security.krb5.conf", krb5Conf);
                    log.info("Sử dụng cấu hình Kerberos krb5.conf tại: {}", krb5Conf);
                }
            }

            // 2. Kiểm tra file keytab
            String keytab = this.gssapiKeytab;
            if (!new File(keytab).exists()) {
                if (new File("docker/krb5/client.keytab").exists()) {
                    keytab = new File("docker/krb5/client.keytab").getAbsolutePath();
                } else if (new File("/var/lib/secret/client.keytab").exists()) {
                    keytab = "/var/lib/secret/client.keytab";
                }
            }

            if (!new File(keytab).exists()) {
                log.warn("⚠️ Không tìm thấy file Kerberos keytab tại [{}]. Nếu bạn đang chạy trên máy Host, hãy copy file 'client.keytab' từ server/container về và set KAFKA_GSSAPI_KEYTAB trong .env.", keytab);
                return;
            }

            Properties props = new Properties();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, gssapiBootstrap);
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            props.put(ProducerConfig.ACKS_CONFIG, "1");
            props.put(ProducerConfig.RETRIES_CONFIG, 3);
            props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 5000);
            props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 5000);

            // Cấu hình tối ưu High-Throughput (10.000+ events/s)
            props.put(ProducerConfig.LINGER_MS_CONFIG, 10);
            props.put(ProducerConfig.BATCH_SIZE_CONFIG, 65536); // 64 KB
            props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
            props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 67108864L); // 64 MB
            props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);

            // Cấu hình SASL_PLAINTEXT GSSAPI (Kerberos với Keytab)
            props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
            props.put(SaslConfigs.SASL_MECHANISM, "GSSAPI");
            props.put(SaslConfigs.SASL_KERBEROS_SERVICE_NAME, "kafka");

            String jaasConfig = String.format(
                    "com.sun.security.auth.module.Krb5LoginModule required useKeyTab=true storeKey=true doNotPrompt=true keyTab=\"%s\" principal=\"%s\";",
                    keytab, gssapiPrincipal
            );
            props.put(SaslConfigs.SASL_JAAS_CONFIG, jaasConfig);

            this.gssapiProducer = new KafkaProducer<>(props);
            log.info("✅ Khởi tạo thành công Kafka Producer cho cụm GSSAPI tại: {} (Principal: {}, Keytab: {})",
                    gssapiBootstrap, gssapiPrincipal, keytab);
        } catch (Exception e) {
            Throwable root = e;
            while (root.getCause() != null && root.getCause() != root) {
                root = root.getCause();
            }
            String detailMsg = (root.getMessage() != null && !root.getMessage().isBlank())
                    ? root.getMessage() : root.getClass().getSimpleName();
            log.warn("⚠️ Không thể khởi tạo Kafka Producer GSSAPI (lỗi xác thực Kerberos/Keytab): {}. Chi tiết lỗi gốc: [{}]. Các sự kiện GSSAPI sẽ được bỏ qua việc gửi mạng.",
                    e.getMessage(), detailMsg);
        }
    }

    public void send(EventRecord record) {
        if (dryRun) {
            long count = dryRunCounter.incrementAndGet();
            if (count <= 20) {
                log.info("[DRY-RUN] [{}] Topic: {}, Key: {}, Payload: {}",
                        record.getClusterType(), record.getTopic(), record.getKey(), record.getPayloadJson());
            } else if (count == 21) {
                log.info("[DRY-RUN] Đã in 20 sự kiện mẫu đầu tiên. Đang tiếp tục giả lập dry-run tốc độ cao...");
            }
            return;
        }

        KafkaProducer<String, String> producer = (record.getClusterType() == KafkaClusterType.PLAIN)
                ? plainProducer : gssapiProducer;

        if (producer == null) {
            log.debug("Bỏ qua gửi lên Kafka vì Producer cho [{}] chưa được khởi tạo: Topic={}",
                    record.getClusterType(), record.getTopic());
            return;
        }

        ProducerRecord<String, String> kafkaRecord = new ProducerRecord<>(
                record.getTopic(), record.getKey(), record.getPayloadJson());

        producer.send(kafkaRecord, (metadata, exception) -> {
            if (exception != null) {
                long err = sendErrorCounter.incrementAndGet();
                if (err <= 10 || err % 1000 == 0) {
                    log.error("❌ Lỗi khi gửi tin nhắn tới Topic [{}]: {} (Tổng số lỗi: {})",
                            record.getTopic(), exception.getMessage(), err);
                }
            } else {
                log.debug("-> Gửi thành công tới Topic [{}] partition {} offset {}",
                        record.getTopic(), metadata.partition(), metadata.offset());
            }
        });
    }

    public void flush() {
        if (plainProducer != null) plainProducer.flush();
        if (gssapiProducer != null) gssapiProducer.flush();
    }

    @Override
    public void close() {
        flush();
        if (plainProducer != null) {
            try { plainProducer.close(); } catch (Exception ignored) {}
        }
        if (gssapiProducer != null) {
            try { gssapiProducer.close(); } catch (Exception ignored) {}
        }
    }
}

