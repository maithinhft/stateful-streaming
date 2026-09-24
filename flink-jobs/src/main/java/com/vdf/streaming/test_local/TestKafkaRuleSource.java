package com.vdf.streaming.test_local;

import com.vdf.streaming.compiler.RuleCompiler;
import com.vdf.streaming.models.CompiledRuleEnvelope;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class TestKafkaRuleSource {
    public static void main(String[] args) {
        System.out.println("=== Khởi động Kafka Consumer (Native Java) để parse Rule ===");

        // Thiết lập trực tiếp cấu hình SASL_PLAINTEXT (tránh dùng class của Flink để không bị lỗi NoClassDefFoundError)
        Properties props = new Properties();
        
        // Bạn có thể đổi localhost:9092 thành localhost:29092 tùy theo port mà Docker map ra ngoài
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"); 
        
        // Cấu hình SASL_PLAINTEXT y hệt như KafkaClusterConfig (nhưng bằng Java thuần)
        props.put("security.protocol", "SASL_PLAINTEXT");
        props.put("sasl.mechanism", "PLAIN");
        props.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"admin\" password=\"admin-secret\";");

        props.put(ConsumerConfig.GROUP_ID_CONFIG, "rule-compiler-test-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        System.out.println("Sử dụng Bootstrap Servers: " + props.getProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
        System.out.println("Security Protocol: " + props.getProperty("security.protocol"));

        RuleCompiler compiler = new RuleCompiler();
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        
        String topic = "rule_definitions";
        consumer.subscribe(Collections.singletonList(topic));

        System.out.println("Đang lắng nghe topic: '" + topic + "'...");

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println("\n---------------------------------------------------");
                    System.out.println("[NHẬN RULE MỚI] Từ Kafka: " + record.value());
                    
                    try {
                        RuleCompiler.CdcRuleEvent cdcEvent = compiler.parseCdcEvent(record.value());
                        
                        if (compiler.isDelete(cdcEvent)) {
                            System.out.println(" => Sự kiện XÓA hoặc VÔ HIỆU HÓA Rule: " + cdcEvent.ruleId());
                            continue;
                        }

                        CompiledRuleEnvelope compiledRule = compiler.compile(cdcEvent, -1);
                        System.out.println(" => [THÀNH CÔNG] Parse & Compile rule hợp lệ!");
                        System.out.println("     - Rule ID:   " + compiledRule.getRuleId());
                        System.out.println("     - Rule Name: " + compiledRule.getRuleName());
                        System.out.println("     - Rule Type: " + compiledRule.getRuleType());
                        System.out.println("     - Triggers:  " + compiledRule.getTriggers());
                    } catch (Exception e) {
                        System.err.println(" => [LỖI PARSE/COMPILE] Không thể biên dịch rule này!");
                        e.printStackTrace();
                    }
                }
            }
        } finally {
            consumer.close();
        }
    }
}
