DO $$
BEGIN
  IF NOT EXISTS (
    SELECT FROM pg_roles
    WHERE rolname = 'replicator'
  ) THEN
    CREATE ROLE replicator
      WITH LOGIN
      REPLICATION
      PASSWORD 'replicatorpassword';
  END IF;
END
$$;

GRANT CONNECT ON DATABASE realtime_core TO replicator;

\c realtime_core

CREATE TABLE IF NOT EXISTS rule_definitions (
    id BIGSERIAL PRIMARY KEY,

    rule_id UUID NOT NULL,

    name VARCHAR(255) NOT NULL,

    rule_json JSONB NOT NULL,

    cooldown_seconds BIGINT NOT NULL DEFAULT 0,

    version BIGINT NOT NULL,

    enabled BOOLEAN NOT NULL DEFAULT TRUE,

    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    user_id VARCHAR(255) NOT NULL DEFAULT 'system',

    CONSTRAINT uq_rule_version
        UNIQUE (rule_id, version)
);

CREATE TABLE IF NOT EXISTS schema_definitions (
    schema_id VARCHAR(255) PRIMARY KEY,
    schema_payload JSONB NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);


ALTER TABLE rule_definitions
REPLICA IDENTITY FULL;

ALTER TABLE schema_definitions
REPLICA IDENTITY FULL;

-- ============================================================
-- Bảng cha: thông tin 1 Kafka cluster (bootstrap servers + bảo mật) cho Stream
-- ============================================================
CREATE TABLE IF NOT EXISTS kafka_stream_cluster_config (
    id SERIAL PRIMARY KEY,
    stream_id VARCHAR(255) NOT NULL,          -- định danh logic, khớp KafkaStream.streamId
    cluster_name VARCHAR(255) NOT NULL,       -- tên gợi nhớ, VD "kafka-plain", "kafka-gssapi"
    bootstrap_servers VARCHAR(500) NOT NULL,  -- "broker1:9092,broker2:9092"

    -- ---- Cấu hình bảo mật riêng theo cluster ----
    security_protocol VARCHAR(50) DEFAULT 'PLAINTEXT',  -- PLAINTEXT | SASL_PLAINTEXT | SASL_SSL
    sasl_mechanism VARCHAR(50),               -- GSSAPI (Kerberos) | PLAIN | SCRAM-SHA-256 ...
    sasl_kerberos_service_name VARCHAR(100),  -- thường là "kafka"
    sasl_jaas_config TEXT,                    -- toàn bộ chuỗi JAAS

    enabled BOOLEAN DEFAULT TRUE,
    updated_at TIMESTAMP DEFAULT NOW(),
    UNIQUE (stream_id, cluster_name)
);

-- ============================================================
-- Bảng con: danh sách topic thuộc về 1 cluster (1-nhiều) cho Stream
-- ============================================================
CREATE TABLE IF NOT EXISTS kafka_stream_topic_config (
    id SERIAL PRIMARY KEY,
    cluster_config_id INTEGER NOT NULL REFERENCES kafka_stream_cluster_config(id) ON DELETE CASCADE,
    topic_name VARCHAR(255) NOT NULL,
    enabled BOOLEAN DEFAULT TRUE,
    updated_at TIMESTAMP DEFAULT NOW(),

    UNIQUE (cluster_config_id, topic_name)   -- tránh khai báo trùng topic trong cùng 1 cluster
);

-- ============================================================
-- Bảng cha & con cho Batch
-- ============================================================
CREATE TABLE IF NOT EXISTS kafka_batch_cluster_config (
    id SERIAL PRIMARY KEY,
    stream_id VARCHAR(255) NOT NULL,
    cluster_name VARCHAR(255) NOT NULL,
    bootstrap_servers VARCHAR(500) NOT NULL,
    security_protocol VARCHAR(50) DEFAULT 'PLAINTEXT',
    sasl_mechanism VARCHAR(50),
    sasl_kerberos_service_name VARCHAR(100),
    sasl_jaas_config TEXT,
    enabled BOOLEAN DEFAULT TRUE,
    updated_at TIMESTAMP DEFAULT NOW(),
    UNIQUE (stream_id, cluster_name)
);

CREATE TABLE IF NOT EXISTS kafka_batch_topic_config (
    id SERIAL PRIMARY KEY,
    cluster_config_id INTEGER NOT NULL REFERENCES kafka_batch_cluster_config(id) ON DELETE CASCADE,
    topic_name VARCHAR(255) NOT NULL,
    enabled BOOLEAN DEFAULT TRUE,
    updated_at TIMESTAMP DEFAULT NOW(),
    UNIQUE (cluster_config_id, topic_name)
);

-- Dữ liệu mẫu ban đầu cho kafka_stream_cluster_config (CHỈ cấu hình cho event streams)
INSERT INTO kafka_stream_cluster_config (stream_id, cluster_name, bootstrap_servers, security_protocol, sasl_mechanism, sasl_kerberos_service_name, sasl_jaas_config, enabled)
VALUES 
(
    'stream-events',
    'kafka-gssapi',
    'kafka-gssapi:29094',
    'SASL_PLAINTEXT',
    'GSSAPI',
    'kafka',
    'com.sun.security.auth.module.Krb5LoginModule required useKeyTab=true storeKey=true doNotPrompt=true keyTab="/var/lib/secret/client.keytab" principal="client@EXAMPLE.COM";',
    TRUE
),
(
    'stream-events',
    'kafka-plain',
    'kafka-plain:29092',
    'SASL_PLAINTEXT',
    'PLAIN',
    NULL,
    'org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="admin-secret";',
    TRUE
)
ON CONFLICT (stream_id, cluster_name) DO NOTHING;


-- ============================================================
-- A. BATCH EVENT SCHEMAS (7 NGUỒN BATCH: B1, B2, B4, B5)
-- Tuân thủ đặc tả Batch Event Envelope (07_BATCH_EVENT_SCHEMA.md)
-- ============================================================

INSERT INTO schema_definitions (schema_id, schema_payload) VALUES
('batch_trial_0d_registered_v1', $json$
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "schema_id": "batch_trial_0d_registered_v1",
  "schema_type": "BATCH",
  "dataset_name": "trial_0d_registered",
  "topic": "batch_trial_0d_registered",
  "version": "v1",
  "description": "B1: Tập KH đã đăng ký trial 0đ (Điều_Kiện_2)",
  "ttl_seconds": 2592000,
  "key_definition": {
    "field": "msisdn",
    "key_type": "PHONE_E164",
    "allow_null": false,
    "auto_normalize": true,
    "default_country_code": "84",
    "regex_pattern": "^\\+[1-9][0-9]{6,14}$"
  },
  "allowed_sync_modes": [
    "FULL_SNAPSHOT",
    "UPSERT",
    "DELETE"
  ],
  "fields": {
    "sub_code": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "allowed_values": [
        "VTM1",
        "VTM2",
        "VTM4",
        "VTM5",
        "VTM6"
      ],
      "description": "Mã gói sub trial 0đ từ L1_SUB_MNGT_SUB"
    }
  }
}
$json$::jsonb)
ON CONFLICT (schema_id) DO NOTHING;

INSERT INTO schema_definitions (schema_id, schema_payload) VALUES
('batch_renewed_subscribers_v1', $json$
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "schema_id": "batch_renewed_subscribers_v1",
  "schema_type": "BATCH",
  "dataset_name": "renewed_subscribers",
  "topic": "batch_renewed_subscribers",
  "version": "v1",
  "description": "B1: Tập KH đã từng gia hạn (Điều_Kiện_3 - loại trừ, chỉ giữ gia hạn lần đầu)",
  "ttl_seconds": 2592000,
  "key_definition": {
    "field": "msisdn",
    "key_type": "PHONE_E164",
    "allow_null": false,
    "auto_normalize": true,
    "default_country_code": "84",
    "regex_pattern": "^\\+[1-9][0-9]{6,14}$"
  },
  "allowed_sync_modes": [
    "FULL_SNAPSHOT",
    "UPSERT",
    "DELETE"
  ],
  "fields": {
    "sub_code": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "allowed_values": [
        "VTM1",
        "VTM2",
        "VTM4",
        "VTM5",
        "VTM6"
      ],
      "description": "Mã gói sub đã từng gia hạn"
    }
  }
}
$json$::jsonb)
ON CONFLICT (schema_id) DO NOTHING;

INSERT INTO schema_definitions (schema_id, schema_payload) VALUES
('batch_active_promo_packages_v1', $json$
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "schema_id": "batch_active_promo_packages_v1",
  "schema_type": "BATCH",
  "dataset_name": "active_promo_packages",
  "topic": "batch_active_promo_packages",
  "version": "v1",
  "description": "B2: Thuê bao đang có gói ưu đãi hoạt động tới ngày n-1",
  "ttl_seconds": 2592000,
  "key_definition": {
    "field": "msisdn",
    "key_type": "PHONE_E164",
    "allow_null": false,
    "auto_normalize": true,
    "default_country_code": "84",
    "regex_pattern": "^\\+[1-9][0-9]{6,14}$"
  },
  "allowed_sync_modes": [
    "FULL_SNAPSHOT",
    "UPSERT",
    "DELETE"
  ],
  "fields": {
    "sub_code": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "allowed_values": [
        "VTM1",
        "VTM2",
        "VTM3",
        "VTM4"
      ],
      "description": "Mã gói ưu đãi VTM hoạt động"
    }
  }
}
$json$::jsonb)
ON CONFLICT (schema_id) DO NOTHING;

INSERT INTO schema_definitions (schema_id, schema_payload) VALUES
('batch_blacklist_qtrr_v1', $json$
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "schema_id": "batch_blacklist_qtrr_v1",
  "schema_type": "BATCH",
  "dataset_name": "blacklist_qtrr",
  "topic": "batch_blacklist_qtrr",
  "version": "v1",
  "description": "B4: Danh sách đen Quản trị Rủi ro (Blacklist QTRR) bản ghi mới nhất",
  "ttl_seconds": 2592000,
  "key_definition": {
    "field": "msisdn",
    "key_type": "PHONE_E164",
    "allow_null": false,
    "auto_normalize": true,
    "default_country_code": "84",
    "regex_pattern": "^\\+[1-9][0-9]{6,14}$"
  },
  "allowed_sync_modes": [
    "FULL_SNAPSHOT",
    "UPSERT",
    "DELETE"
  ],
  "fields": {}
}
$json$::jsonb)
ON CONFLICT (schema_id) DO NOTHING;

INSERT INTO schema_definitions (schema_id, schema_payload) VALUES
('batch_simfarm_3_tram_v1', $json$
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "schema_id": "batch_simfarm_3_tram_v1",
  "schema_type": "BATCH",
  "dataset_name": "simfarm_3_tram",
  "topic": "batch_simfarm_3_tram",
  "version": "v1",
  "description": "B4: Danh sách Simfarm 3 trạm BTS tháng gần nhất",
  "ttl_seconds": 2592000,
  "key_definition": {
    "field": "msisdn",
    "key_type": "PHONE_E164",
    "allow_null": false,
    "auto_normalize": true,
    "default_country_code": "84",
    "regex_pattern": "^\\+[1-9][0-9]{6,14}$"
  },
  "allowed_sync_modes": [
    "FULL_SNAPSHOT",
    "UPSERT",
    "DELETE"
  ],
  "fields": {}
}
$json$::jsonb)
ON CONFLICT (schema_id) DO NOTHING;

INSERT INTO schema_definitions (schema_id, schema_payload) VALUES
('batch_cep_pushed_msisdn_v1', $json$
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "schema_id": "batch_cep_pushed_msisdn_v1",
  "schema_type": "BATCH",
  "dataset_name": "cep_pushed_msisdn",
  "topic": "batch_cep_pushed_msisdn",
  "version": "v1",
  "description": "B4: Tập msisdn đã đẩy sang CEP trước đó (29923_CEP chống trùng)",
  "ttl_seconds": 2592000,
  "key_definition": {
    "field": "msisdn",
    "key_type": "PHONE_E164",
    "allow_null": false,
    "auto_normalize": true,
    "default_country_code": "84",
    "regex_pattern": "^\\+[1-9][0-9]{6,14}$"
  },
  "allowed_sync_modes": [
    "FULL_SNAPSHOT",
    "UPSERT",
    "DELETE"
  ],
  "fields": {}
}
$json$::jsonb)
ON CONFLICT (schema_id) DO NOTHING;

INSERT INTO schema_definitions (schema_id, schema_payload) VALUES
('batch_vip_customer_list_v1', $json$
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "schema_id": "batch_vip_customer_list_v1",
  "schema_type": "BATCH",
  "dataset_name": "vip_customer_list",
  "topic": "batch_vip_customer_list",
  "version": "v1",
  "description": "B5: Danh sách KH vị thế (VIP - tap_kh_vi_the_v2) dùng để lọc giao dịch thất bại",
  "ttl_seconds": 2592000,
  "key_definition": {
    "field": "msisdn",
    "key_type": "PHONE_E164",
    "allow_null": false,
    "auto_normalize": true,
    "default_country_code": "84",
    "regex_pattern": "^\\+[1-9][0-9]{6,14}$"
  },
  "allowed_sync_modes": [
    "FULL_SNAPSHOT",
    "UPSERT",
    "DELETE"
  ],
  "fields": {}
}
$json$::jsonb)
ON CONFLICT (schema_id) DO NOTHING;

-- ============================================================
-- B. REALTIME STREAM SCHEMAS (10 NGUỒN KAFKA STREAM CHÍNH THEO TOPIC)
-- Định nghĩa cấu trúc trường, kiểu dữ liệu, ràng buộc validate
-- ============================================================

INSERT INTO schema_definitions (schema_id, schema_payload) VALUES
('stream_V1-INSERT-TRANS-DAILY-HIS_v1', $json$
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "schema_id": "stream_V1-INSERT-TRANS-DAILY-HIS_v1",
  "schema_type": "STREAM",
  "source": "TDH",
  "topic": "V1-INSERT-TRANS-DAILY-HIS",
  "version": "v1",
  "description": "Lịch sử giao dịch tài chính lõi core payment (Insert)",
  "key_definition": {
    "field": "msisdn",
    "key_type": "PHONE_E164",
    "auto_normalize": true,
    "default_country_code": "84"
  },
  "fields": {
    "processCode": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Mã tiến trình xử lý giao dịch"
    },
    "requestContent": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "JSON nội dung yêu cầu thanh toán"
    },
    "requestDate": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Thời gian gửi yêu cầu (yyyy-MM-dd HH:mm:ss)"
    },
    "requestId": {
      "type": "LONG",
      "required": true,
      "nullable": false,
      "description": "Mã định danh yêu cầu giao dịch"
    },
    "requestMti": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Message Type Identifier yêu cầu (0200)"
    },
    "responseDate": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Thời gian phản hồi giao dịch"
    },
    "transDailyHisFinanceId": {
      "type": "LONG",
      "required": true,
      "nullable": false,
      "description": "ID khóa chính giao dịch tài chính"
    },
    "msisdn": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Số điện thoại thuê bao thực hiện"
    },
    "custId": {
      "type": "INT",
      "required": true,
      "nullable": false,
      "description": "Mã khách hàng trong hệ thống"
    },
    "custName": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Tên khách hàng"
    },
    "custMobileNo": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Số di động liên hệ khách hàng"
    },
    "idNo": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Số giấy tờ tùy thân (CCCD/CMND)"
    },
    "idType": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Loại giấy tờ tùy thân (CCCD)"
    },
    "idTypeName": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Tên loại giấy tờ tùy thân"
    },
    "gender": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Giới tính (M/F)"
    },
    "birthday": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Ngày sinh (yyyy-MM-dd)"
    },
    "custAddress": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Địa chỉ khách hàng"
    },
    "mobileId": {
      "type": "INT",
      "required": true,
      "nullable": false,
      "description": "ID định danh thiết bị/thuê bao di động"
    },
    "transAmount": {
      "type": "LONG",
      "required": true,
      "nullable": false,
      "min": 0,
      "description": "Số tiền giao dịch gốc (VNĐ)"
    },
    "transFee": {
      "type": "LONG",
      "required": true,
      "nullable": false,
      "min": 0,
      "description": "Phí giao dịch (VNĐ)"
    },
    "discount": {
      "type": "LONG",
      "required": true,
      "nullable": false,
      "min": 0,
      "description": "Số tiền giảm giá / khuyến mại (VNĐ)"
    },
    "finalAmount": {
      "type": "LONG",
      "required": true,
      "nullable": false,
      "min": 0,
      "description": "Số tiền thanh toán cuối cùng (VNĐ)"
    },
    "errorCode": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Mã lỗi giao dịch (00 = Thành công)"
    },
    "errorCodeName": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Mô tả tên mã lỗi"
    },
    "correctCode": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Mã hiệu chỉnh đối soát (00 = Bình thường, 05 = Chờ hiệu chỉnh)"
    },
    "correctCodeName": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Tên mã hiệu chỉnh đối soát"
    },
    "serviceCode": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Mã dịch vụ thanh toán (EVN, NUOC, FLTSEDU...)"
    },
    "transType": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Loại giao dịch (PAYMENT, TRANSFER)"
    },
    "transContent": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Nội dung giao dịch"
    },
    "viettelBankCode": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Mã ngân hàng / ví đối tác (VTM)"
    },
    "appId": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Mã ứng dụng nguồn (VTM)"
    },
    "shopCode": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Mã điểm bán / kênh giao dịch"
    },
    "shopName": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Tên điểm bán / kênh giao dịch"
    },
    "staffCode": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Mã nhân viên / hệ thống thực hiện"
    },
    "staffName": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Tên nhân viên / hệ thống"
    },
    "accNo": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Số tài khoản ví Viettel Money"
    },
    "accName": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Tên chủ tài khoản ví"
    },
    "accType": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Loại tài khoản ví"
    },
    "accTypeName": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Tên hiển thị loại tài khoản"
    },
    "telcoCode": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Mã nhà mạng viễn thông (VTT)"
    },
    "nationCode": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Mã quốc gia (VNM)"
    },
    "nationName": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Tên quốc gia"
    },
    "languageCode": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Mã ngôn ngữ (vi)"
    },
    "languageName": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Tên ngôn ngữ"
    },
    "responseMti": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Message Type Identifier phản hồi (0210)"
    },
    "partnerRequestId": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Mã yêu cầu phía đối tác"
    },
    "billCode": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Mã hóa đơn dịch vụ"
    },
    "benAccNo": {
      "type": "STRING",
      "required": false,
      "nullable": true,
      "description": "Số tài khoản người thụ hưởng (khi chuyển tiền)"
    },
    "benBankCode": {
      "type": "STRING",
      "required": false,
      "nullable": true,
      "description": "Mã ngân hàng người thụ hưởng"
    },
    "benCustName": {
      "type": "STRING",
      "required": false,
      "nullable": true,
      "description": "Tên người thụ hưởng"
    }
  }
}
$json$::jsonb)
ON CONFLICT (schema_id) DO NOTHING;

INSERT INTO schema_definitions (schema_id, schema_payload) VALUES
('stream_V1-UPDATE-TRANS-DAILY-HIS_v1', $json$
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "schema_id": "stream_V1-UPDATE-TRANS-DAILY-HIS_v1",
  "schema_type": "STREAM",
  "source": "TDH",
  "topic": "V1-UPDATE-TRANS-DAILY-HIS",
  "version": "v1",
  "description": "Điều chỉnh giao dịch đối soát (Update trans_daily_his)",
  "key_definition": {
    "field": "msisdn",
    "key_type": "PHONE_E164",
    "auto_normalize": true,
    "default_country_code": "84"
  },
  "fields": {
    "transDailyHisFinanceId": {
      "type": "LONG",
      "required": true,
      "nullable": false,
      "description": "ID tham chiếu giao dịch gốc cần điều chỉnh"
    },
    "requestId": {
      "type": "LONG",
      "required": true,
      "nullable": false,
      "description": "Mã yêu cầu điều chỉnh đối soát"
    },
    "requestMti": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Message Type Identifier (0200)"
    },
    "processCode": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Mã tiến trình xử lý đối soát (000001)"
    },
    "processName": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Tên tiến trình đối soát điều chỉnh"
    },
    "serviceCode": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Mã dịch vụ"
    },
    "msisdn": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Số điện thoại khách hàng"
    },
    "mobileId": {
      "type": "INT",
      "required": true,
      "nullable": false,
      "description": "ID thuê bao"
    },
    "accNo": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Số tài khoản ví"
    },
    "accTypeName": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Tên loại tài khoản"
    },
    "appId": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Mã ứng dụng (VTM)"
    },
    "auditNo": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Số kiểm toán đối soát"
    },
    "cardNo": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Số thẻ liên kết"
    },
    "correctCode": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Mã trạng thái điều chỉnh (00 = Thành công)"
    },
    "correctCodeName": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Mô tả kết quả điều chỉnh"
    },
    "errorCode": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Mã lỗi (00)"
    },
    "errorCodeName": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Tên mã lỗi"
    },
    "insertDate": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Thời gian tạo bản ghi điều chỉnh"
    },
    "requestDate": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Thời gian yêu cầu đối soát"
    },
    "responseDate": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Thời gian hoàn tất phản hồi"
    },
    "shopCode": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Mã điểm bán"
    },
    "staffCode": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Mã hệ thống thực hiện điều chỉnh"
    },
    "transContent": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Nội dung diễn giải điều chỉnh"
    },
    "viettelBankCode": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Mã ngân hàng (VTM)"
    },
    "custId": {
      "type": "INT",
      "required": false,
      "nullable": true
    },
    "custName": {
      "type": "STRING",
      "required": false,
      "nullable": true
    },
    "birthday": {
      "type": "STRING",
      "required": false,
      "nullable": true
    },
    "gender": {
      "type": "STRING",
      "required": false,
      "nullable": true
    },
    "idType": {
      "type": "STRING",
      "required": false,
      "nullable": true
    },
    "idTypeName": {
      "type": "STRING",
      "required": false,
      "nullable": true
    },
    "accName": {
      "type": "STRING",
      "required": false,
      "nullable": true
    },
    "accType": {
      "type": "STRING",
      "required": false,
      "nullable": true
    },
    "transAmount": {
      "type": "LONG",
      "required": false,
      "nullable": true
    },
    "transFee": {
      "type": "LONG",
      "required": false,
      "nullable": true
    },
    "discount": {
      "type": "LONG",
      "required": false,
      "nullable": true
    },
    "finalAmount": {
      "type": "LONG",
      "required": false,
      "nullable": true
    },
    "transType": {
      "type": "STRING",
      "required": false,
      "nullable": true
    },
    "telcoCode": {
      "type": "STRING",
      "required": false,
      "nullable": true
    },
    "nationCode": {
      "type": "STRING",
      "required": false,
      "nullable": true
    },
    "nationName": {
      "type": "STRING",
      "required": false,
      "nullable": true
    },
    "languageCode": {
      "type": "STRING",
      "required": false,
      "nullable": true
    },
    "languageName": {
      "type": "STRING",
      "required": false,
      "nullable": true
    },
    "responseMti": {
      "type": "STRING",
      "required": false,
      "nullable": true
    },
    "shopName": {
      "type": "STRING",
      "required": false,
      "nullable": true
    },
    "staffName": {
      "type": "STRING",
      "required": false,
      "nullable": true
    },
    "srcRequestId": {
      "type": "LONG",
      "required": false,
      "nullable": true
    }
  }
}
$json$::jsonb)
ON CONFLICT (schema_id) DO NOTHING;

INSERT INTO schema_definitions (schema_id, schema_payload) VALUES
('stream_P1-EVENT-TRACKING_v1', $json$
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "schema_id": "stream_P1-EVENT-TRACKING_v1",
  "schema_type": "STREAM",
  "source": "EVT",
  "topic": "P1-EVENT-TRACKING",
  "version": "v1",
  "description": "Clickstream sự kiện tương tác ứng dụng di động ViettelMoney",
  "key_definition": {
    "field": "identity",
    "key_type": "PHONE_E164",
    "auto_normalize": true,
    "default_country_code": "84"
  },
  "fields": {
    "id": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Mã sự kiện duy nhất UUID"
    },
    "action": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Hành vi người dùng (click, view)"
    },
    "app_name": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Tên ứng dụng (ViettelMoney)"
    },
    "app_version": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Phiên bản ứng dụng (5.2.1)"
    },
    "device_session_id": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Phiên thiết bị duy nhất"
    },
    "event_src": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Nguồn sự kiện (APP_CLIENT)"
    },
    "language": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Ngôn ngữ máy (vi)"
    },
    "manufacturer": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Hãng sản xuất thiết bị (Apple, Samsung)"
    },
    "model": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Dòng máy"
    },
    "object_name": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Tên đối tượng tương tác (Telecom_topup_view_info_user_button_continue, Telecom_topup_view_transactionresult_app_view_info)"
    },
    "object_type": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Loại đối tượng (BUTTON, VIEW)"
    },
    "os": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Hệ điều hành (Android, iOS)"
    },
    "os_version": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Phiên bản hệ điều hành"
    },
    "systemFake": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Hệ thống lõi (VTM_CORE)"
    },
    "time_stamp": {
      "type": "LONG",
      "required": true,
      "nullable": false,
      "description": "Thời điểm xảy ra sự kiện Epoch Millis"
    },
    "time_zone": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Múi giờ (Asia/Ho_Chi_Minh)"
    },
    "universeFake": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Môi trường (PROD)"
    },
    "identity": {
      "type": "STRING",
      "required": false,
      "nullable": true,
      "description": "Số điện thoại định danh người dùng"
    },
    "imei": {
      "type": "STRING",
      "required": false,
      "nullable": true,
      "description": "Số IMEI thiết bị"
    },
    "ip_addr": {
      "type": "STRING",
      "required": false,
      "nullable": true,
      "description": "Địa chỉ IP người dùng"
    },
    "user_session_id": {
      "type": "STRING",
      "required": false,
      "nullable": true,
      "description": "Mã phiên đăng nhập người dùng"
    },
    "event_value": {
      "type": "STRING",
      "required": false,
      "nullable": true,
      "description": "JSON giá trị ngữ cảnh sự kiện (orderId, amount)"
    },
    "geo": {
      "type": "STRING",
      "required": false,
      "nullable": true,
      "description": "Tọa độ địa lý (kinh độ, vĩ độ)"
    }
  }
}
$json$::jsonb)
ON CONFLICT (schema_id) DO NOTHING;

INSERT INTO schema_definitions (schema_id, schema_payload) VALUES
('stream_GNOTIFY_SAVE_MESSAGE_HBASE_v1', $json$
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "schema_id": "stream_GNOTIFY_SAVE_MESSAGE_HBASE_v1",
  "schema_type": "STREAM",
  "source": "GNOTI",
  "topic": "GNOTIFY_SAVE_MESSAGE_HBASE",
  "version": "v1",
  "description": "Thông báo biến động số dư giao dịch cộng tiền (CREDIT) phục vụ bài toán A1",
  "key_definition": {
    "field": "msisdn",
    "key_type": "PHONE_E164",
    "auto_normalize": true,
    "default_country_code": "84"
  },
  "fields": {
    "msisdn": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Số điện thoại nhận thông báo"
    },
    "clientId": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "ID ứng dụng tích hợp"
    },
    "clientCode": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Mã đối tác thanh toán (NAPAS, VietQR, ViCong, MB, VTP)"
    },
    "client_code": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Mã đối tác (snake_case)"
    },
    "requestId": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Mã yêu cầu giao dịch"
    },
    "orderId": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Mã đơn hàng liên kết"
    },
    "channelType": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Kênh thông báo (APP_PUSH)"
    },
    "msgType": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Loại tin nhắn (BALANCE_UPDATE)"
    },
    "msgContent": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Nội dung tin nhắn biến động số dư"
    },
    "msg_content": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Nội dung tin nhắn (snake_case)"
    },
    "templateId": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "ID mẫu thông báo"
    },
    "accountId": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Số tài khoản ví"
    },
    "amount": {
      "type": "LONG",
      "required": true,
      "nullable": false,
      "min": 0,
      "description": "Số tiền biến động (VNĐ)"
    },
    "trans_amount": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Số tiền dạng chuỗi"
    },
    "fee": {
      "type": "LONG",
      "required": true,
      "nullable": false,
      "min": 0,
      "description": "Phí biến động"
    },
    "balance": {
      "type": "LONG",
      "required": true,
      "nullable": false,
      "description": "Số dư ví sau biến động (VNĐ)"
    },
    "bankTransId": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Mã giao dịch ngân hàng"
    },
    "bank_trans_id": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Mã giao dịch ngân hàng (snake_case)"
    },
    "description": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Diễn giải giao dịch"
    },
    "processCode": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Mã tiến trình"
    },
    "serviceCode": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Mã dịch vụ"
    },
    "cardType": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Loại thẻ (DOMESTIC)"
    },
    "fullAddress": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Địa chỉ khách hàng"
    },
    "originalBankTransId": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Mã giao dịch ngân hàng gốc"
    },
    "sourceBankCode": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Mã ngân hàng nguồn"
    },
    "sourceAccountNumber": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Số tài khoản nguồn"
    },
    "sourceCustomerName": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Tên chủ tài khoản nguồn"
    },
    "paymentType": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "allowed_values": [
        "CREDIT"
      ],
      "description": "Chiều giao dịch: Bắt buộc CREDIT (cộng tiền)"
    },
    "payment_type": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "allowed_values": [
        "CREDIT"
      ],
      "description": "Chiều giao dịch (snake_case)"
    },
    "status": {
      "type": "INT",
      "required": true,
      "nullable": false,
      "description": "Trạng thái (1 = Thành công, 0 = Thất bại)"
    },
    "transDetailContent": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Chi tiết thanh toán"
    },
    "failureCount": {
      "type": "INT",
      "required": true,
      "nullable": false,
      "description": "Số lần thất bại"
    },
    "pushContent": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Nội dung đẩy thông báo"
    },
    "errorCode": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Mã lỗi (00)"
    },
    "errorMsg": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Thông báo lỗi"
    },
    "additionalInfoOne": {
      "type": "STRING",
      "required": true,
      "nullable": false
    },
    "additionalInfoTwo": {
      "type": "STRING",
      "required": true,
      "nullable": false
    },
    "iosDeeplink": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Deeplink iOS mở màn hình chi tiết"
    },
    "androidDeeplink": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Deeplink Android mở màn hình chi tiết"
    },
    "transDate": {
      "type": "LONG",
      "required": true,
      "nullable": false,
      "description": "Thời gian giao dịch Epoch Millis"
    },
    "trans_date": {
      "type": "LONG",
      "required": true,
      "nullable": false,
      "description": "Thời gian giao dịch (snake_case)"
    },
    "createdDate": {
      "type": "LONG",
      "required": true,
      "nullable": false,
      "description": "Thời gian tạo bản ghi"
    },
    "lastModifiedDate": {
      "type": "LONG",
      "required": true,
      "nullable": false,
      "description": "Thời gian cập nhật bản ghi"
    },
    "segment": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Phân khúc khách hàng (RETAIL)"
    }
  }
}
$json$::jsonb)
ON CONFLICT (schema_id) DO NOTHING;

INSERT INTO schema_definitions (schema_id, schema_payload) VALUES
('stream_history_service_insert_hbase_product_v1', $json$
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "schema_id": "stream_history_service_insert_hbase_product_v1",
  "schema_type": "STREAM",
  "source": "COREPAY",
  "topic": "history_service_insert_hbase_product",
  "version": "v1",
  "description": "Lịch sử giao dịch dịch vụ sản phẩm ghi vào HBase",
  "key_definition": {
    "field": "identifyValue",
    "key_type": "PHONE_E164",
    "auto_normalize": true,
    "default_country_code": "84"
  },
  "fields": {
    "tableName": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Tên bảng HBase đích (history_product)"
    },
    "identifyValue": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Số điện thoại / khóa định danh khách hàng"
    },
    "originalRequestId": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Mã yêu cầu gốc"
    },
    "transDate": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Thời gian giao dịch"
    },
    "content": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "JSON nội dung sản phẩm dịch vụ"
    },
    "logTransaction": {
      "type": "BOOLEAN",
      "required": true,
      "nullable": false,
      "description": "Cờ ghi log giao dịch"
    },
    "serviceName": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Tên dịch vụ"
    },
    "eventName": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Tên sự kiện (INSERT_PRODUCT_HISTORY)"
    }
  }
}
$json$::jsonb)
ON CONFLICT (schema_id) DO NOTHING;

INSERT INTO schema_definitions (schema_id, schema_payload) VALUES
('stream_HISTORY_SERVICE_INSERT_HBASE_OBJECT_v1', $json$
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "schema_id": "stream_HISTORY_SERVICE_INSERT_HBASE_OBJECT_v1",
  "schema_type": "STREAM",
  "source": "COREPAY",
  "topic": "HISTORY_SERVICE_INSERT_HBASE_OBJECT",
  "version": "v1",
  "description": "Lịch sử thao tác đối tượng giao dịch ghi vào HBase phục vụ bài toán B5",
  "key_definition": {
    "field": "identifyValue",
    "key_type": "PHONE_E164",
    "auto_normalize": true,
    "default_country_code": "84"
  },
  "fields": {
    "eventName": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Tên sự kiện (INSERT_OBJECT_HISTORY)"
    },
    "originalRequestId": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Mã yêu cầu gốc"
    },
    "transDate": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Thời gian giao dịch"
    },
    "identifyValue": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Số điện thoại / định danh khách hàng"
    },
    "logTransaction": {
      "type": "BOOLEAN",
      "required": true,
      "nullable": false,
      "description": "Cờ ghi log giao dịch"
    },
    "tableName": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Tên bảng HBase đích (history_object)"
    },
    "serviceName": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Tên dịch vụ thanh toán"
    },
    "serviceCode": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Mã dịch vụ thanh toán"
    },
    "master": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Mã nghiệp vụ đối tượng chi tiết"
    },
    "processCode": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Mã tiến trình xử lý"
    },
    "paymentId": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Mã giao dịch thanh toán"
    },
    "requestId": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Mã yêu cầu"
    },
    "paymentDetails": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Chi tiết thanh toán dạng JSON string"
    },
    "content": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Nội dung diễn giải lưu vết"
    }
  }
}
$json$::jsonb)
ON CONFLICT (schema_id) DO NOTHING;

INSERT INTO schema_definitions (schema_id, schema_payload) VALUES
('stream_cdcn_log_central_prod_v1', $json$
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "schema_id": "stream_cdcn_log_central_prod_v1",
  "schema_type": "STREAM",
  "source": "CDCN",
  "topic": "cdcn_log_central_prod",
  "version": "v1",
  "description": "Log tập trung từ central API gateway cho các giao dịch (Bài toán B5)",
  "key_definition": {
    "field": "account",
    "key_type": "PHONE_E164",
    "auto_normalize": true,
    "default_country_code": "84"
  },
  "fields": {
    "applicationCode": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Mã ứng dụng gateway (VTM_API_GATEWAY)"
    },
    "account": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Tài khoản / số điện thoại người dùng"
    },
    "serviceCode": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Mã dịch vụ gọi qua gateway"
    },
    "threadID": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Mã luồng xử lý server"
    },
    "requestID": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Mã yêu cầu API"
    },
    "sessionID": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Mã phiên làm việc người dùng"
    },
    "ipPortParentNode": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "IP và cổng nút cha"
    },
    "ipPortCurrentNode": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "IP và cổng nút xử lý hiện tại"
    },
    "startTime": {
      "type": "LONG",
      "required": true,
      "nullable": false,
      "description": "Thời điểm bắt đầu xử lý Epoch Millis"
    },
    "endTime": {
      "type": "LONG",
      "required": true,
      "nullable": false,
      "description": "Thời điểm kết thúc xử lý Epoch Millis"
    },
    "requestContent": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Nội dung JSON request payload"
    },
    "duration": {
      "type": "LONG",
      "required": true,
      "nullable": false,
      "description": "Thời gian thực thi API (mili-giây)"
    },
    "errorCode": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Mã lỗi phản hồi (00 = Thành công)"
    },
    "errorDescription": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Mô tả chi tiết mã lỗi"
    },
    "transactionStatus": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "allowed_values": [
        "SUCCESS",
        "FAILED"
      ],
      "description": "Trạng thái giao dịch"
    },
    "actionName": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Tên hành động API thực thi"
    },
    "username": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Tên định danh tài khoản gọi API"
    },
    "threadName": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Tên luồng thực thi trong container"
    },
    "sourceClass": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Lớp Java xử lý giao dịch"
    },
    "sourceLine": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Dòng code xử lý"
    },
    "sourceMethod": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Hàm xử lý nghiệp vụ"
    },
    "serviceProvider": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Đơn vị cung cấp dịch vụ"
    },
    "transactionID": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Mã giao dịch nghiệp vụ orderId"
    },
    "clientRequestID": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Mã request phía client"
    },
    "clientIP": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Địa chỉ IP client gọi lên"
    },
    "responseContent": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "JSON phản hồi từ gateway"
    },
    "transactionType": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Loại giao dịch"
    },
    "system": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Hệ thống xử lý (VTM_CORE)"
    },
    "actionType": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Loại hành động (ONLINE_PAYMENT)"
    },
    "dataType": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Định dạng dữ liệu (JSON)"
    },
    "numRecord": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Số lượng bản ghi"
    },
    "correlationID": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Mã liên kết giao dịch cross-system"
    },
    "imeiTel": {
      "type": "STRING",
      "required": false,
      "nullable": true,
      "description": "IMEI thiết bị di động"
    },
    "imei": {
      "type": "STRING",
      "required": false,
      "nullable": true,
      "description": "IMEI máy"
    },
    "appVersion": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Phiên bản app"
    },
    "spanId": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Mã trace span distributed tracing"
    },
    "typeOS": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Hệ điều hành di động"
    },
    "osVersion": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Phiên bản OS"
    },
    "userAgent": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Chuỗi User-Agent gọi API"
    },
    "client": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Loại client (MOBILE_CLIENT)"
    }
  }
}
$json$::jsonb)
ON CONFLICT (schema_id) DO NOTHING;

INSERT INTO schema_definitions (schema_id, schema_payload) VALUES
('stream_ADS-THIRD-PARTY-GIFT-DATA-RESULT-CMD_v1', $json$
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "schema_id": "stream_ADS-THIRD-PARTY-GIFT-DATA-RESULT-CMD_v1",
  "schema_type": "STREAM",
  "source": "ADS",
  "topic": "ADS-THIRD-PARTY-GIFT-DATA-RESULT-CMD",
  "version": "v1",
  "description": "Callback kết quả tặng quà / nạp thẻ data từ đối tác thứ 3 (Bài toán B3)",
  "key_definition": {
    "field": "data.request.msisdn",
    "key_type": "PHONE_E164",
    "auto_normalize": true,
    "default_country_code": "84"
  },
  "fields": {
    "signature": {
      "type": "STRING",
      "required": false,
      "nullable": true,
      "description": "Chữ ký xác thực số (thường là null)"
    },
    "data": {
      "type": "OBJECT",
      "required": true,
      "nullable": false,
      "description": "Khối dữ liệu phản hồi nghiệp vụ callback"
    },
    "data.cmd": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Tên lệnh thực thi (GIFT_RESULT)"
    },
    "data.cmdDesc": {
      "type": "STRING",
      "required": false,
      "nullable": true,
      "description": "Mô tả lệnh"
    },
    "data.messageId": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Mã định danh bản tin duy nhất"
    },
    "data.paymentId": {
      "type": "STRING",
      "required": false,
      "nullable": true,
      "description": "Mã thanh toán"
    },
    "data.time": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Thời gian gửi kết quả"
    },
    "data.request": {
      "type": "OBJECT",
      "required": true,
      "nullable": false,
      "description": "Khối chi tiết kết quả quà tặng"
    },
    "data.request.billCode": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Mã hóa đơn dịch vụ quà tặng"
    },
    "data.request.errorCode": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Mã lỗi đối tác trả về (00 = Thành công, 01 = Timeout)"
    },
    "data.request.errorMessage": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Thông báo lỗi chi tiết"
    },
    "data.request.msisdn": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Số điện thoại người nhận quà"
    },
    "data.request.requestId": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Mã định danh yêu cầu"
    },
    "data.request.status": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Trạng thái mã kết quả (00 = Thành công, 01 = Thất bại)"
    },
    "data.request.statusMessage": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Chuỗi trạng thái (SUCCESS, FAILED)"
    },
    "data.request.transferMoneyRequestId": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Mã yêu cầu điều chuyển tiền"
    },
    "data.request.processDate": {
      "type": "OBJECT",
      "required": true,
      "nullable": false,
      "description": "Cấu trúc thời gian chi tiết năm, tháng, ngày, giờ, phút, giây"
    }
  }
}
$json$::jsonb)
ON CONFLICT (schema_id) DO NOTHING;

INSERT INTO schema_definitions (schema_id, schema_payload) VALUES
('stream_core-recharge-history_v1', $json$
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "schema_id": "stream_core-recharge-history_v1",
  "schema_type": "STREAM",
  "source": "TOPUP",
  "topic": "core-recharge-history",
  "version": "v1",
  "description": "Lịch sử nạp tiền vào ví qua liên kết ngân hàng (Bài toán B5)",
  "key_definition": {
    "field": "msisdn",
    "key_type": "PHONE_E164",
    "auto_normalize": true,
    "default_country_code": "84"
  },
  "fields": {
    "benAccNo": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Số tài khoản ví được nạp tiền"
    },
    "benBankCode": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Mã ngân hàng liên kết (MBBANK)"
    },
    "debitErrorCode": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Mã lỗi trừ tiền tài khoản ngân hàng (00 = Thành công)"
    },
    "debitLinkType": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Hình thức liên kết (DIRECT_LINK)"
    },
    "debitStatus": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Trạng thái trừ tiền ngân hàng (SUCCESS, FAILED)"
    },
    "debitTransactionId": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Mã giao dịch trừ tiền phía ngân hàng"
    },
    "msisdn": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Số điện thoại thuê bao thực hiện nạp tiền"
    },
    "orderId": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Mã đơn hàng nạp tiền"
    },
    "rechargeErrorCode": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Mã lỗi cộng tiền vào ví VTM (00 = Thành công, 01 = Timeout)"
    },
    "rechargeStatus": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Trạng thái cộng tiền ví (SUCCESS, FAILED)"
    },
    "serviceCode": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Mã dịch vụ (RECHARGE_BANK)"
    },
    "status": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "allowed_values": [
        "SUCCESS",
        "FAILED"
      ],
      "description": "Trạng thái tổng hợp giao dịch nạp tiền"
    },
    "tranContent": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Nội dung giao dịch nạp tiền"
    },
    "transAmount": {
      "type": "LONG",
      "required": true,
      "nullable": false,
      "min": 0,
      "description": "Số tiền nạp (VNĐ)"
    },
    "transDate": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Thời gian giao dịch"
    },
    "transFee": {
      "type": "LONG",
      "required": true,
      "nullable": false,
      "min": 0,
      "description": "Phí nạp tiền"
    },
    "rechargeTransactionId": {
      "type": "STRING",
      "required": false,
      "nullable": true,
      "description": "Mã giao dịch cộng tiền ví (nếu có)"
    }
  }
}
$json$::jsonb)
ON CONFLICT (schema_id) DO NOTHING;

INSERT INTO schema_definitions (schema_id, schema_payload) VALUES
('stream_PMT-TRANSACTION-SYNC-CMD_v1', $json$
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "schema_id": "stream_PMT-TRANSACTION-SYNC-CMD_v1",
  "schema_type": "STREAM",
  "source": "CPM",
  "topic": "PMT-TRANSACTION-SYNC-CMD",
  "version": "v1",
  "description": "Nguồn CPM - Đồng bộ giao dịch Core Payment phục vụ bài toán A1 (Deduplication đa nguồn)",
  "key_definition": {
    "field": "data.request.identifyValue",
    "key_type": "PHONE_E164",
    "auto_normalize": true,
    "default_country_code": "84"
  },
  "fields": {
    "signature": {
      "type": "STRING",
      "required": false,
      "nullable": true,
      "description": "Chữ ký số (thường là null)"
    },
    "data": {
      "type": "OBJECT",
      "required": true,
      "nullable": false,
      "description": "Khối thông tin lệnh đồng bộ"
    },
    "data.cmd": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Lệnh thực thi (DONG_BO_GIAO_DICH)"
    },
    "data.messageId": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Mã định danh bản tin duy nhất"
    },
    "data.originalPaymentId": {
      "type": "STRING",
      "required": false,
      "nullable": true
    },
    "data.paymentDetailId": {
      "type": "STRING",
      "required": false,
      "nullable": true
    },
    "data.paymentId": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Mã thanh toán core payment"
    },
    "data.time": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Thời gian tạo bản tin"
    },
    "data.viettelRequestId": {
      "type": "STRING",
      "required": false,
      "nullable": true
    },
    "data.request": {
      "type": "OBJECT",
      "required": true,
      "nullable": false,
      "description": "Khối yêu cầu chi tiết"
    },
    "data.request.billingCode": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Mã hóa đơn dịch vụ"
    },
    "data.request.identifyValue": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Số điện thoại khách hàng thực hiện (khóa liên kết)"
    },
    "data.request.logTransaction": {
      "type": "BOOLEAN",
      "required": true,
      "nullable": false,
      "description": "Cờ ghi log giao dịch"
    },
    "data.request.moneySourceType": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Nguồn tiền giao dịch"
    },
    "data.request.originalRequestId": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Mã request gốc liên kết với V1-INSERT-TRANS-DAILY-HIS.requestId"
    },
    "data.request.processCode": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Mã tiến trình nghiệp vụ"
    },
    "data.request.processName": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Tên tiến trình nghiệp vụ"
    },
    "data.request.serviceCode": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Mã dịch vụ thanh toán"
    },
    "data.request.serviceName": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Tên dịch vụ thanh toán"
    },
    "data.request.transDate": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Thời gian giao dịch"
    },
    "data.request.viettelBankCode": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Mã ngân hàng Viettel (VTM)"
    },
    "data.request.detailSources": {
      "type": "ARRAY",
      "required": true,
      "nullable": false,
      "description": "Danh sách các nguồn tiền con cấu thành"
    },
    "data.request.content": {
      "type": "OBJECT",
      "required": true,
      "nullable": false,
      "description": "Khối nội dung chi tiết giao dịch"
    },
    "data.request.content.accountId": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Số tài khoản ví"
    },
    "data.request.content.accountType": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Loại tài khoản ví"
    },
    "data.request.content.additionalInfo": {
      "type": "STRING",
      "required": true,
      "nullable": false
    },
    "data.request.content.billingCode": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Mã hóa đơn"
    },
    "data.request.content.channel": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Kênh giao dịch"
    },
    "data.request.content.errorCode": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Mã lỗi giao dịch (SUCCESS khi thành công)"
    },
    "data.request.content.identifyValue": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Số điện thoại định danh khách hàng"
    },
    "data.request.content.requestDate": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Ngày giờ gửi yêu cầu"
    },
    "data.request.content.requestId": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Mã định danh yêu cầu"
    },
    "data.request.content.responseDate": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Ngày giờ nhận phản hồi"
    },
    "data.request.content.serviceCode": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Mã dịch vụ"
    },
    "data.request.content.transAmount": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Số tiền giao dịch gốc dạng chuỗi (VNĐ)"
    },
    "data.request.content.transDesc": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Diễn giải giao dịch"
    },
    "data.request.content.transFee": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Phí giao dịch dạng chuỗi (VNĐ)"
    },
    "data.request.content.transType": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Loại giao dịch (PAYMENT)"
    }
  }
}
$json$::jsonb)
ON CONFLICT (schema_id) DO NOTHING;

-- ============================================================
-- C. REALTIME STREAM SCHEMA ALIASES (THEO SOURCE CODE TRONG RULE TRIGGER)
-- Cho phép Rule Engine / Inverted Index tra cứu schema trực tiếp theo source code
-- (TDH, EVT, GNOTI, CPM, ADS, CDCN, TOPUP, COREPAY)
-- ============================================================

INSERT INTO schema_definitions (schema_id, schema_payload) VALUES
('stream_TDH_v1', $json$
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "schema_id": "stream_TDH_v1",
  "schema_type": "STREAM",
  "source": "TDH",
  "topic": "V1-INSERT-TRANS-DAILY-HIS",
  "version": "v1",
  "description": "Lịch sử giao dịch tài chính lõi core payment (Insert)",
  "key_definition": {
    "field": "msisdn",
    "key_type": "PHONE_E164",
    "auto_normalize": true,
    "default_country_code": "84"
  },
  "fields": {
    "processCode": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Mã tiến trình xử lý giao dịch"
    },
    "requestContent": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "JSON nội dung yêu cầu thanh toán"
    },
    "requestDate": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Thời gian gửi yêu cầu (yyyy-MM-dd HH:mm:ss)"
    },
    "requestId": {
      "type": "LONG",
      "required": true,
      "nullable": false,
      "description": "Mã định danh yêu cầu giao dịch"
    },
    "requestMti": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Message Type Identifier yêu cầu (0200)"
    },
    "responseDate": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Thời gian phản hồi giao dịch"
    },
    "transDailyHisFinanceId": {
      "type": "LONG",
      "required": true,
      "nullable": false,
      "description": "ID khóa chính giao dịch tài chính"
    },
    "msisdn": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Số điện thoại thuê bao thực hiện"
    },
    "custId": {
      "type": "INT",
      "required": true,
      "nullable": false,
      "description": "Mã khách hàng trong hệ thống"
    },
    "custName": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Tên khách hàng"
    },
    "custMobileNo": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Số di động liên hệ khách hàng"
    },
    "idNo": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Số giấy tờ tùy thân (CCCD/CMND)"
    },
    "idType": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Loại giấy tờ tùy thân (CCCD)"
    },
    "idTypeName": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Tên loại giấy tờ tùy thân"
    },
    "gender": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Giới tính (M/F)"
    },
    "birthday": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Ngày sinh (yyyy-MM-dd)"
    },
    "custAddress": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Địa chỉ khách hàng"
    },
    "mobileId": {
      "type": "INT",
      "required": true,
      "nullable": false,
      "description": "ID định danh thiết bị/thuê bao di động"
    },
    "transAmount": {
      "type": "LONG",
      "required": true,
      "nullable": false,
      "min": 0,
      "description": "Số tiền giao dịch gốc (VNĐ)"
    },
    "transFee": {
      "type": "LONG",
      "required": true,
      "nullable": false,
      "min": 0,
      "description": "Phí giao dịch (VNĐ)"
    },
    "discount": {
      "type": "LONG",
      "required": true,
      "nullable": false,
      "min": 0,
      "description": "Số tiền giảm giá / khuyến mại (VNĐ)"
    },
    "finalAmount": {
      "type": "LONG",
      "required": true,
      "nullable": false,
      "min": 0,
      "description": "Số tiền thanh toán cuối cùng (VNĐ)"
    },
    "errorCode": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Mã lỗi giao dịch (00 = Thành công)"
    },
    "errorCodeName": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Mô tả tên mã lỗi"
    },
    "correctCode": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Mã hiệu chỉnh đối soát (00 = Bình thường, 05 = Chờ hiệu chỉnh)"
    },
    "correctCodeName": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Tên mã hiệu chỉnh đối soát"
    },
    "serviceCode": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Mã dịch vụ thanh toán (EVN, NUOC, FLTSEDU...)"
    },
    "transType": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Loại giao dịch (PAYMENT, TRANSFER)"
    },
    "transContent": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Nội dung giao dịch"
    },
    "viettelBankCode": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Mã ngân hàng / ví đối tác (VTM)"
    },
    "appId": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Mã ứng dụng nguồn (VTM)"
    },
    "shopCode": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Mã điểm bán / kênh giao dịch"
    },
    "shopName": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Tên điểm bán / kênh giao dịch"
    },
    "staffCode": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Mã nhân viên / hệ thống thực hiện"
    },
    "staffName": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Tên nhân viên / hệ thống"
    },
    "accNo": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Số tài khoản ví Viettel Money"
    },
    "accName": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Tên chủ tài khoản ví"
    },
    "accType": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Loại tài khoản ví"
    },
    "accTypeName": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Tên hiển thị loại tài khoản"
    },
    "telcoCode": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Mã nhà mạng viễn thông (VTT)"
    },
    "nationCode": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Mã quốc gia (VNM)"
    },
    "nationName": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Tên quốc gia"
    },
    "languageCode": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Mã ngôn ngữ (vi)"
    },
    "languageName": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Tên ngôn ngữ"
    },
    "responseMti": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Message Type Identifier phản hồi (0210)"
    },
    "partnerRequestId": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Mã yêu cầu phía đối tác"
    },
    "billCode": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Mã hóa đơn dịch vụ"
    },
    "benAccNo": {
      "type": "STRING",
      "required": false,
      "nullable": true,
      "description": "Số tài khoản người thụ hưởng (khi chuyển tiền)"
    },
    "benBankCode": {
      "type": "STRING",
      "required": false,
      "nullable": true,
      "description": "Mã ngân hàng người thụ hưởng"
    },
    "benCustName": {
      "type": "STRING",
      "required": false,
      "nullable": true,
      "description": "Tên người thụ hưởng"
    }
  }
}
$json$::jsonb)
ON CONFLICT (schema_id) DO NOTHING;

INSERT INTO schema_definitions (schema_id, schema_payload) VALUES
('stream_EVT_v1', $json$
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "schema_id": "stream_EVT_v1",
  "schema_type": "STREAM",
  "source": "EVT",
  "topic": "P1-EVENT-TRACKING",
  "version": "v1",
  "description": "Clickstream sự kiện tương tác ứng dụng di động ViettelMoney",
  "key_definition": {
    "field": "identity",
    "key_type": "PHONE_E164",
    "auto_normalize": true,
    "default_country_code": "84"
  },
  "fields": {
    "id": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Mã sự kiện duy nhất UUID"
    },
    "action": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Hành vi người dùng (click, view)"
    },
    "app_name": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Tên ứng dụng (ViettelMoney)"
    },
    "app_version": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Phiên bản ứng dụng (5.2.1)"
    },
    "device_session_id": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Phiên thiết bị duy nhất"
    },
    "event_src": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Nguồn sự kiện (APP_CLIENT)"
    },
    "language": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Ngôn ngữ máy (vi)"
    },
    "manufacturer": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Hãng sản xuất thiết bị (Apple, Samsung)"
    },
    "model": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Dòng máy"
    },
    "object_name": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Tên đối tượng tương tác (Telecom_topup_view_info_user_button_continue, Telecom_topup_view_transactionresult_app_view_info)"
    },
    "object_type": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Loại đối tượng (BUTTON, VIEW)"
    },
    "os": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Hệ điều hành (Android, iOS)"
    },
    "os_version": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Phiên bản hệ điều hành"
    },
    "systemFake": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Hệ thống lõi (VTM_CORE)"
    },
    "time_stamp": {
      "type": "LONG",
      "required": true,
      "nullable": false,
      "description": "Thời điểm xảy ra sự kiện Epoch Millis"
    },
    "time_zone": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Múi giờ (Asia/Ho_Chi_Minh)"
    },
    "universeFake": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Môi trường (PROD)"
    },
    "identity": {
      "type": "STRING",
      "required": false,
      "nullable": true,
      "description": "Số điện thoại định danh người dùng"
    },
    "imei": {
      "type": "STRING",
      "required": false,
      "nullable": true,
      "description": "Số IMEI thiết bị"
    },
    "ip_addr": {
      "type": "STRING",
      "required": false,
      "nullable": true,
      "description": "Địa chỉ IP người dùng"
    },
    "user_session_id": {
      "type": "STRING",
      "required": false,
      "nullable": true,
      "description": "Mã phiên đăng nhập người dùng"
    },
    "event_value": {
      "type": "STRING",
      "required": false,
      "nullable": true,
      "description": "JSON giá trị ngữ cảnh sự kiện (orderId, amount)"
    },
    "geo": {
      "type": "STRING",
      "required": false,
      "nullable": true,
      "description": "Tọa độ địa lý (kinh độ, vĩ độ)"
    }
  }
}
$json$::jsonb)
ON CONFLICT (schema_id) DO NOTHING;

INSERT INTO schema_definitions (schema_id, schema_payload) VALUES
('stream_GNOTI_v1', $json$
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "schema_id": "stream_GNOTI_v1",
  "schema_type": "STREAM",
  "source": "GNOTI",
  "topic": "GNOTIFY_SAVE_MESSAGE_HBASE",
  "version": "v1",
  "description": "Thông báo biến động số dư giao dịch cộng tiền (CREDIT) phục vụ bài toán A1",
  "key_definition": {
    "field": "msisdn",
    "key_type": "PHONE_E164",
    "auto_normalize": true,
    "default_country_code": "84"
  },
  "fields": {
    "msisdn": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Số điện thoại nhận thông báo"
    },
    "clientId": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "ID ứng dụng tích hợp"
    },
    "clientCode": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Mã đối tác thanh toán (NAPAS, VietQR, ViCong, MB, VTP)"
    },
    "client_code": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Mã đối tác (snake_case)"
    },
    "requestId": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Mã yêu cầu giao dịch"
    },
    "orderId": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Mã đơn hàng liên kết"
    },
    "channelType": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Kênh thông báo (APP_PUSH)"
    },
    "msgType": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Loại tin nhắn (BALANCE_UPDATE)"
    },
    "msgContent": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Nội dung tin nhắn biến động số dư"
    },
    "msg_content": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Nội dung tin nhắn (snake_case)"
    },
    "templateId": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "ID mẫu thông báo"
    },
    "accountId": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Số tài khoản ví"
    },
    "amount": {
      "type": "LONG",
      "required": true,
      "nullable": false,
      "min": 0,
      "description": "Số tiền biến động (VNĐ)"
    },
    "trans_amount": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Số tiền dạng chuỗi"
    },
    "fee": {
      "type": "LONG",
      "required": true,
      "nullable": false,
      "min": 0,
      "description": "Phí biến động"
    },
    "balance": {
      "type": "LONG",
      "required": true,
      "nullable": false,
      "description": "Số dư ví sau biến động (VNĐ)"
    },
    "bankTransId": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Mã giao dịch ngân hàng"
    },
    "bank_trans_id": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Mã giao dịch ngân hàng (snake_case)"
    },
    "description": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Diễn giải giao dịch"
    },
    "processCode": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Mã tiến trình"
    },
    "serviceCode": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Mã dịch vụ"
    },
    "cardType": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Loại thẻ (DOMESTIC)"
    },
    "fullAddress": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Địa chỉ khách hàng"
    },
    "originalBankTransId": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Mã giao dịch ngân hàng gốc"
    },
    "sourceBankCode": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Mã ngân hàng nguồn"
    },
    "sourceAccountNumber": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Số tài khoản nguồn"
    },
    "sourceCustomerName": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Tên chủ tài khoản nguồn"
    },
    "paymentType": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "allowed_values": [
        "CREDIT"
      ],
      "description": "Chiều giao dịch: Bắt buộc CREDIT (cộng tiền)"
    },
    "payment_type": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "allowed_values": [
        "CREDIT"
      ],
      "description": "Chiều giao dịch (snake_case)"
    },
    "status": {
      "type": "INT",
      "required": true,
      "nullable": false,
      "description": "Trạng thái (1 = Thành công, 0 = Thất bại)"
    },
    "transDetailContent": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Chi tiết thanh toán"
    },
    "failureCount": {
      "type": "INT",
      "required": true,
      "nullable": false,
      "description": "Số lần thất bại"
    },
    "pushContent": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Nội dung đẩy thông báo"
    },
    "errorCode": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Mã lỗi (00)"
    },
    "errorMsg": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Thông báo lỗi"
    },
    "additionalInfoOne": {
      "type": "STRING",
      "required": true,
      "nullable": false
    },
    "additionalInfoTwo": {
      "type": "STRING",
      "required": true,
      "nullable": false
    },
    "iosDeeplink": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Deeplink iOS mở màn hình chi tiết"
    },
    "androidDeeplink": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Deeplink Android mở màn hình chi tiết"
    },
    "transDate": {
      "type": "LONG",
      "required": true,
      "nullable": false,
      "description": "Thời gian giao dịch Epoch Millis"
    },
    "trans_date": {
      "type": "LONG",
      "required": true,
      "nullable": false,
      "description": "Thời gian giao dịch (snake_case)"
    },
    "createdDate": {
      "type": "LONG",
      "required": true,
      "nullable": false,
      "description": "Thời gian tạo bản ghi"
    },
    "lastModifiedDate": {
      "type": "LONG",
      "required": true,
      "nullable": false,
      "description": "Thời gian cập nhật bản ghi"
    },
    "segment": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Phân khúc khách hàng (RETAIL)"
    }
  }
}
$json$::jsonb)
ON CONFLICT (schema_id) DO NOTHING;

INSERT INTO schema_definitions (schema_id, schema_payload) VALUES
('stream_CPM_v1', $json$
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "schema_id": "stream_CPM_v1",
  "schema_type": "STREAM",
  "source": "CPM",
  "topic": "PMT-TRANSACTION-SYNC-CMD",
  "version": "v1",
  "description": "Nguồn CPM - Đồng bộ giao dịch Core Payment phục vụ bài toán A1 (Deduplication đa nguồn)",
  "key_definition": {
    "field": "data.request.identifyValue",
    "key_type": "PHONE_E164",
    "auto_normalize": true,
    "default_country_code": "84"
  },
  "fields": {
    "signature": {
      "type": "STRING",
      "required": false,
      "nullable": true,
      "description": "Chữ ký số (thường là null)"
    },
    "data": {
      "type": "OBJECT",
      "required": true,
      "nullable": false,
      "description": "Khối thông tin lệnh đồng bộ"
    },
    "data.cmd": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Lệnh thực thi (DONG_BO_GIAO_DICH)"
    },
    "data.messageId": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Mã định danh bản tin duy nhất"
    },
    "data.originalPaymentId": {
      "type": "STRING",
      "required": false,
      "nullable": true
    },
    "data.paymentDetailId": {
      "type": "STRING",
      "required": false,
      "nullable": true
    },
    "data.paymentId": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Mã thanh toán core payment"
    },
    "data.time": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Thời gian tạo bản tin"
    },
    "data.viettelRequestId": {
      "type": "STRING",
      "required": false,
      "nullable": true
    },
    "data.request": {
      "type": "OBJECT",
      "required": true,
      "nullable": false,
      "description": "Khối yêu cầu chi tiết"
    },
    "data.request.billingCode": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Mã hóa đơn dịch vụ"
    },
    "data.request.identifyValue": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Số điện thoại khách hàng thực hiện (khóa liên kết)"
    },
    "data.request.logTransaction": {
      "type": "BOOLEAN",
      "required": true,
      "nullable": false,
      "description": "Cờ ghi log giao dịch"
    },
    "data.request.moneySourceType": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Nguồn tiền giao dịch"
    },
    "data.request.originalRequestId": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Mã request gốc liên kết với V1-INSERT-TRANS-DAILY-HIS.requestId"
    },
    "data.request.processCode": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Mã tiến trình nghiệp vụ"
    },
    "data.request.processName": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Tên tiến trình nghiệp vụ"
    },
    "data.request.serviceCode": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Mã dịch vụ thanh toán"
    },
    "data.request.serviceName": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Tên dịch vụ thanh toán"
    },
    "data.request.transDate": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Thời gian giao dịch"
    },
    "data.request.viettelBankCode": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Mã ngân hàng Viettel (VTM)"
    },
    "data.request.detailSources": {
      "type": "ARRAY",
      "required": true,
      "nullable": false,
      "description": "Danh sách các nguồn tiền con cấu thành"
    },
    "data.request.content": {
      "type": "OBJECT",
      "required": true,
      "nullable": false,
      "description": "Khối nội dung chi tiết giao dịch"
    },
    "data.request.content.accountId": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Số tài khoản ví"
    },
    "data.request.content.accountType": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Loại tài khoản ví"
    },
    "data.request.content.additionalInfo": {
      "type": "STRING",
      "required": true,
      "nullable": false
    },
    "data.request.content.billingCode": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Mã hóa đơn"
    },
    "data.request.content.channel": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Kênh giao dịch"
    },
    "data.request.content.errorCode": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Mã lỗi giao dịch (SUCCESS khi thành công)"
    },
    "data.request.content.identifyValue": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Số điện thoại định danh khách hàng"
    },
    "data.request.content.requestDate": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Ngày giờ gửi yêu cầu"
    },
    "data.request.content.requestId": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Mã định danh yêu cầu"
    },
    "data.request.content.responseDate": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Ngày giờ nhận phản hồi"
    },
    "data.request.content.serviceCode": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Mã dịch vụ"
    },
    "data.request.content.transAmount": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Số tiền giao dịch gốc dạng chuỗi (VNĐ)"
    },
    "data.request.content.transDesc": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Diễn giải giao dịch"
    },
    "data.request.content.transFee": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Phí giao dịch dạng chuỗi (VNĐ)"
    },
    "data.request.content.transType": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Loại giao dịch (PAYMENT)"
    }
  }
}
$json$::jsonb)
ON CONFLICT (schema_id) DO NOTHING;

INSERT INTO schema_definitions (schema_id, schema_payload) VALUES
('stream_ADS_v1', $json$
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "schema_id": "stream_ADS_v1",
  "schema_type": "STREAM",
  "source": "ADS",
  "topic": "ADS-THIRD-PARTY-GIFT-DATA-RESULT-CMD",
  "version": "v1",
  "description": "Callback kết quả tặng quà / nạp thẻ data từ đối tác thứ 3 (Bài toán B3)",
  "key_definition": {
    "field": "data.request.msisdn",
    "key_type": "PHONE_E164",
    "auto_normalize": true,
    "default_country_code": "84"
  },
  "fields": {
    "signature": {
      "type": "STRING",
      "required": false,
      "nullable": true,
      "description": "Chữ ký xác thực số (thường là null)"
    },
    "data": {
      "type": "OBJECT",
      "required": true,
      "nullable": false,
      "description": "Khối dữ liệu phản hồi nghiệp vụ callback"
    },
    "data.cmd": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Tên lệnh thực thi (GIFT_RESULT)"
    },
    "data.cmdDesc": {
      "type": "STRING",
      "required": false,
      "nullable": true,
      "description": "Mô tả lệnh"
    },
    "data.messageId": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Mã định danh bản tin duy nhất"
    },
    "data.paymentId": {
      "type": "STRING",
      "required": false,
      "nullable": true,
      "description": "Mã thanh toán"
    },
    "data.time": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Thời gian gửi kết quả"
    },
    "data.request": {
      "type": "OBJECT",
      "required": true,
      "nullable": false,
      "description": "Khối chi tiết kết quả quà tặng"
    },
    "data.request.billCode": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Mã hóa đơn dịch vụ quà tặng"
    },
    "data.request.errorCode": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Mã lỗi đối tác trả về (00 = Thành công, 01 = Timeout)"
    },
    "data.request.errorMessage": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Thông báo lỗi chi tiết"
    },
    "data.request.msisdn": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Số điện thoại người nhận quà"
    },
    "data.request.requestId": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Mã định danh yêu cầu"
    },
    "data.request.status": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Trạng thái mã kết quả (00 = Thành công, 01 = Thất bại)"
    },
    "data.request.statusMessage": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Chuỗi trạng thái (SUCCESS, FAILED)"
    },
    "data.request.transferMoneyRequestId": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Mã yêu cầu điều chuyển tiền"
    },
    "data.request.processDate": {
      "type": "OBJECT",
      "required": true,
      "nullable": false,
      "description": "Cấu trúc thời gian chi tiết năm, tháng, ngày, giờ, phút, giây"
    }
  }
}
$json$::jsonb)
ON CONFLICT (schema_id) DO NOTHING;

INSERT INTO schema_definitions (schema_id, schema_payload) VALUES
('stream_CDCN_v1', $json$
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "schema_id": "stream_CDCN_v1",
  "schema_type": "STREAM",
  "source": "CDCN",
  "topic": "cdcn_log_central_prod",
  "version": "v1",
  "description": "Log tập trung từ central API gateway cho các giao dịch (Bài toán B5)",
  "key_definition": {
    "field": "account",
    "key_type": "PHONE_E164",
    "auto_normalize": true,
    "default_country_code": "84"
  },
  "fields": {
    "applicationCode": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Mã ứng dụng gateway (VTM_API_GATEWAY)"
    },
    "account": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Tài khoản / số điện thoại người dùng"
    },
    "serviceCode": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Mã dịch vụ gọi qua gateway"
    },
    "threadID": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Mã luồng xử lý server"
    },
    "requestID": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Mã yêu cầu API"
    },
    "sessionID": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Mã phiên làm việc người dùng"
    },
    "ipPortParentNode": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "IP và cổng nút cha"
    },
    "ipPortCurrentNode": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "IP và cổng nút xử lý hiện tại"
    },
    "startTime": {
      "type": "LONG",
      "required": true,
      "nullable": false,
      "description": "Thời điểm bắt đầu xử lý Epoch Millis"
    },
    "endTime": {
      "type": "LONG",
      "required": true,
      "nullable": false,
      "description": "Thời điểm kết thúc xử lý Epoch Millis"
    },
    "requestContent": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Nội dung JSON request payload"
    },
    "duration": {
      "type": "LONG",
      "required": true,
      "nullable": false,
      "description": "Thời gian thực thi API (mili-giây)"
    },
    "errorCode": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Mã lỗi phản hồi (00 = Thành công)"
    },
    "errorDescription": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Mô tả chi tiết mã lỗi"
    },
    "transactionStatus": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "allowed_values": [
        "SUCCESS",
        "FAILED"
      ],
      "description": "Trạng thái giao dịch"
    },
    "actionName": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Tên hành động API thực thi"
    },
    "username": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Tên định danh tài khoản gọi API"
    },
    "threadName": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Tên luồng thực thi trong container"
    },
    "sourceClass": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Lớp Java xử lý giao dịch"
    },
    "sourceLine": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Dòng code xử lý"
    },
    "sourceMethod": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Hàm xử lý nghiệp vụ"
    },
    "serviceProvider": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Đơn vị cung cấp dịch vụ"
    },
    "transactionID": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Mã giao dịch nghiệp vụ orderId"
    },
    "clientRequestID": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Mã request phía client"
    },
    "clientIP": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Địa chỉ IP client gọi lên"
    },
    "responseContent": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "JSON phản hồi từ gateway"
    },
    "transactionType": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Loại giao dịch"
    },
    "system": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Hệ thống xử lý (VTM_CORE)"
    },
    "actionType": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Loại hành động (ONLINE_PAYMENT)"
    },
    "dataType": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Định dạng dữ liệu (JSON)"
    },
    "numRecord": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Số lượng bản ghi"
    },
    "correlationID": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Mã liên kết giao dịch cross-system"
    },
    "imeiTel": {
      "type": "STRING",
      "required": false,
      "nullable": true,
      "description": "IMEI thiết bị di động"
    },
    "imei": {
      "type": "STRING",
      "required": false,
      "nullable": true,
      "description": "IMEI máy"
    },
    "appVersion": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Phiên bản app"
    },
    "spanId": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Mã trace span distributed tracing"
    },
    "typeOS": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Hệ điều hành di động"
    },
    "osVersion": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Phiên bản OS"
    },
    "userAgent": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Chuỗi User-Agent gọi API"
    },
    "client": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Loại client (MOBILE_CLIENT)"
    }
  }
}
$json$::jsonb)
ON CONFLICT (schema_id) DO NOTHING;

INSERT INTO schema_definitions (schema_id, schema_payload) VALUES
('stream_TOPUP_v1', $json$
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "schema_id": "stream_TOPUP_v1",
  "schema_type": "STREAM",
  "source": "TOPUP",
  "topic": "core-recharge-history",
  "version": "v1",
  "description": "Lịch sử nạp tiền vào ví qua liên kết ngân hàng (Bài toán B5)",
  "key_definition": {
    "field": "msisdn",
    "key_type": "PHONE_E164",
    "auto_normalize": true,
    "default_country_code": "84"
  },
  "fields": {
    "benAccNo": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Số tài khoản ví được nạp tiền"
    },
    "benBankCode": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Mã ngân hàng liên kết (MBBANK)"
    },
    "debitErrorCode": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Mã lỗi trừ tiền tài khoản ngân hàng (00 = Thành công)"
    },
    "debitLinkType": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Hình thức liên kết (DIRECT_LINK)"
    },
    "debitStatus": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Trạng thái trừ tiền ngân hàng (SUCCESS, FAILED)"
    },
    "debitTransactionId": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Mã giao dịch trừ tiền phía ngân hàng"
    },
    "msisdn": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Số điện thoại thuê bao thực hiện nạp tiền"
    },
    "orderId": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Mã đơn hàng nạp tiền"
    },
    "rechargeErrorCode": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Mã lỗi cộng tiền vào ví VTM (00 = Thành công, 01 = Timeout)"
    },
    "rechargeStatus": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Trạng thái cộng tiền ví (SUCCESS, FAILED)"
    },
    "serviceCode": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Mã dịch vụ (RECHARGE_BANK)"
    },
    "status": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "allowed_values": [
        "SUCCESS",
        "FAILED"
      ],
      "description": "Trạng thái tổng hợp giao dịch nạp tiền"
    },
    "tranContent": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Nội dung giao dịch nạp tiền"
    },
    "transAmount": {
      "type": "LONG",
      "required": true,
      "nullable": false,
      "min": 0,
      "description": "Số tiền nạp (VNĐ)"
    },
    "transDate": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Thời gian giao dịch"
    },
    "transFee": {
      "type": "LONG",
      "required": true,
      "nullable": false,
      "min": 0,
      "description": "Phí nạp tiền"
    },
    "rechargeTransactionId": {
      "type": "STRING",
      "required": false,
      "nullable": true,
      "description": "Mã giao dịch cộng tiền ví (nếu có)"
    }
  }
}
$json$::jsonb)
ON CONFLICT (schema_id) DO NOTHING;

INSERT INTO schema_definitions (schema_id, schema_payload) VALUES
('stream_COREPAY_product_v1', $json$
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "schema_id": "stream_COREPAY_product_v1",
  "schema_type": "STREAM",
  "source": "COREPAY",
  "topic": "history_service_insert_hbase_product",
  "version": "v1",
  "description": "Lịch sử giao dịch dịch vụ sản phẩm ghi vào HBase",
  "key_definition": {
    "field": "identifyValue",
    "key_type": "PHONE_E164",
    "auto_normalize": true,
    "default_country_code": "84"
  },
  "fields": {
    "tableName": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Tên bảng HBase đích (history_product)"
    },
    "identifyValue": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Số điện thoại / khóa định danh khách hàng"
    },
    "originalRequestId": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Mã yêu cầu gốc"
    },
    "transDate": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Thời gian giao dịch"
    },
    "content": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "JSON nội dung sản phẩm dịch vụ"
    },
    "logTransaction": {
      "type": "BOOLEAN",
      "required": true,
      "nullable": false,
      "description": "Cờ ghi log giao dịch"
    },
    "serviceName": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Tên dịch vụ"
    },
    "eventName": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Tên sự kiện (INSERT_PRODUCT_HISTORY)"
    }
  }
}
$json$::jsonb)
ON CONFLICT (schema_id) DO NOTHING;

INSERT INTO schema_definitions (schema_id, schema_payload) VALUES
('stream_COREPAY_object_v1', $json$
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "schema_id": "stream_COREPAY_object_v1",
  "schema_type": "STREAM",
  "source": "COREPAY",
  "topic": "HISTORY_SERVICE_INSERT_HBASE_OBJECT",
  "version": "v1",
  "description": "Lịch sử thao tác đối tượng giao dịch ghi vào HBase phục vụ bài toán B5",
  "key_definition": {
    "field": "identifyValue",
    "key_type": "PHONE_E164",
    "auto_normalize": true,
    "default_country_code": "84"
  },
  "fields": {
    "eventName": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Tên sự kiện (INSERT_OBJECT_HISTORY)"
    },
    "originalRequestId": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Mã yêu cầu gốc"
    },
    "transDate": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Thời gian giao dịch"
    },
    "identifyValue": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Số điện thoại / định danh khách hàng"
    },
    "logTransaction": {
      "type": "BOOLEAN",
      "required": true,
      "nullable": false,
      "description": "Cờ ghi log giao dịch"
    },
    "tableName": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Tên bảng HBase đích (history_object)"
    },
    "serviceName": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Tên dịch vụ thanh toán"
    },
    "serviceCode": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Mã dịch vụ thanh toán"
    },
    "master": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Mã nghiệp vụ đối tượng chi tiết"
    },
    "processCode": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Mã tiến trình xử lý"
    },
    "paymentId": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Mã giao dịch thanh toán"
    },
    "requestId": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Mã yêu cầu"
    },
    "paymentDetails": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Chi tiết thanh toán dạng JSON string"
    },
    "content": {
      "type": "STRING",
      "required": true,
      "nullable": false,
      "description": "Nội dung diễn giải lưu vết"
    }
  }
}
$json$::jsonb)
ON CONFLICT (schema_id) DO NOTHING;

-- ============================================================
-- D. BỔ SUNG TOPIC CHO KAFKA STREAM & BATCH CLUSTER CONFIG
-- ============================================================

-- 1. Thêm 10 Realtime Topics thực tế vào kafka_stream_topic_config
-- Topics thuộc cụm kafka-gssapi (Kerberos SASL_PLAINTEXT :29094)
INSERT INTO kafka_stream_topic_config (cluster_config_id, topic_name, enabled)
SELECT id, t.topic, TRUE FROM kafka_stream_cluster_config,
(VALUES 
  ('V1-INSERT-TRANS-DAILY-HIS'),
  ('V1-UPDATE-TRANS-DAILY-HIS'),
  ('P1-EVENT-TRACKING')
) AS t(topic)
WHERE stream_id = 'stream-events' AND cluster_name = 'kafka-gssapi'
ON CONFLICT (cluster_config_id, topic_name) DO NOTHING;

-- Topics thuộc cụm kafka-plain (SASL_PLAINTEXT PLAIN :29092)
INSERT INTO kafka_stream_topic_config (cluster_config_id, topic_name, enabled)
SELECT id, t.topic, TRUE FROM kafka_stream_cluster_config,
(VALUES 
  ('GNOTIFY_SAVE_MESSAGE_HBASE'),
  ('history_service_insert_hbase_product'),
  ('HISTORY_SERVICE_INSERT_HBASE_OBJECT'),
  ('cdcn_log_central_prod'),
  ('ADS-THIRD-PARTY-GIFT-DATA-RESULT-CMD'),
  ('core-recharge-history'),
  ('PMT-TRANSACTION-SYNC-CMD')
) AS t(topic)
WHERE stream_id = 'stream-events' AND cluster_name = 'kafka-plain'
ON CONFLICT (cluster_config_id, topic_name) DO NOTHING;

-- 2. Cấu hình Cluster cho Batch Events (kafka-plain:29092)
INSERT INTO kafka_batch_cluster_config (stream_id, cluster_name, bootstrap_servers, security_protocol, sasl_mechanism, sasl_kerberos_service_name, sasl_jaas_config, enabled)
VALUES (
    'batch-events',
    'kafka-plain',
    'kafka-plain:29092',
    'SASL_PLAINTEXT',
    'PLAIN',
    NULL,
    'org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="admin-secret";',
    TRUE
)
ON CONFLICT (stream_id, cluster_name) DO NOTHING;

-- 3. Thêm 7 Batch Topics vào kafka_batch_topic_config
INSERT INTO kafka_batch_topic_config (cluster_config_id, topic_name, enabled)
SELECT id, t.topic, TRUE FROM kafka_batch_cluster_config,
(VALUES 
  ('batch_trial_0d_registered'),
  ('batch_renewed_subscribers'),
  ('batch_active_promo_packages'),
  ('batch_blacklist_qtrr'),
  ('batch_simfarm_3_tram'),
  ('batch_cep_pushed_msisdn'),
  ('batch_vip_customer_list')
) AS t(topic)
WHERE stream_id = 'batch-events' AND cluster_name = 'kafka-plain'
ON CONFLICT (cluster_config_id, topic_name) DO NOTHING;

GRANT USAGE ON SCHEMA public TO replicator;

GRANT SELECT ON ALL TABLES IN SCHEMA public TO replicator;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO postgres;
GRANT SELECT, USAGE ON ALL SEQUENCES IN SCHEMA public TO replicator, postgres;

CREATE PUBLICATION realtime_publication
FOR TABLE
    public.rule_definitions,
    public.schema_definitions;
