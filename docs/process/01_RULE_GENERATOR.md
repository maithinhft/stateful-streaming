# Rule Generator — Quá trình thực hiện

## 1. Mục tiêu

Tạo tool sinh dữ liệu rule ngẫu nhiên theo đúng schema quy định trong [04_RULE_SCHEMA.md](../spec/04_RULE_SCHEMA.md), phục vụ:
- Test hệ thống xử lý rule engine với dữ liệu đa dạng
- Nạp dữ liệu vào PostgreSQL bảng `rule_definitions` để test CDC (Change Data Capture) qua Debezium
- Cung cấp 7 rule mẫu cụ thể ánh xạ từ 7 bài toán nghiệp vụ (A1, A2, B1–B5)

---

## 2. Thiết kế tổng quan

### Tách biệt 2 luồng: Gen ≠ Write

| Luồng | Class | Chức năng |
|---|---|---|
| **Gen** | `RuleSimulatorMain` | Sinh N rules ngẫu nhiên → ghi **1 file JSON** (mảng) ra `local/data/rules/` |
| **Write** | `RulePostgresWriter` | Đọc **1 file JSON** → ghi batch vào PostgreSQL |

Lý do tách: Không phải lúc nào gen xong cũng ghi ngay. Có thể gen trước, review/chỉnh sửa JSON, rồi mới quyết định ghi vào DB.

### Cấu trúc output

```
local/data/rules/
  ├── sample_7_rules.json            ← 7 rule mẫu nghiệp vụ (hardcoded)
  └── {NUM_RULES}/                   ← folder con = số rules
      └── 20260922_145700.json       ← 1 file JSON array, tên = timestamp dễ đọc
```

---

## 3. Danh sách source — ánh xạ từ 7 bài toán

| Source | Bài toán | Kafka Topic / Nguồn | Bản chất |
|---|---|---|---|
| `TDH` | A1 | trans_daily_his, corepayment | GD batch/near-RT |
| `EVT` | A1, A2 | Event tracking app VTM | Event hành vi |
| `GNOTI` | A1 | Bản tin biến động số dư | GD cộng tiền |
| `CPM` | A1 | Kafka core payment | GD thanh toán |
| `PMT` | B1 | PMT-THIRD-PARTY-PAYMENT-RESULT-CMD | Gia hạn sub |
| `SUB_MNGT` | B2 | sub-mngt (RT_LOG_CENTRAL_NEW) | Mua/gia hạn gói |
| `ADS` | B3 | ADS-THIRD-PARTY-GIFT-DATA-RESULT-CMD | Tặng gói data |
| `TMS` | B4 | etc-sync-tms-topic | Thanh toán ePass |
| `CDCN` | B5 | cdcn_log_central_prod | Đăng nhập |
| `TOPUP` | B5 | core-recharge-history | Nạp tiền liên kết |
| `COREPAY` | B5 | HISTORY_SERVICE_INSERT_HBASE_OBJECT | Topup/data/hoá đơn |
| `SAVING` | B5 | SAVINGS-SYNC-TRANS-HISTORY | Gửi tiết kiệm |

---

## 4. Thiết kế 7 rule mẫu nghiệp vụ

File: [`local/data/rules/sample_7_rules.json`](../../local/data/rules/sample_7_rules.json)

### 4.1. Rule A1 — RT_PSGD thứ 2 (Dedup đa nguồn)

| Thuộc tính | Giá trị |
|---|---|
| **rule_id** | `rule_A1_psgd_thu_2` |
| **Loại** | Stateful — `IS_FIRST_ARRIVAL` (Case 2 trong spec) |
| **Trigger** | 4 nguồn: TDH, EVT, GNOTI, CPM |
| **Mục tiêu** | Bắt GD đầu tiên trong tháng từ 4 nguồn, dedup theo `(msisdn, product)`, giữ event đến sớm nhất |

**Trigger conditions:**
- `GNOTI`: chỉ nhận `payment_type == "CREDIT"`
- `CPM`: chỉ nhận `error_code == "SUCCESS"`
- `TDH`, `EVT`: nhận tất cả (conditions rỗng)

**Condition tree:**
```
CONDITION: IS_FIRST_ARRIVAL
  ├── order_by: processing_time
  ├── key_fields: [msisdn, product]
  └── ttl: 30d (reset hàng tháng)
```

---

### 4.2. Rule A2 — Đứt gãy sản phẩm Topup

| Thuộc tính | Giá trị |
|---|---|
| **rule_id** | `rule_A2_dut_gay_topup` |
| **Loại** | Stateful — `SEQUENCE NOT_FOLLOWED_BY` (Case 1 trong spec) |
| **Trigger** | 1 nguồn: EVT |
| **Mục tiêu** | Phát hiện KH bấm Continue (Event A) nhưng không hoàn tất GD (Event B) trong 120s |

**Trigger conditions:**
- `object_name IN ["Telecom_topup_view_info_user_button_continue", "Telecom_topup_view_transactionresult_app_view_info"]`

**Condition tree:**
```
SEQUENCE: NOT_FOLLOWED_BY
  ├── min_time: 0, max_time: 120, time_unit: second
  ├── join_keys: first.device_session_id == second.device_session_id
  ├── first:  EVT, object_name == "..._button_continue"
  └── second: EVT, object_name == "..._view_info"
```

---

### 4.3. Rule B1 — Hoàn tiền trial + gia hạn lần đầu

| Thuộc tính | Giá trị |
|---|---|
| **rule_id** | `rule_B1_hoan_tien_trial` |
| **Loại** | Hybrid: Stateless + Stateful + IN_DATASET |
| **Trigger** | 1 nguồn: PMT |
| **Mục tiêu** | Bắt event gia hạn sub thành công, kiểm tra KH đã đăng ký trial 0đ, và chỉ cho gia hạn lần đầu |

**Trigger conditions:**
- `channelId == "CPSUB"` AND `status == "THANH_CONG"`

**Condition tree:**
```
AND
  ├── CONDITION: serviceCode == subCode (KH đang dùng khuyến mại)
  ├── CONDITION: IN_DATASET(msisdn, subCode) ∈ dataset_trial_0d_registered
  └── CONDITION: IS_FIRST_ARRIVAL(msisdn, sub_code), ttl=60d
```

---

### 4.4. Rule B2 — Kích thích mua gói ưu đãi VTM

| Thuộc tính | Giá trị |
|---|---|
| **rule_id** | `rule_B2_kich_thich_mua_goi` |
| **Loại** | Stateless + IN_DATASET |
| **Trigger** | 1 nguồn: SUB_MNGT |
| **Mục tiêu** | Bắt event mua/gia hạn gói sub, lọc theo gói VTM1-4, join keymap giải mã msisdn |

**Trigger conditions:**
- `application_code == "sub-mngt"` AND `transaction_id == "/sub/.../update"` AND `error_code == "00"`

**Condition tree:**
```
AND
  ├── CONDITION: sub_code IN ["VTM1","VTM2","VTM3","VTM4"]
  ├── OR
  │   ├── CONDITION: is_renew == "false" (mua mới)
  │   └── CONDITION: is_renew == "true"  (gia hạn)
  └── CONDITION: IN_DATASET(account) ∈ dataset_msisdn_keymap
```

---

### 4.5. Rule B3 — Tặng gói cước data (đơn giản nhất)

| Thuộc tính | Giá trị |
|---|---|
| **rule_id** | `rule_B3_tang_goi_cuoc_data` |
| **Loại** | Stateless — kafka-to-kafka thuần tuý |
| **Trigger** | 1 nguồn: ADS |
| **Mục tiêu** | Lọc KH nhận quà thành công theo billCode, map processCode, đẩy sang topic đích |

**Trigger conditions:**
- `billCode IN ["KS100MB", "KS500MB"]`

**Condition tree:**
```
CONDITION: billCode != "" (pass-through, logic chính nằm ở trigger)
```

> **Ghi chú:** Đây là rule đơn giản nhất — không có state, không lookup batch, chỉ filter + transform.

---

### 4.6. Rule B4 — CTKM ePass hoàn tiền 50%

| Thuộc tính | Giá trị |
|---|---|
| **rule_id** | `rule_B4_ctkm_epass` |
| **Loại** | Stateful + IN_DATASET (loại trừ blacklist) |
| **Trigger** | 1 nguồn: TMS |
| **Mục tiêu** | Bắt GD ePass thành công qua MM, loại blacklist QTRR + simfarm, mỗi KH chỉ 1 lần |

**Trigger conditions:**
- `viettel_bank_code == "MM"` AND `errorCode == "00"`

**Condition tree:**
```
AND
  ├── CONDITION: transAmount > 0
  ├── CONDITION: IN_DATASET(msisdn) ∈ dataset_blacklist_qtrr (loại trừ)
  ├── CONDITION: IN_DATASET(msisdn) ∈ dataset_simfarm_3_tram (loại trừ)
  └── CONDITION: IS_FIRST_ARRIVAL(msisdn), ttl=30d (mỗi KH 1 lần)
```

> **Ghi chú:** Các IN_DATASET cho blacklist/simfarm ở đây có ngữ nghĩa **loại trừ** — engine cần xử lý ngược lại (NOT match = pass). Tuỳ vào thiết kế engine, có thể cần bổ sung field `negate: true` hoặc dùng operator riêng.

---

### 4.7. Rule B5 — Dashboard hỗ trợ KH vị thế

| Thuộc tính | Giá trị |
|---|---|
| **rule_id** | `rule_B5_dashboard_kh_vi_the` |
| **Loại** | Multi-source + IN_DATASET |
| **Trigger** | 4 nguồn: CDCN, TOPUP, COREPAY, SAVING |
| **Mục tiêu** | Bắt GD lỗi của KH VIP trên nhiều nghiệp vụ, lọc mã lỗi loại trừ, đẩy lên dashboard |

**Trigger conditions (mỗi source khác nhau):**
- `CDCN`: `end_point IN ["/auth/v1/authn/login", "/auth/v2/authn/login"]` AND `imei IS_NOT_NULL` AND `error_code != "00"`
- `TOPUP`: `service_code == "TOPUPLKNT"` AND `status != "INIT"`
- `COREPAY`: `error_code != "00"`
- `SAVING`: `error_code != "00"`

**Condition tree:**
```
AND
  ├── CONDITION: IN_DATASET(msisdn) ∈ dataset_tap_kh_vi_the_v2 (chỉ KH VIP)
  └── CONDITION: IN_DATASET(error_code) ∈ dataset_ma_loi_loai_khoi_dash (loại mã lỗi nhiễu)
```

> **Ghi chú:** B5 là bài phức tạp nhất — 12 nhóm nghiệp vụ, mỗi nhóm có bộ mã riêng. Rule mẫu này cover 4 nguồn kafka chính cần bổ sung (STT 1, 2, 8 trong PYC). Các nguồn đã có sẵn trong `trans_daily_his` (STT 3, 4, 6, 7, 9a, 10, 11) có thể dùng chung source `TDH`.

---

## 5. Tổng hợp operators sử dụng trong 7 rules

| Operator | Loại | Sử dụng ở rule |
|---|---|---|
| `==`, `!=`, `>` | Stateless comparison | Tất cả |
| `IN` | Stateless set lookup | A2, B2, B3, B5 |
| `IS_NOT_NULL` | Stateless null check | B5 |
| `IN_DATASET` | Stateless batch lookup (RocksDB) | B1, B2, B4, B5 |
| `IS_FIRST_ARRIVAL` | **Stateful** — dedup/first arrival | A1, B1, B4 |
| `SEQUENCE NOT_FOLLOWED_BY` | **Stateful** — CEP pattern | A2 |

---

## 6. Các file đã tạo / sửa

### 6.1. `data-simulator/pom.xml` (sửa)

Thêm 2 dependencies:
- `com.fasterxml.jackson.core:jackson-databind:2.17.2` — serialize/deserialize JSON
- `org.postgresql:postgresql:42.7.3` — JDBC driver kết nối PostgreSQL

---

### 6.2. `RuleGenerator.java` — Logic sinh rule ngẫu nhiên

**Đường dẫn:** `data-simulator/src/main/java/com/vdf/streaming/rule/RuleGenerator.java`

Class chứa toàn bộ logic sinh rule ngẫu nhiên. Không có `main()`, được gọi bởi `RuleSimulatorMain`.

**Tham số khởi tạo:**
- `sources` — danh sách source
- `versions` — danh sách version
- `maxTreeHeight` — chiều cao tối đa cây condition_tree
- `maxSourcesPerTrigger` — số source tối đa trong trigger

**Cấu trúc rule được sinh:**

```text
rule
├── rule_id          : "rule_GEN_{index}"
├── rule_name        : random từ pool + "_{index}"
├── rule_version     : random "1"-"9"
├── metadata
│   ├── event_time   : ISO-8601
│   └── user_id      : random từ pool
├── trigger_criteria : mảng 1→N source entries (DNF conditions)
└── condition_tree   : cây đệ quy AND/OR → CONDITION lá
```

**Các operators được sinh:** `==`, `!=`, `>`, `<`, `>=`, `<=`, `BETWEEN`, `IN`, `NOT IN`, `STARTS_WITH`, `ENDS_WITH`, `CONTAINS`, `IS_NULL`, `IS_NOT_NULL`, `LENGTH`

**Lưu ý:** Generator chỉ sinh stateless conditions. 2 case đặc biệt (SEQUENCE, IS_FIRST_ARRIVAL) và IN_DATASET được thiết kế thủ công trong 7 rule mẫu.

---

### 6.3. `RuleSimulatorMain.java` — Main sinh rule

**Đường dẫn:** `data-simulator/src/main/java/com/vdf/streaming/rule/RuleSimulatorMain.java`

Tham số cấu hình trực tiếp:

```java
NUM_RULES = 100
MAX_TREE_HEIGHT = 3
SOURCES = ["TDH","EVT","GNOTI","CPM","PMT","SUB_MNGT","ADS","TMS","CDCN","TOPUP","COREPAY","SAVING"]
VERSIONS = ["v1","v2","v3"]
MAX_SOURCES_PER_TRIGGER = 3
OUTPUT_BASE_DIR = "local/data/rules"
```

**Luồng:** Gen N rules → ghi 1 file JSON array → tên `yyyyMMdd_HHmmss.json`

---

### 6.4. `RulePostgresWriter.java` — Main ghi PostgreSQL

**Đường dẫn:** `data-simulator/src/main/java/com/vdf/streaming/rule/RulePostgresWriter.java`

Tham số:

```java
JSON_FILE = "local/data/rules/100/20260922_145700.json"
JDBC_URL = "jdbc:postgresql://localhost:5433/realtime_core"
BATCH_SIZE = 500
```

**Luồng:** Đọc 1 file JSON array → ghi batch 500/lần → ON CONFLICT upsert

---

### 6.5. `sample_7_rules.json` — 7 rule mẫu nghiệp vụ

**Đường dẫn:** `local/data/rules/sample_7_rules.json`

7 rules thiết kế thủ công, ánh xạ 1:1 từ 7 bài toán A1, A2, B1–B5. Sử dụng đầy đủ các loại operators: stateless, stateful (IS_FIRST_ARRIVAL, SEQUENCE), và IN_DATASET.

---

## 7. Cách chạy

### Gen rules ngẫu nhiên
Chạy class `com.vdf.streaming.rule.RuleSimulatorMain`

### Ghi vào PostgreSQL
1. Set `JSON_FILE` trỏ đến file JSON (có thể dùng `sample_7_rules.json` hoặc file vừa gen)
2. Đảm bảo PostgreSQL đang chạy (`docker compose up postgres`)
3. Chạy class `com.vdf.streaming.rule.RulePostgresWriter`
