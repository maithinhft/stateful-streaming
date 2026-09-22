# Quy định về kiểu dữ liệu, toán tử, các phép tổng hợp 

## 1. Danh sách kiểu dữ liệu và toán tử hỗ trợ
* **`INT` / `LONG`**: `==`, `!=`, `>`, `<`, `>=`, `<=`, `BETWEEN`, `IN`, `NOT IN`, `+`, `-`, `*`, `/`, `%`
* **`FLOAT` / `DOUBLE`**: `==`, `!=`, `>`, `<`, `>=`, `<=`, `BETWEEN`, `+`, `-`, `*`, `/`
* **`STRING`**: `==`, `!=`, `IN`, `NOT IN`, `STARTS_WITH`, `NOT_STARTS_WITH`, `ENDS_WITH`, `NOT_ENDS_WITH`, `CONTAINS`, `NOT_CONTAINS`, `IS_NULL`, `IS_NOT_NULL`, `LENGTH`
    * **Lưu ý thiết kế (Không hỗ trợ `LIKE`, `RLIKE`, `REGEX`):**
        * *Tránh ReDoS (NFA):* Các engine Regex mặc định (như Java `Pattern`) dùng NFA Backtracking có độ phức tạp Worst-Case lên tới $O(2^N)$ ([tham khảo phân tích NFA Complexity](https://swtch.com/~rsc/regexp/regexp1.html)), dễ gây treo CPU.
        * *Hạn chế của DFA:* Dù dùng engine DFA (Google RE2, Hyperscan) đạt $O(N)$ với $N$ là độ dài chuỗi, CPU vẫn tốn chi phí do liên tục tra cứu bảng chuyển trạng thái (State Transition Table Lookup), gây Pointer Chasing và trượt L1/L2 Cache Misses liên tục trên từng ký tự.
        * *So sánh `==` (`String.equals`):* Đạt độ phức tạp $O(L)$ với $L = \min(\text{length}_1, \text{length}_2)$ (dừng sớm $\Omega(1)$ nếu khác độ dài/hash). Do cấu trúc chuỗi Java dùng mảng `byte[]`, `String.equals` gọi gián tiếp tới `Arrays.equals` của JVM ([tham khảo JVM Array Optimization](https://stackoverflow.com/questions/41153992/why-is-arrays-equalschar-char-8-times-faster-than-all-the-other-versions)), tận dụng SIMD của CPU để so sánh song song với truy xuất bộ nhớ tuần tự.
        * *Giải pháp thay thế an toàn:* Thiết kế chỉ mở các toán tử thao tác chuỗi thuần túy (Exact Substring / Prefix / Suffix Primitives). Các toán tử như `STARTS_WITH`, `ENDS_WITH` hay `CONTAINS` đều gọi trực tiếp hàm nội tại của String JVM, tận dụng SIMD Instructions (AVX2/SSE4.2) để quét mảng byte song song trên CPU, loại bỏ hoàn toàn rủi ro ReDoS.
        * *Độ phức tạp nhân theo Event và Pattern (Workload Amplification):* Với mỗi event đến, hệ thống phải thực hiện kiểm tra liên tục trên từng pattern/điều kiện (ở bước `trigger_criteria` và trong `condition_tree`). Tổng chi phí tính toán bị nhân lên theo công thức: $\text{Số lượng Event} \times \text{Số lượng Pattern cần kiểm tra}$. Nếu dùng Regex/Like, việc nhân chi phí đắt đỏ này trên luồng streaming quy mô lớn sẽ gây quá tải hệ thống.
* **`BOOLEAN`**: `==`, `!=`
* **`TIMESTAMP`**: `==`, `!=`, `>`, `<`, `>=`, `<=`, `BETWEEN`
* **`OBJECT`**: Cấu trúc lồng nhau (Nested Objects), truy xuất dữ liệu đa cấp qua cú pháp Dot Notation (ví dụ: `risk_signals.fraud_probability_score`).
* **`ANY / TUPLE`**: `IN_DATASET` (Toán tử đặc thù dùng để đối soát 1 hoặc cụm nhiều trường đồng thời với tập dữ liệu lớn lưu ở Broadcast State).

---

## 2. Cấu trúc Rule và JSON mẫu

* **Bộ lọc kích hoạt (`trigger_criteria`):**
    * Cấu trúc dạng mảng: `[{ "source": "...", "version": "...", "key_field": "...", "event_time_field": "...", "conditions": [ [cond1, cond2], [cond3] ] }]`.
    * **Đa nguồn (Multi-source):** Khai báo `source` và `version` riêng giúp định tuyến và lọc dữ liệu nhanh theo từng loại event. Cả 2 nguồn dùng chung `schema_version` được cấu hình trong `application.properties`.
    * **Khóa gom nhóm (`key_field`):** Khai báo trường dữ liệu dùng để `keyBy` (cùng đại diện cho số điện thoại nhưng mỗi schema có thể có tên trường khác nhau, ví dụ `msisdn`, `phone`). **Yêu cầu đầu vào:** Giá trị của trường số điện thoại này bắt buộc phải theo định dạng `(+84)...`.
    * **Trường thời gian sự kiện (`event_time_field`):** Khai báo tên trường hiển thị cho event time.
    * **Tra cứu danh sách lớn:** Hỗ trợ kiểm tra giá trị qua `IN_DATASET` chung cho cả 1 trường hoặc nhiều trường kết hợp (thay cho `IN` thông thường khi số lượng phần tử quá lớn hoặc giá trị các trường cần đi theo bộ). Engine sẽ dựa vào RocksDB để tra cứu hiệu quả.
    * **Cấu trúc `conditions` (Mảng 2 chiều — DNF):** Là danh sách các list điều kiện con. Chỉ cần thỏa mãn **toàn bộ phần tử trong 1 list điều kiện con** là pass trigger (outer list: OR; inner list: AND).
    * **Kiến trúc Inverted Index:** Để đảm bảo tốc độ lọc event ($O(1)$), tầng `trigger_criteria` áp dụng mô hình Dual-Index (chỉ lập chỉ mục các phép toán có độ chọn lọc cao như `==`, `IN` và `IN_DATASET`), giúp loại bỏ sớm $95\%$ lượng rule không khớp trước khi đi vào `condition_tree`. Chi tiết thiết kế luồng xử lý xem tại 👉 [**Kiến trúc Inverted Index cho Trigger**](06_RULE_INVERTED_INDEX.md).

### 2.1. Cấu trúc JSON Rule hoàn chỉnh:
```text
{
  "rule_id": "rule_B_54",
  "rule_name": "example_rule_name",
  "rule_version": "1",
  "metadata": {
    "event_time": "2026-08-24T16:02:37.123+07:00",
    "user_id" : "user_011"
  },
  "trigger_criteria": [
    {
      "source": "B",
      "schema_version": "v2",
      "key_field": "msisdn",
      "event_time_field": "timestamp",
      "conditions": [
        [
          {
            "fields": [
              "process_code",
              "service_code"
            ],
            "op": "IN_DATASET",
            "dataset_id": "dataset_nps_high_risk"
          },
          {
            "field": "device_type",
            "op": "==",
            "value": "TABLET"
          }
        ],
        [
          {
            "field": "is_vip",
            "op": "==",
            "value": true
          }
        ]
      ]
    },
    {
      "source": "A",
      "version": "v2",
      "key_field": "phone_number",
      "event_time_field": "event_time",
      "conditions": [
        [
          {
            "field": "user_status",
            "op": "==",
            "value": "ACTIVE"
          }
        ]
      ]
    }
  ],
  "condition_tree": {                               // Logic chính của rule 
    "type": "OR",
    "children": [
      {
        "type": "CONDITION",
        "expression": {
          "field": "B.v2.risk_signals.fraud_probability_score",
          "op": "<=",
          "value": 40.13
        }
      },
      {
        "type": "CONDITION",
        "expression": {
          "field": "A.v2.daily_spend_total_vnd",
          "op": ">",
          "value": 10000000.0
        }
      },
      {
        "type": "CONDITION",
        "expression": {
          "field": "A.v2.age",
          "op": ">=",
          "value": 25
        }
      }
    ]
  }
}
```
### 2.2. Tra cứu danh sách lớn qua Dataset (IN_DATASET)

**Lý do thiết kế toán tử `IN_DATASET`:** \
Trong thực tế vận hành, hệ thống phát sinh hai rào cản lớn đối với toán tử `IN` thông thường:
1. Khi 1 trường cần kiểm tra `IN` với một danh sách quá nhiều giá trị (hàng ngàn hoặc chục ngàn phần tử).
2. Khi cần đối soát các bộ trường (composite tuple) với nhiều bộ giá trị khác nhau (ví dụ: đối soát tập hợp `[process_code, service_code]` với list các bộ giá trị).

Nếu lưu mảng giá trị trực tiếp vào Rule JSON để lưu trên RAM, bộ nhớ của engine sẽ bị phình to và hiệu năng giảm sút. Do đó, hệ thống bổ sung cơ chế lưu trữ và tra cứu riêng biệt (thông qua Broadcast State và RocksDB), cùng toán tử `IN_DATASET`.

Với trường hợp có 1 trường riêng lẻ (không đi theo bộ), core coi như 1 bộ nhưng có 1 phần tử (trường `fields` là list nhưng có 1 phần tử).

#### Định dạng dữ liệu Dataset (Gửi qua Kafka)
Đầu vào của bộ dataset sẽ được đẩy qua luồng Kafka dưới định dạng JSON:
```json
{
  "dataset_id": "dataset_telecom_high_risk",
  "dataset_version": "v1",
  "fields": ["device_type", "channel", "mcc_code"],
  "values": [
    ["TABLET", "MOBILE_APP", 5411],
    ["DESKTOP", "WEB", 5812]
  ]
}
```

> **Lưu ý:** Nếu danh sách dưới 50 phần tử, người dùng nên dùng `op`: `"IN"` với mảng `value`: `[...]` như cũ để tra cứu trực tiếp trên RAM. Khi vượt ngưỡng, chuyển sang `IN_DATASET` để trỏ vào Broadcast State.

#### Cấu hình Rule sử dụng IN_DATASET
Ví dụ minh họa việc kết hợp đồng thời tra cứu 1 trường danh sách đen (`beneficiary_account`) và 1 tập mẫu rủi ro phức hợp đa trường (`mcc_code`, `transaction_channel`, `device_province_code`).

```json
{
  "rule_id": "rule_fraud_combined_check",
  "rule_name": "block_suspicious_accounts_and_channels",
  "rule_version": "1",
  "trigger_criteria": [
    {
      "source": "TRANS_STREAM",
      "schema_version": "v2",
      "key_field": "msisdn",
      "event_time_field": "timestamp",
      "conditions": [
        [
          {
            "fields": [
              "beneficiary_account"
            ],
            "op": "IN_DATASET",
            "dataset_id": "dataset_blacklist_accounts_202609",
            "dataset_version": "v1"
          },
          {
            // Khai báo thứ tự các trường để tạo Composite Key
            "fields": [
              "mcc_code",
              "transaction_channel",
              "device_province_code"
            ],
            "op": "IN_DATASET",
            "dataset_id": "dataset_high_risk_tuples_q3"
          }
        ]
      ]
    }
  ]
}
```

> **Chi tiết thiết kế** cơ chế lưu trữ Dataset trên RocksDB, kỹ thuật băm `xxHash64`, thiết kế Key 16 Bytes (nhằm tối ưu hóa RAM và L1/L2 Cache), đánh giá tỷ lệ đụng độ:
> 👉 [**Chi tiết cơ chế lưu trữ IN_DATASET**](./details/IN_DATASET_STORAGE_SPEC.md)

## 3. Tổng quát hóa các Node (Type) trong Condition Tree

Cấu trúc của `condition_tree` là thành phần lõi chứa logic chính của hệ thống. Các node trong cây được chia làm 3 nhóm logic cốt lõi dựa trên thuộc tính `type`:

| Nhóm Logic | Type | Mục đích & Hoạt động | Cấu trúc (Thành phần bên trong) |
| :--- | :--- | :--- | :--- |
| **Routing & Grouping** | `AND`, `OR`<br>(`LOGICAL`) | Dùng để nhóm và kết hợp nhiều điều kiện con lại với nhau. Các node này đóng vai trò làm Node nhánh (Branch Node) trong cây điều kiện. Chúng không tự tính toán ra giá trị, mà chỉ gộp kết quả từ các node con lại với nhau để tạo ra các rule phức tạp ($A \text{ and } (B \text{ or } C)$). | - **`children`** (Array): Danh sách các Node con. Các Node con này có thể tiếp tục là `LOGICAL`, `CONDITION` hoặc `SEQUENCE`. |
| **Evaluator** | `CONDITION` | Thực hiện so sánh hoặc kiểm tra dữ liệu thực tế. Đóng vai trò là Node lá (Leaf Node) tính toán ra `true`/`false`. Có 2 biến thể:<br><br>- **Stateless**: Thẩm định event hiện tại (nhẹ, độ phức tạp $O(1)$).<br>- **Stateful**: Duy trì trạng thái của các event trước đó (ví dụ `IS_FIRST_ARRIVAL`). | - **`expression`** (Object): Cấu trúc định nghĩa phép toán. Phụ thuộc vào `op`:<br>&nbsp;&nbsp;&nbsp;&nbsp;+ **Stateless** (vd: `==`, `>`, `IN`): Cần `field`/`expr`, `op`, và `value`/`right_field`.<br>&nbsp;&nbsp;&nbsp;&nbsp;+ **Stateful** (vd: `IS_FIRST_ARRIVAL`): Cần `op` đi kèm `order_by`, `key_fields`, `ttl`. |
| **Pattern Matcher** | `SEQUENCE` | **(Stateful)** Sử dụng cho Complex Event Processing (CEP), nhận diện pattern chuỗi sự kiện. Hoạt động như một Timer mở ra khi gặp sự kiện mở đầu và thay đổi trạng thái khi thời gian trôi qua hoặc khi bắt gặp sự kiện kết thúc. | - **`pattern`** (String): `FOLLOWED_BY` (có B theo sau A), `NOT_FOLLOWED_BY` (không có B theo sau A).<br>- **`min_time`** (Number): Thời gian chờ tối thiểu.<br>- **`max_time`** (Number): Thời gian chờ tối đa.<br>- **`time_unit`** (String): Đơn vị thời gian (`second`, `minute`, `hour`). Không hỗ trợ `day` trở lên.<br>- **`join_keys`** (Array): Khóa liên kết định danh ngữ cảnh (vd `device_session_id`).<br>- **`first`** (Object): Bộ lọc sự kiện bắt đầu.<br>- **`second`** (Object): Bộ lọc sự kiện thứ hai. |

> **Lưu ý:** Engine sẽ từ các thuộc tính và phép toán (op) stateful để xét xem nên đưa những gì trong event stream vào state.

## 4. Quy tắc toán tử:
* **Vế trái:** Luôn là một trường dữ liệu `Field` (hoặc mảng `fields` đối với `IN_DATASET`), hoặc biểu thức tuyến tính `Expr` (chỉ áp dụng đối với `INT`, `LONG`, `FLOAT` hoặc `DOUBLE`).
* **Vế phải:** Có thể là giá trị cụ thể (Literal), danh sách giá trị (`List`), hoặc trường dữ liệu khác (`Field`), biểu thức tuyến tính `Expr` (với `INT`, `LONG`, `FLOAT` hoặc `DOUBLE`). Riêng với toán tử `IN_DATASET`, vế phải là các thuộc tính tham chiếu `dataset_id` và `dataset_version`.
* **Quy định bất biến cho `IN`, `NOT IN`, `BETWEEN`:** Vế phải bắt buộc là danh sách/mảng các **giá trị cụ thể (Literal values)**, không chứa `Field` bên trong để tránh phức tạp hóa cây điều kiện.

---

### Bảng chi tiết toán tử, toán hạng và ví dụ mẫu

> **Chú thích bảng:**
> * `Expr`: Biểu thức số học tuyến tính kết hợp giữa các trường và hằng số (ví dụ: `pending * 0.7 + spend * 0.3`).
> * `Field`: Tên một trường trong bản tin dữ liệu.
> * Trong ngoặc đơn `(...)`: Kiểu dữ liệu tương ứng của trường đó.




| Data Type            | Operators                        | Vế trái                         | Vế phải                                   | Ghi chú ngữ nghĩa                                              | VD: Vế phải Literal Value                                                                                                                                    | VD: Vế phải Cross-Field                                                                                                        | VD: Chứa Expr                                                                                                                            |
|:---------------------|:---------------------------------|:--------------------------------|:------------------------------------------|:---------------------------------------------------------------|:-------------------------------------------------------------------------------------------------------------------------------------------------------------|:-------------------------------------------------------------------------------------------------------------------------------|:-----------------------------------------------------------------------------------------------------------------------------------------|
| **`INT / LONG`**     | `==`, `!=`, `>`, `<`, `>=`, `<=` | `Field (Int/Long)` / `Expr`     | `Field (Number)` / `Number` / `Expr`      | So sánh đơn trị                                                | <pre>{<br>  "field": "age",<br>  "op": ">=",<br>  "value": 18<br>}</pre>                                                                                     | <pre>{<br>  "field": "login_attempts_count",<br>  "op": ">",<br>  "right_field": "app_session_count_today"<br>}</pre>          | <pre>{<br>  "expr": "pages_viewed - products_viewed",<br>  "op": ">=",<br>  "value": 5<br>}</pre>                                        |
|                      | `BETWEEN`                        | `Field (Int/Long)` / `Expr`     | `[min, max]` (`Number[2]`)                | Bao gồm đầu mút (`>= min && <= max`)                           | <pre>{<br>  "field": "login_attempts_count",<br>  "op": "BETWEEN",<br>  "value": [1.5, 8.5]<br>}</pre>                                                       |                                                                                                                                | <pre>{<br>  "expr": "age + tenure_months / 12",<br>  "op": "BETWEEN",<br>  "value": [20, 60]<br>}</pre>                                  |
|                      | `IN`, `NOT IN`                   | `Field (Int/Long)` / `Expr`     | `List<Int/Long>`                          | Tra cứu tập hợp ($O(1)$ Hash Set)                              | <pre>{<br>  "field": "mcc_code",<br>  "op": "IN",<br>  "value": [5411, 5812, 5999]<br>}</pre>                                                                |                                                                                                                                | <pre>{<br>  "expr": "number_of_products_held % 10",<br>  "op": "IN",<br>  "value": [1, 3, 5]<br>}</pre>                                  |
|                      | `+`, `-`, `*`, `/`, `%`          | `Field (Int/Long)` / `Expr`     | `Field (Number)` / `Number` / `Expr`      | Biểu thức số học trả về `Int/Long/Float/Double`                | <pre>{<br>  "expr": "number_of_products_held % 2",<br>  "op": "==",<br>  "value": 0<br>}</pre>                                                               | <pre>{<br>  "expr": "pages_viewed - products_viewed",<br>  "op": "<",<br>  "right_field": "app_session_count_today"<br>}</pre> | <pre>{<br>  "expr": "total_loan_count + total_card_count",<br>  "op": ">",<br>  "right_expr": "number_of_products_held * 2"<br>}</pre>   |
| **`FLOAT / DOUBLE`** | `==`, `!=`, `>`, `<`, `>=`, `<=` | `Field (Float/Double)` / `Expr` | `Field (Number)` / `Number` / `Expr`      | So sánh ngưỡng số thực                                         | <pre>{<br>  "field": "fraud_probability_score",<br>  "op": ">",<br>  "value": 70.0<br>}</pre>                                                                | <pre>{<br>  "field": "last_transaction_amount_vnd",<br>  "op": ">",<br>  "right_field": "monthly_income_vnd"<br>}</pre>        | <pre>{<br>  "expr": "(pending * 0.7 + spend * 0.3)",<br>  "op": ">=",<br>  "value": 1000000.0<br>}</pre>                                 |
|                      | `BETWEEN`                        | `Field (Float/Double)` / `Expr` | `[min, max]` (`Number[2]`)                | Bao gồm đầu mút (`>= min && <= max`)                           | <pre>{<br>  "field": "response_latency_ms",<br>  "op": "BETWEEN",<br>  "value": [100.0, 500.0]<br>}</pre>                                                    |                                                                                                                                | <pre>{<br>  "expr": "(latency_ms + idle_time * 1000) / 2",<br>  "op": "BETWEEN",<br>  "value": [500.0, 3000.0]<br>}</pre>                |
|                      | `+`, `-`, `*`, `/`               | `Field (Float/Double)` / `Expr` | `Field (Number)` / `Number` / `Expr`      | Biểu thức số học trả về `Float/Double`                         | <pre>{<br>  "expr": "last_transaction_amount / monthly_income",<br>  "op": ">",<br>  "value": 0.8<br>}</pre>                                                 | <pre>{<br>  "expr": "current_balance - last_txn_amount",<br>  "op": "<",<br>  "right_field": "credit_limit_vnd"<br>}</pre>     | <pre>{<br>  "expr": "pending_amount * 0.7",<br>  "op": ">=",<br>  "right_expr": "daily_spend_total * 0.3"<br>}</pre>                     |
| **`STRING`**         | `==`, `!=`                       | `Field (String)`                | `Field (String)` / `String`               | Khớp chuỗi chính xác (hỗ trợ so sánh chéo giữa các trường)     | <pre>{<br>  "field": "login_channel",<br>  "op": "==",<br>  "value": "MOBILE_APP"<br>}</pre>                                                                 | <pre>{<br>  "field": "home_province",<br>  "op": "!=",<br>  "right_field": "current_province"<br>}</pre>                       |                                                                                                                                          |
|                      | `IN`, `NOT IN`                   | `Field (String)`                | `List<String>`                            | Tra cứu tập chuỗi ($O(1)$ Hash Set)                            | <pre>{<br>  "field": "login_channel",<br>  "op": "IN",<br>  "value": [<br>    "MOBILE_APP",<br>    "WEB"<br>  ]<br>}</pre>                                   |                                                                                                                                |                                                                                                                                          |
|                      | `STARTS_WITH`, `NOT_STARTS_WITH` | `Field (String)`                | `String`                                  | Khớp tiền tố chuỗi (Prefix matching) / Không bắt đầu bằng      | <pre>{<br>  "field": "msg_content",<br>  "op": "STARTS_WITH",<br>  "value": "GD nhan tien"<br>}</pre>                                                            |                                                                                                                                |                                                                                                                                          |
|                      | `CONTAINS`, `NOT_CONTAINS`       | `Field (String)`                | `String`                                  | Kiểm tra chuỗi con (Substring matching) / Không chứa chuỗi con | <pre>{<br>  "field": "msg_content",<br>  "op": "CONTAINS",<br>  "value": "NAPTIENVIETTELPAY"<br>}</pre>                                                         |                                                                                                                                |                                                                                                                                          |
|                      | `ENDS_WITH`, `NOT_ENDS_WITH`     | `Field (String)`                | `String`                                  | Khớp hậu tố chuỗi (Suffix matching) / Không kết thúc bằng      | <pre>{<br>  "field": "end_point",<br>  "op": "ENDS_WITH",<br>  "value": "/auth/v1/authn/login"<br>}</pre>                                                       |                                                                                                                                |                                                                                                                                          |
|                      | `IS_NULL`, `IS_NOT_NULL`         | `Field (String/Any)`            | *(Không cần vế phải)*                     | Kiểm tra rỗng hoặc tồn tại                                     | <pre>{<br>  "field": "imei",<br>  "op": "IS_NOT_NULL"<br>}</pre>                                                                                                 |                                                                                                                                |                                                                                                                                          |
|                      | Thuộc tính `function="LENGTH"`   | `Field (String)`                | `Number`                                  | Đo chiều dài chuỗi (trả về số nguyên để áp dụng các op so sánh `==`, `>`, `<`...) | <pre>{<br>  "field": "imei",<br>  "function": "LENGTH",<br>  "op": "==",<br>  "value": 36<br>}</pre>                                                      |                                                                                                                                |                                                                                                                                          |
| **`BOOLEAN`**        | `==`, `!=`                       | `Field (Boolean)`               | `Field (Boolean)` / `Boolean`             | Kiểm tra hoặc đối chiếu cờ trạng thái                          | <pre>{<br>  "field": "is_suspicious_ip",<br>  "op": "==",<br>  "value": true<br>}</pre>                                                                      | <pre>{<br>  "field": "is_2fa_enabled",<br>  "op": "==",<br>  "right_field": "is_biometric_enabled"<br>}</pre>                  |                                                                                                                                          |
| **`TIMESTAMP`**      | `==`, `!=`, `>`, `<`, `>=`, `<=` | `Field (Timestamp)`             | `Field (Timestamp)` / `String (ISO-8601)` | So sánh mốc thời gian (hỗ trợ so sánh giữa 2 trường thời gian) | <pre>{<br>  "field": "account_created_date",<br>  "op": ">",<br>  "value": "2026-01-01T00:00:00Z"<br>}</pre>                                                 | <pre>{<br>  "field": "event_time",<br>  "op": ">",<br>  "right_field": "last_login_time"<br>}</pre>                            |                                                                                                                                          |
|                      | `BETWEEN`                        | `Field (Timestamp)`             | `[from, to]` (`Timestamp[2]`)             | Khoảng thời gian (`>= from && <= to`)                          | <pre>{<br>  "field": "last_login_time",<br>  "op": "BETWEEN",<br>  "value": [<br>    "2026-08-01T00:00:00Z",<br>    "2026-08-24T23:59:59Z"<br>  ]<br>}</pre> |                                                                                                                                |                                                                                                                                          |
| **`ANY / TUPLE`**    | `IN_DATASET`                     | `List<Field>` (Mảng `fields`)   | `dataset_id`                              | Tra cứu tập dữ liệu lớn qua Broadcast State (RocksDB)          | <pre>{<br>  "fields": ["mcc_code"],<br>  "op": "IN_DATASET",<br>  "dataset_id": "ds_1"<br>}</pre> |                                                                                                                                |                                                                                                                                          |

## 5. Các mẫu Rule đặc biệt (Cases)

Dưới đây là 2 case đặc biệt cho 2 bài toán A1 và A2:
* **Case 1 (Luồng đứt gãy - Absence CEP):** Event $A$ xuất hiện, nhưng trong vòng $X$ giây tiếp theo không xuất hiện Event $B$ (có cùng `device_session_id`).
* **Case 2 (Lọc bản ghi đến sớm nhất qua 4 source - Deduplication / First Arrival):** Có 4 event sources đổ về, gom nhóm theo composite key (`id`, `trans_type`), chỉ cho phép duy nhất event đầu tiên xuất hiện (dựa vào `processing_time`) đi tiếp; các event đến sau có cùng key sẽ bị loại bỏ.

Dưới đây là cấu hình JSON chi tiết cho từng case.

### Case 1: Phát hiện đứt gãy luồng giao dịch (Event A đợi Event B)
Cấu trúc sử dụng node kiểu `SEQUENCE` với pattern `NOT_FOLLOWED_BY`.

```json
{
  "rule_id": "rule_dropoff_telecom_topup_120s",
  "rule_name": "product_journey",
  "rule_version": "1",
  
  // 1. BỘ LỌC KÍCH HOẠT (TRIGGER CRITERIA)
  // Chỉ tải rule này vào RAM khi stream nhận đúng các event liên quan, tránh kiểm tra vô tội vạ.
  "trigger_criteria": [
    {
      "source": "EVT",
      "version": "v1",
      "key_field": "msisdn",
      "event_time_field": "event_time",
      "conditions": [
        [
          {
            // Kiểm tra event hiện tại có phải là bước bấm Continue (A) hoặc bước Thành công (B)
            "field": "object_name",
            "op": "IN",
            "value": [
              "Telecom_topup_view_info_user_button_continue",
              "Telecom_topup_view_transactionresult_app_view_info"
            ]
          }
        ]
      ]
    }
  ],

  // 2. CÂY ĐIỀU KIỆN CHÍNH (CONDITION TREE)
  "condition_tree": {
    // "SEQUENCE": Báo hiệu cho engine chuyển sang chế độ CEP (xử lý chuỗi thời gian)
    "type": "SEQUENCE",
    
    // "NOT_FOLLOWED_BY": Pass rule khi sự kiện thứ nhất (first) xuất hiện 
    // NHƯNG sự kiện thứ hai (second) KHÔNG xuất hiện trong khung thời gian quy định
    "pattern": "NOT_FOLLOWED_BY",
    
    // Khoảng thời gian chờ (tối thiểu và tối đa) cùng đơn vị
    "min_time": 0,
    "max_time": 120,
    "time_unit": "second",
    
    // JOIN KEYS: Khóa định danh ngữ cảnh.
    // Bắt buộc event B phải có cùng `device_session_id` với event A.
    // Nếu B là của thiết bị khác, engine sẽ bỏ qua, không tính là hoàn tất cho A này.
    "join_keys": [
      {
        "left_field": "first.device_session_id",
        "right_field": "second.device_session_id"
      }
    ],

    // ĐỊNH NGHĨA SỰ KIỆN THỨ NHẤT (Event A)
    "first": {
      "source": "EVT",
      "filter": [
        {
          "field": "object_name",
          "op": "==",
          "value": "Telecom_topup_view_info_user_button_continue"
        }
      ]
    },

    // ĐỊNH NGHĨA SỰ KIỆN THỨ HAI (Event B)
    "second": {
      "source": "EVT",
      "filter": [
        {
          "field": "object_name",
          "op": "==",
          "value": "Telecom_topup_view_transactionresult_app_view_info"
        }
      ]
    }
  }
}
```

**Cơ chế chạy ngầm:**
* Khi Event A đến: Engine ghi nhớ `device_session_id` vào bộ nhớ tạm (State Store) và tạo một Timer đếm lùi 120 giây.
* Nếu Event B đến trước khi hết 120 giây và khớp `device_session_id`: Engine hủy Timer, xóa State của A $\rightarrow$ Điều kiện không thỏa mãn (không đứt gãy).
* Nếu hết 120 giây mà Timer nổ chuông: State của A vẫn còn $\rightarrow$ Điều kiện `NOT_FOLLOWED_BY` được kích hoạt $\rightarrow$ Đánh giá true (xác nhận đứt gãy).

### Case 2: Lọc lấy duy nhất Event đến sớm nhất qua 4 source (First Arrival / Deduplication)
Với yêu cầu: Trong 4 source A, B, C, D, khi các event có cùng `id` và `trans_type` đổ về, chỉ giữ lại duy nhất event đầu tiên tính theo `processing_time`.
Để đưa vào `condition_tree`, logic này được mô hình hóa thành một toán tử trạng thái: `IS_FIRST_ARRIVAL`.

```json
{
  "rule_id": "rule_first_arrival_4_sources",
  "rule_name": "dedup_first_seen",
  "rule_version": "1",

  // 1. BỘ LỌC KÍCH HOẠT (TRIGGER CRITERIA)
  // Lắng nghe cả 4 nguồn dữ liệu A, B, C, D đổ vào hệ thống
  "trigger_criteria": [
    { "source": "A", "version": "v1", "key_field": "id", "event_time_field": "processing_time", "conditions": [] },
    { "source": "B", "version": "v1", "key_field": "id", "event_time_field": "processing_time", "conditions": [] },
    { "source": "C", "version": "v1", "key_field": "id", "event_time_field": "processing_time", "conditions": [] },
    { "source": "D", "version": "v1", "key_field": "id", "event_time_field": "processing_time", "conditions": [] }
  ],

  // 2. CÂY ĐIỀU KIỆN CHÍNH (CONDITION TREE)
  "condition_tree": {
    "type": "CONDITION",
    "expression": {
      // Toán tử chuyên biệt kiểm tra lần đầu xuất hiện
      "op": "IS_FIRST_ARRIVAL",
      
      // Tiêu chí xác định thứ tự: Theo thời điểm engine nhận được bản ghi (Processing Time)
      "order_by": "processing_time",
      
      // COMPOSITE KEY: Khóa gom nhóm trùng lặp.
      // Engine sẽ hash chuỗi: hash(metadata.id + "_" + trans_type) để lưu vào State.
      "key_fields": [
        "metadata.id",
        "trans_type"
      ],
      
      // TIME-TO-LIVE (TTL): Vùng nhớ giữ key.
      // Trong vòng 10 phút, bất kỳ event nào từ cả 4 source trùng cặp (id, trans_type) sẽ bị chặn.
      // Sau 10 phút, State tự giải phóng RAM để tránh tràn bộ nhớ.
      "ttl": "10m"
    }
  }
}
```
