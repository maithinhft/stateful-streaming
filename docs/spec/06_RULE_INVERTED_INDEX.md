# Kiến trúc Rule Inverted Index

## 1. Chia 2 tập chỉ mục (Index Schema)

Tầng trigger được tổ chức thành 2 tập chỉ mục riêng biệt. Giá trị (Value) của các chỉ mục này được lưu trữ dưới dạng một **chuỗi bit (RoaringBitmap)**.
Trong chuỗi bit này:
- **Mỗi vị trí bit (bit index)** tương ứng với định danh (Slot ID) của 1 Rule.
- **Bit = 1**: Có nghĩa là Rule đó thỏa mãn điều kiện của Key (chứa giá trị `value` hoặc `dataset_id` tương ứng, hoặc chứa toán tử phức tạp ở Tập 2).
- **Bit = 0**: Rule không chứa giá trị/điều kiện đó.

### Tập 1: ExactMatchIndex (Chỉ mục định danh chính xác)
- **Toán tử xử lý:** `==`, `IN` (đã làm phẳng/flatten), `IN_DATASET`.
- **Cấu trúc:** `Map<Key, RoaringBitmap>` với Key là `field:value` (hoặc `field:dataset_id`).
- **Đặc tính:** Băm chính xác $O(1)$. Chỉ bật bit cho các rule khớp cụ thể giá trị đó.

### Tập 2: ComplexPredicateIndex (Chỉ mục sự hiện diện của toán tử khó)
- **Toán tử xử lý:** `>`, `<`, `!=`, `CONTAINS`, `ENDS_WITH`, `REGEX`...
- **Cấu trúc:** `RoaringBitmap` (Một chuỗi bit duy nhất cho mỗi Source/Version).
- **Đặc tính:** Do mỗi Inverted Index đã được phân vùng theo `source` và `version` (các event lọt vào đều chung schema), Tập 2 dùng một chuỗi bit gom chung. Bất kỳ rule nào kiểm tra toán tử khó ở bước trigger (ví dụ `amount > 5000000`) đều sẽ được bật bit 1 tại chuỗi bit này.

> **Lưu ý hiệu năng:** Việc sử dụng một chuỗi bit duy nhất cho Tập 2 có ưu điểm là cực kỳ tối giản. Tuy nhiên, nếu tập Rule thực tế chứa **quá nhiều** các toán tử Complex, chuỗi bit này sẽ bị bật quá nhiều bit `1`. Hậu quả là lượng "ứng viên ảo" (False Positives) lọt vào Condition Tree sẽ tăng cao, làm suy giảm hiệu năng lọc của hệ thống.

## 2. Quản lý Index khi cập nhật Rule

Mỗi Source và mỗi Version cấu hình sở hữu một gói chỉ mục độc lập, giảm số lượng Rule không nhận Source của event đến:

```text
Class RuleIndexState:
    exact_index: Map<String, RoaringBitmap>        // Tập 1: field:value -> Bitmap
    complex_index: RoaringBitmap                   // Tập 2: 1 chuỗi bit duy nhất
    slot_to_rule: Array<RuleInstance>              // Slot ID -> Rule Definition
```

Khi CDC đẩy bản cập nhật Rule (xóa, thêm, cập nhật) qua Broadcast State, worker Flink sẽ **tạo một bản Clone (Copy-On-Write)** của `RuleIndexState` hiện tại ở background.
Sau khi áp dụng các thay đổi (Thêm/Sửa/Xóa rule) trên bản nháp này xong, worker thực hiện **Atomic Pointer Swap** sang instance mới, bảo đảm zero-downtime.

## 3. Luồng xử lý khi Event đến (Runtime Pipeline)

Khi một Event đi vào, do Inverted Index đã được chia theo Source/Version, Event được ngầm định có chung cấu trúc schema với gói Index.

```text
Incoming Event (100 trường)
          │
          ├───────────────────────────────────────────┐
          ▼                                           ▼
  [ Tra cứu Tập 1: Exact ]                    [ Tra cứu Tập 2: Complex ]
  Với mỗi field:                              Lấy trực tiếp chuỗi bit
  BM_exact = exact_index.get("field:val")     của cụm Source/Version:
          │                                   BM_complex = complex_index
          ▼                                           │
     Exact_BM_List                                    │
          │                                           │
          └─────────────────────┬─────────────────────┘
                                ▼
         [ SIMD FastOr: Gom toàn bộ chuỗi Bit ]
 Candidates = OR(Exact_BM_List)  |  BM_complex
                                │
                                ▼
         [ Condition Tree: Thẩm định chi tiết ]
  Chạy short-circuit evaluation trên tập Candidates đã lọc
```

## 4. Ví dụ kịch bản cụ thể

Giả sử hệ thống quản lý 5 Rule, tương ứng với 5 Slot (từ Slot 1 đến Slot 5). Ta biểu diễn chuỗi bit minh họa từ trái sang phải: `[Slot1][Slot2][Slot3][Slot4][Slot5]`. Ban đầu các chuỗi bit đều là `00000`.
- **Rule 1 (Slot 1)**: `country == 'VN'` $\rightarrow$ Bật bit 1 ở Tập Exact tại key `country:VN`. Bitmap: `10000`.
- **Rule 2 (Slot 2)**: `amount > 10000000` $\rightarrow$ Bật bit 2 ở Tập Complex. Bitmap Complex: `01000`.
- **Rule 3 (Slot 3)**: `mcc == 5411` AND `user_agent CONTAINS "Chrome"`:
    - Điều kiện `mcc == 5411` $\rightarrow$ Bật bit 3 ở Tập Exact tại key `mcc:5411`. Bitmap: `00100`.
    - Điều kiện `user_agent CONTAINS ...` $\rightarrow$ Bật bit 3 ở Tập Complex. Bitmap Complex: `01100`.
- **Rule 4 (Slot 4)**: `country == 'VN'` AND `mcc == 5411`:
    - `country == 'VN'` $\rightarrow$ Bật bit 4 ở Tập Exact key `country:VN`. Bitmap lúc này: `10010`.
    - `mcc == 5411` $\rightarrow$ Bật bit 4 ở Tập Exact key `mcc:5411`. Bitmap lúc này: `00110`.
- **Rule 5 (Slot 5)**: `country == 'TH'` $\rightarrow$ Bật bit 5 ở Tập Exact tại key `country:TH`. Bitmap lúc này: `00001`.

**Trạng thái Index sau khi nạp 5 rules:**
- `exact_index.get("country:VN")` = `10010` (Rule 1, Rule 4)
- `exact_index.get("country:TH")` = `00001` (Rule 5)
- `exact_index.get("mcc:5411")` = `00110` (Rule 3, Rule 4)
- `complex_index` = `01100` (Rule 2, Rule 3)

**Khi Event đến:** `{ country: "TH", amount: 5000000, mcc: 9999, user_agent: "Safari", ip: "1.1.1.1", ... (95 trường khác) }`

1. **Tra cứu Tập 1:** Lấy các trường của Event để ghép key tra cứu.
   - Tra key `country:TH` $\rightarrow$ Lấy được chuỗi bit `00001`.
   - Tra key `mcc:9999` $\rightarrow$ Không có rule nào tìm key này nên trả về `00000`.
   - Tập `Exact_BM_List` = `[00001, 00000]`.
2. **Tra cứu Tập 2:** Lấy thẳng chuỗi bit `complex_index`.
   - Chuỗi `BM_complex` = `01100`.
3. **Phép OR (Gom ứng viên):**
   - `Candidates` = `00001` OR `00000` OR `01100` = `01101` (Rule 2, Rule 3, Rule 5 lọt vào danh sách ứng viên).
4. **Condition Tree thẩm định chi tiết các Candidates (01101):**
   - **Rule 1 và Rule 4:** **Bị gạt bỏ hoàn toàn** ở $O(1)$ từ bước Index (không tiêu tốn CPU của Condition Tree).
   - **Rule 2:** `amount > 10000000` (giá trị thực tế là 5M) $\rightarrow$ Loại.
   - **Rule 3:** Thẩm định loại do `mcc` thực tế là `9999` (khác `5411`).
   - **Rule 5:** `country == 'TH'` $\rightarrow$ Khớp (Trigger).

## 5. Cơ chế lưu trữ Rule trong Inverted Index (Roaring Bitmap & ID Allocator)

Để giải quyết bài toán biểu diễn tập hợp các Rule ID khớp với một điều kiện, mô hình chuẩn nhất là sử dụng **Roaring Bitmap Inverted Index** kết hợp với **ID Allocator (Dense Slot Manager)**.

### 5.1. Quản lý Rule ID, Thu hồi Bit Index

Vì các rule trong hệ thống được tạo, sửa, xóa liên tục, việc lục tìm và xóa tận gốc từng bit trong hàng ngàn key của Inverted Index khi xóa 1 rule là cực kỳ đắt đỏ. Giải pháp tối ưu là sử dụng **State Bitmap (Chuỗi bit trạng thái)**.

**Cấu trúc bảng quản lý Slot:**
- `slot_to_rule`: Array / Slice `[]RuleMetadata`. Chỉ số mảng chính là BitIndex ($0, 1, 2, \dots, N-1$).
- `rule_to_slot`: `HashMap<RuleID, uint32>`. Lưu mapping ngược để tra cứu nhanh.
- `free_slots_bitmap`: Một `RoaringBitmap` đóng vai trò lưu trạng thái của Slot.
  - **Bit = 1**: Slot đang trống (rule đã bị xóa hoặc chưa cấp phát).
  - **Bit = 0**: Slot đang được sử dụng bởi 1 rule.
- `max_allocated_index`: `uint32`. Vị trí bit cao nhất đã từng cấp phát.

**Cơ chế Thêm / Xóa / Sửa (CRUD):**

| Thao tác | Cơ chế thực thi | Chi phí |
| :--- | :--- | :--- |
| **Xóa rule (DELETE)** | 1. Tìm `bit_index = rule_to_slot[rule_id]`.<br>2. Bật bit đó thành `1` trong `free_slots_bitmap` (Đánh dấu là đã xóa).<br>3. **KHÔNG CẦN** chui vào Inverted Index để xóa rác. Rác sẽ bị mask out bằng toán tử `AND NOT(free_slots_bitmap)` khi event đến. | $O(1)$ |
| **Thêm rule mới (ADD)** | 1. Tìm 1 bit `1` trong `free_slots_bitmap` để cấp phát. Nếu không có, lấy `++max_allocated_index`.<br>2. Chuyển bit vừa được cấp phát về `0` (Đánh dấu đã sử dụng).<br>3. Rule mới duyệt qua các điều kiện của mình và update (thêm bit) vào Inverted Index. (*Lưu ý: Các bit rác cũ từ rule trước đó nếu còn sót lại ở key khác sẽ chỉ gây ra đánh giá sai ở tập Candidates, nhưng sẽ bị chặn đứng an toàn tại Condition Tree*). | $O(K)$ |
| **Cập nhật rule (UPDATE)**| Thực hiện DELETE theo `bit_index` cũ $\rightarrow$ ADD lại (hoặc tái sử dụng chính slot đó để cập nhật key mới). | $O(K)$ |

### 5.2. Vấn đề "Co giãn mảng bit" (Array Expansion)

Trong Roaring Bitmap, không cần cơ chế nhân đôi dung lượng (x2) thủ công:
- **Cách Roaring Bitmap hoạt động:** Không phân bổ một chuỗi bit liên tục dạng `byte[]` hay `long[]` từ $0$ đến $N$. Roaring chia dải 32-bit thành các chunk cố định $2^{16}$ (65,536 bit).
  - Nếu một chunk có ít hơn 4.096 bit: Lưu dạng Array (mảng các số `uint16`).
  - Nếu nhiều hơn 4.096 bit: Chuyển thành Bitset ($8\text{KB}$ cố định).
  - Nếu chứa các chuỗi bit liên tiếp: Nén dạng Run (RLE - Run-Length Encoding).
- **Mở rộng tự động:** Khi cấp phát một bit index lớn hơn (ví dụ từ bit 10,000 nhảy lên bit 70,000), Roaring Bitmap chỉ tạo thêm một container mới ở tầng trên (chỉ tốn vài byte con trỏ). Không có chi phí copy mảng hay reallocate toàn bộ bitmap như `java.util.BitSet`.

### 5.3. Thiết kế đồng bộ & Đảm bảo hiệu năng cao (Concurrency & Zero Downtime)

Trong các hệ thống throughput lớn, luồng matching event chạy liên tục hàng microsecond, không thể dùng Global Lock (Mutex) chặn quá trình match mỗi khi có rule thêm/sửa/xóa.

**Chiến lược: Copy-On-Write (COW) kết hợp Double Buffering**

```text
[ Write Thread (Worker) ]
   │
   ├─► Tạo bản clone của Inverted Index (hoặc chỉ clone các RoaringBitmap bị thay đổi)
   ├─► Thêm/Xóa bit trên bản nháp
   └─► Atomic Pointer Swap (Atomic.set) trỏ sang Index mới
                                  ▲
[ Read Threads (Workers) ]        │
   ├─► Event 1 ──► Đọc Index v1 ──┘
   └─► Event 2 ──► Đọc Index v2 (sau khi swap)
```

- **Tối ưu Matching Event:**
  - Lấy danh sách bitmap từ các key: `bm1 = index.get("mcc:5411")`, `bm2 = index.get("country:VN")`.
  - Thực hiện gộp bit: `RoaringBitmap.and(bm1, bm2)`. Quá trình này dùng tập lệnh SIMD (AVX-512 / AVX2) của CPU, xử lý hàng triệu bit chỉ mất vài nano-giây.
  - Duyệt qua kết quả: `iterator = resultBitmap.getIntIterator()`, lấy ra từng `bit_index` để trỏ tới Rule và thẩm định `condition_tree`.
