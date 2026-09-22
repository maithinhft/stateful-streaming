# Chi tiết cơ chế lưu trữ IN_DATASET

## Cơ chế lưu trữ và băm Tuple Dataset (Hashing & Encoding)

### 1. Băm bằng Length-prefixed Binary Encoding (Nguyên lý & Lý do sử dụng)
Khi ghép nhiều trường thành một bộ (tuple), lỗi logic phổ biến nhất là dùng ký tự nối (như dấu `_`). Nếu giá trị của trường chứa chính ký tự đó, các bộ giá trị khác nhau sẽ bị dính thành một chuỗi giống hệt nhau (ví dụ: `("A", "B_C")` và `("A_B", "C")` đều ra `"A_B_C"`).

Để triệt tiêu hoàn toàn rủi ro này mà không tốn CPU xử lý chuỗi:
- **Cấu trúc**: Mỗi trường được biểu diễn bằng `[Độ dài mảng byte (2 bytes)]` + `[Dữ liệu mảng byte thực tế]`.
- **Cơ chế ghép**:
  $$\text{Encoded Bytes} = [\text{len}_1][\text{bytes}_1] + [\text{len}_2][\text{bytes}_2] + \dots$$
- **Ví dụ**: Cặp `("TABLET", 5411)`:
  - **Trường 1** (`"TABLET"`): Ghi 2 byte độ dài mang giá trị `6`, liền sau là 6 byte của chuỗi `"TABLET"`.
  - **Trường 2** (`5411`): Ghi 2 byte độ dài mang giá trị `4`, liền sau là 4 byte nhị phân của số nguyên `5411`.
- **Ưu điểm**: Khi đưa vào hàm băm, engine đọc theo ranh giới byte chính xác tuyệt đối mà không cần quan tâm chuỗi bên trong chứa ký tự gì, không cần escape và không cần cấp phát chuỗi tạm (zero object allocation) trên JVM.

### 2. Thiết kế Key - Value trên RocksDB
Thay vì chỉ dùng một mảng HashSet tĩnh trên RAM, hệ thống dùng cấu trúc Key-Value lưu vào RocksDB kết hợp với **nguyên lý Separate Chaining (Mở chuỗi)** kinh điển của Hash Table. Thiết kế này giúp hệ thống chống lại hoàn toàn lỗi sai sót dữ liệu do đụng độ hàm băm (Hash Collision), dù tỷ lệ xảy ra vô cùng hiếm hoi.

#### a. Cấu trúc Key (16 Bytes cố định)
Bất kể tuple có 2 trường hay 5 trường, chuỗi dài 10 byte hay 100 byte, nó đều được tính toán bằng `xxHash64` và quy đổi về duy nhất một mã băm 8 bytes. Đồng thời, tên của tập dữ liệu (`dataset_id`) cũng được đưa qua `xxHash64` ép về số nguyên 8 bytes.

Định dạng khóa cuối cùng lưu xuống DB có độ dài **16 bytes**:
```text
|<------------------------- 16 BYTES ---------------------------->|
+--------------------------------+--------------------------------+
|     dataset_id (8 bytes)       |       tuple_hash (8 bytes)     |
|   (ID số nguyên tự tăng hoặc   |  (xxHash64 của mảng byte tuple |
|   xxHash64 của tên dataset)    |      length-prefixed)          |
+--------------------------------+--------------------------------+
```

#### b. Cấu trúc Value (Binary Bucket Format)
Thay vì chỉ lưu 1 cờ `true` hay 1 chuỗi byte đơn lẻ, Value đóng vai trò như một **Bucket** chứa danh sách các mảng byte gốc (`List<byte[]>`) có cùng mã băm 64-bit đó. 
Để không phải serialize/deserialize JSON hay Java Object phức tạp, toàn bộ Bucket được đóng gói thành một mảng byte phẳng dạng Length-prefixed lồng nhau:

```text
|<------------------------------ VALUE BUCKET --------------------------------------------->|
+---------------+---------------------+---------------+-------------------------------------+
| Số lượng tuple| Độ dài Tuple 1 (L1) | Dữ liệu gốc 1 | Độ dài Tuple 2 (L2) | Dữ liệu gốc 2 |
|   (1 byte)    |      (2 bytes)      |   (L1 bytes)  |      (2 bytes)      |   (L2 bytes)  |
+---------------+---------------------+---------------+-------------------------------------+
```
- **Byte đầu tiên (`count`)**: Số lượng tuple gốc đang chia sẻ chung mã hash này.
  - Với hàm xxHash64, gần như trong trường hợp, `count = 1`.
  - Chỉ khi có đụng độ thực sự xảy ra, `count = 2` (hoặc lớn hơn).
- **Các byte tiếp theo**: Lặp lại `[độ dài (2 bytes)]` + `[mảng byte length-prefixed gốc]`.

### 3. Quy trình Đọc / Ghi chống đụng độ

#### Quy trình Ghi (Broadcast Stream - UPSERT Logic)
Khi nhận một bộ tuple mới cần nạp vào State:
1. Tính **Key 16 bytes**: `[dataset_id (8B)] + [tuple_hash (8B)]`.
2. Kiểm tra xem Key này đã tồn tại trong State chưa:
   - **Nếu chưa có**: Tạo Value Bucket mới với `count = 1` đi kèm dữ liệu gốc của tuple, gọi `state.put(key, value)`.
   - **Nếu đã có (Phát hiện đụng độ hoặc ghi đè)**:
     - Đọc Value cũ lên, giải mã Bucket và duyệt qua các phần tử bên trong.
     - Nếu chuỗi byte mới đã tồn tại trong bucket $\rightarrow$ Bỏ qua (đây là dữ liệu trùng lặp).
     - Nếu chuỗi byte mới chưa tồn tại (đây là đụng độ hash thật sự) $\rightarrow$ Tăng `count` lên `count + 1`, append thêm `[độ dài mới] + [mảng byte mới]` vào đuôi mảng Bucket và ghi cập nhật lại vào State.

#### Quy trình Đọc và Kiểm tra (Event Stream - O(1))
Khi event từ luồng giao dịch/sự kiện đổ về:
```text
[Event đến] 
     │
     ▼
[Encode mảng byte gốc + Tính xxHash64] ──► Dựng Key 16 bytes
     │
     ▼
[Gọi state.get(Key)]
     │
     ├── Trả về null ────────► DROP EVENT NGAY
     │
     └── Có Value (Đọc Bucket)
           │
           ▼
     [Duyệt qua các tuple gốc trong Bucket]
           │
           ├── Có 1 tuple khớp (Arrays.equals) ──► PASS TRIGGER (Khớp chính xác 100%)
           │
           └── Không có tuple nào khớp ─────────► DROP EVENT (Chặn đứng đụng độ)
```
Quy trình này xử lý siêu tốc vì hầu hết các event rác sẽ bị đánh rớt ngay bước `state.get()` trả về `null`. Chỉ khi mã băm vô tình trùng khớp, hệ thống mới đọc Bucket lên và so sánh từng byte một (`Arrays.equals`) để tìm ra sự thật gốc rễ, đảm bảo chính xác $100\%$ không bị lọt event sai lệch.
