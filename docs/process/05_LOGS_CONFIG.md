# 05 – Logging & Config Setup (`flink-jobs`)

> Cập nhật: **2026-09-25**  
> Phạm vi: module `flink-jobs`

---

## Tổng quan

Task này thêm hệ thống **logging có cấu trúc (SLF4J + Logback)** và **config loader đọc YAML + `.env`**  
cho module `flink-jobs`, đồng thời loại bỏ toàn bộ `System.out.println` khỏi production code.

---

## Các thay đổi đã thực hiện

### 1. `pom.xml`

| Thay đổi | Chi tiết |
|---|---|
| Nâng version SLF4J | `1.7.36` → `2.0.13` (scope: `provided` – dùng logging của Flink cluster) |
| Nâng version Logback | `1.2.11` → `1.5.6` (scope: `provided`) |
| Thêm SnakeYAML | `org.yaml:snakeyaml:2.2` – để `ConfigLoader` parse YAML |
| Thêm properties | `snakeyaml.version`, `slf4j.version`, `logback.version` |

> **Lý do `provided`**: Flink cluster đã bundle SLF4J + Logback, đặt `provided` tránh conflict khi submit fat JAR.

---

### 2. `ConfigLoader.java`

File được viết lại hoàn toàn với các tính năng:

- **Tự động tìm `.env`**: tìm theo thứ tự `DOTENV_PATH` env var → `./.env` → `../.env` → `../../.env`.
- **Parse `.env` đúng định dạng**: bỏ comment `#`, bỏ dòng trống, strip nháy đơn/đôi, bỏ inline comment.
- **Biến môi trường thật có độ ưu tiên cao nhất**: chỉ inject vào `System.setProperty` nếu key chưa tồn tại trong `System.getenv()`.
- **Hỗ trợ `${VAR:-default}` syntax** trong YAML (ngoài `${VAR}` đơn giản).
- **Helper methods**: `getString(path)`, `getString(path, default)`, `getInt`, `getLong` – đọc nested key dạng dấu chấm.
- **Thread-safe**: vẫn dùng `synchronized` + cache.

**Load order:**
```
OS env → .env (injected as System.setProperty) → YAML placeholder resolution → cache
```

---

### 3. `config.dev.yml` (làm lại)

Bổ sung đầy đủ các section theo thực tế hạ tầng:

```
app / postgres / kafka.plain / kafka.gssapi / kafka.topics / kafka.group /
kafka.stream / flink / logging
```

- `postgres.url` = `jdbc:postgresql://localhost:${POSTGRES_PORT:-5433}/...`
- `kafka.plain.bootstrap_servers` = `localhost:${KAFKA_PLAIN_PORT:-9092}`
- Tất cả passwords/secrets đều dùng `${VAR:-}` để lấy từ `.env`

---

### 4. `config.prod.yml` (làm mới)

Giống `dev` nhưng dùng địa chỉ **Docker internal**:

- `postgres.url` = `jdbc:postgresql://${SERVER_IP:-postgres}:5432/...`
- `kafka.plain.bootstrap_servers` = `kafka-plain:29092` (internal Docker)
- `kafka.gssapi.bootstrap_servers` = `kafka-gssapi:29094`
- `flink.parallelism` = 4
- `logging.log_dir` = `/opt/flink/log/jobs`

---

### 5. `logback.xml` (sửa lại)

Dựa theo `sample_logback.xml`, bổ sung:

| Thành phần | Mô tả |
|---|---|
| `<timestamp>` | Tạo key `startTime` để đặt tên thư mục log theo session |
| `STDOUT` | Console appender – format `yyyy-MM-dd HH:mm:ss.SSS \| LEVEL \| logger \| msg` |
| `FILE` | `RollingFileAppender` → `outputs/logs/{startTime}/flink-job.log` |
| Rolling policy | `SizeAndTimeBasedRollingPolicy` – 50 MB/file, max 7 ngày, cap 500 MB |
| Logger suppression | `org.apache.kafka=WARN`, `io.netty=WARN`, `org.apache.zookeeper=WARN` |
| App logger | `com.vdf.streaming=DEBUG` |
| `scan="true"` | Logback tự reload config mỗi 30 giây |

---

### 6. Java source files – thay `System.out.println` → SLF4J

| File | Thay đổi |
|---|---|
| `Main.java` | `System.out.println` → `LOG.info/error`; load bootstrap/topic/group từ `ConfigLoader` |
| `InvertedIndexManager.java` | `printDebugInfo()` → `LOG.debug` |
| `SourceVersionIndex.java` | `printDebugInfo()` → `LOG.debug` |
| `TestFlinkRuleJob.java` | Toàn bộ `System.out/err.println` → `LOG.info/warn/error`; load pgUrl, topic từ `ConfigLoader` |
| `TestKafkaRuleSource.java` | Toàn bộ `System.out/err.println` → `LOG.info/warn/error`; load pgUrl, topic từ `ConfigLoader` |
| `TestInvertedIndexLocal.java` | `System.out.println` → `LOG.info/warn/error` |
| `DynamicPassThroughJob.java` | Đã có LOG; bổ sung load `pgUrl`, `pgUser`, `pgPassword`, `tablePrefix`, `resultTopic`, `eventsStreamId` từ `ConfigLoader` |

---

## Kết quả kiểm tra

```bash
$ mvn compile -q
# → exit code 0, không có lỗi hay warning
```

```bash
$ grep -rn "System\.out\." src/main/java/
# → không có kết quả (đã xóa hoàn toàn)
```

---

## Lưu ý vận hành

- **Local dev**: copy `.env.example` → `.env`, điền `POSTGRES_PASSWORD` / `KAFKA_PLAIN_PASSWORD`. `ConfigLoader` sẽ tự tìm file này.
- **Flink cluster (prod)**: set `APP_ENV=prod` trong môi trường container → sẽ load `config.prod.yml`.
- **Override runtime**: mọi biến môi trường thật (`export VAR=value`) luôn override `.env`.
- **Log files**: tạo tại `outputs/logs/<startTime>/flink-job.log` (dev) hoặc `/opt/flink/log/jobs/` (prod).
