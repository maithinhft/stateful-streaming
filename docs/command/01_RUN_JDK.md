# Hướng dẫn chạy Java / Maven trong Project

> **Cập nhật:** 2026-09-24

## 1. Vị trí Maven

Maven **không** nằm trong `$PATH` mặc định trên máy này. Sử dụng đường dẫn tuyệt đối:

```bash
# Maven wrapper (từ .m2)
MVN="/home/tmkhoa1812/.m2/wrapper/dists/apache-maven-3.9.11/a2d47e15/bin/mvn"

# Hoặc Maven từ IntelliJ
MVN="/home/tmkhoa1812/.local/share/JetBrains/Toolbox/apps/intellij-idea/plugins/maven/lib/maven3/bin/mvn"
```

**Ghi nhớ:** Luôn dùng `$MVN` thay vì `mvn` khi chạy từ terminal.

## 2. Java Version

```bash
$ java -version
java version "17.0.18" 2026-01-20 LTS

$ which java
/usr/bin/java
```

## 3. Các lệnh thường dùng

### 3.1 Compile toàn bộ project

```bash
cd /home/tmkhoa1812/CodingSpace/Projects/VDT/2026/Phase2/StatefulStreamingNew/stateful-streaming

# Compile không chạy test
$MVN clean compile -DskipTests=true

# Compile chỉ module flink-jobs
$MVN -f flink-jobs/pom.xml compile
```

### 3.2 Chạy Test

```bash
# Chạy tất cả test trong flink-jobs
$MVN -f flink-jobs/pom.xml test -DskipTests=false

# Chạy 1 test class cụ thể
$MVN -f flink-jobs/pom.xml test -DskipTests=false \
    -Dtest=com.vdf.streaming.RuleCdcPipelineMockTest

# Chạy 1 nested class (test group) — dùng \$ để escape ký tự $
$MVN -f flink-jobs/pom.xml test -DskipTests=false \
    -Dtest="com.vdf.streaming.RuleCdcPipelineMockTest\$VersionCheckTests"

# Chạy 1 test method cụ thể
$MVN -f flink-jobs/pom.xml test -DskipTests=false \
    -Dtest="com.vdf.streaming.RuleCdcPipelineMockTest#testParseCdcCreateEvent"
```

> **Lưu ý:** POM có `<skipTests>true</skipTests>` mặc định. Phải thêm `-DskipTests=false` khi chạy test.

### 3.3 Build Fat JAR (cho Flink Cluster)

```bash
$MVN -f flink-jobs/pom.xml clean package -DskipTests=true
# Output: flink-jobs/target/flink-jobs-1.0-SNAPSHOT.jar
```

### 3.4 Chạy Data Simulator

```bash
# Sinh rules JSON
$MVN -f data-simulator/pom.xml exec:java \
    -Dexec.mainClass="com.vdf.streaming.rule.RuleSimulatorMain"

# Ghi rules vào PostgreSQL
$MVN -f data-simulator/pom.xml exec:java \
    -Dexec.mainClass="com.vdf.streaming.rule.RulePostgresWriter"

# Sinh events
$MVN -f data-simulator/pom.xml exec:java \
    -Dexec.mainClass="com.vdf.streaming.event.EventSimulatorMain"
```

### 3.5 Chạy Flink Job local (qua exec-maven-plugin)

```bash
# Chạy Main.java (test Kafka rule matcher)
$MVN -f flink-jobs/pom.xml exec:java \
    -Dexec.mainClass="com.vdf.streaming.Main"

# Chạy TestFlinkRuleJob (nối Kafka CDC)
$MVN -f flink-jobs/pom.xml exec:java \
    -Dexec.mainClass="com.vdf.streaming.test_local.TestFlinkRuleJob"
```

> **Quan trọng:** `exec-maven-plugin` cần `<includeProvidedDependencies>true</includeProvidedDependencies>` để bao gồm Flink libs (scope=provided) vào classpath khi chạy local.

## 4. Lỗi thường gặp

| Lỗi | Nguyên nhân | Giải pháp |
|-----|-------------|-----------|
| `mvn: command not found` | Maven không trong PATH | Dùng đường dẫn tuyệt đối `$MVN` |
| `Tests run: 0` | `<skipTests>true</skipTests>` trong POM | Thêm `-DskipTests=false` |
| `ClassNotFoundException: Flink...` | Flink scope=provided | Dùng `exec:java` thay vì `java -jar` |
| `Unable to autodetect 'javac' path` | Warning vô hại | Bỏ qua, compile vẫn chạy đúng |
