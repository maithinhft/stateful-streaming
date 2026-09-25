#!/usr/bin/env bash
# =============================================================================
# scripts/submit-dynamic-passthrough-job.sh
# Submit Flink Job: DynamicPassThroughJob lên Flink JobManager qua REST API
#
# Cấu hình IP và Port được nạp từ file .env:
#   SERVER_IP="${SERVER_IP:-localhost}"
#   FLINK_PORT="${FLINK_PORT:-8081}"
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# -----------------------------------------------------------------------------
# 1. Đọc cấu hình từ file .env (nếu có)
# -----------------------------------------------------------------------------
ENV_FILE="${ENV_FILE:-$PROJECT_ROOT/.env}"
if [ -f "$ENV_FILE" ]; then
    echo "📄 Đang tải cấu hình từ: $ENV_FILE"
    # shellcheck disable=SC1090
    set -a
    source "$ENV_FILE"
    set +a
else
    echo "ℹ️  Không tìm thấy file .env tại $ENV_FILE, sử dụng cấu hình mặc định."
fi

# Cấu hình IP và Port Flink theo yêu cầu
SERVER_IP="${SERVER_IP:-localhost}"
FLINK_PORT="${FLINK_PORT:-8081}"

MAIN_CLASS="com.vdf.streaming.DynamicPassThroughJob"
JAR_PATH="$PROJECT_ROOT/flink-jobs/target/flink-jobs-1.0-SNAPSHOT.jar"
PARALLELISM=4
FORCE_BUILD=false
EXTRA_ARGS=()

# -----------------------------------------------------------------------------
# 2. Xử lý các tham số dòng lệnh
# -----------------------------------------------------------------------------
usage() {
    cat <<EOF
Sử dụng: $0 [TÙY CHỌN] [-- CÁC THAM SỐ CHO FLINK JOB...]

Tùy chọn:
  -s, --server <ip>         Địa chỉ IP của Flink server (mặc định từ .env: \$SERVER_IP hoặc localhost)
  -p, --port <port>         Cổng Web/REST của Flink JobManager (mặc định từ .env: \$FLINK_PORT hoặc 8081)
  -j, --jar <path>          Đường dẫn tới file JAR (mặc định: flink-jobs/target/flink-jobs-1.0-SNAPSHOT.jar)
  -c, --class <class>       Main entry-class (mặc định: com.vdf.streaming.DynamicPassThroughJob)
  --parallelism <n>         Độ song song của Job (mặc định: 4)
  -b, --build               Build lại file JAR trước khi submit (mvn clean package)
  -h, --help                Hiển thị hướng dẫn này

Các tham số bổ sung cho Flink Job (DynamicPassThroughJob):
  --events.stream.id <id>                 Stream ID metadata (mặc định: stream-events)
  --result.topic <topic>                  Topic kết quả (mặc định: result)
  --postgres.host <host>                  Host PostgreSQL metadata (mặc định: postgres)
  --postgres.port <port>                  Port PostgreSQL metadata (mặc định: 5432)
  --stream.metadata.discovery.interval.ms Chu kỳ quét metadata PostgreSQL (mặc định: 30000)
  --use.dynamic.source <true|false>       Sử dụng DynamicKafkaSource (mặc định: true)

Ví dụ:
  # Submit với cấu hình mặc định (tự động build JAR nếu chưa tồn tại)
  $0

  # Build lại JAR rồi submit
  $0 --build

  # Submit tới server từ xa
  $0 --server 192.168.1.100 --port 8081

  # Submit kèm tham số tùy biến cho Job
  $0 --parallelism 2 --events.stream.id stream-events --result.topic result
EOF
    exit 0
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        -s|--server)
            SERVER_IP="$2"
            shift 2
            ;;
        -p|--port)
            FLINK_PORT="$2"
            shift 2
            ;;
        -j|--jar)
            JAR_PATH="$2"
            shift 2
            ;;
        -c|--class)
            MAIN_CLASS="$2"
            shift 2
            ;;
        --parallelism)
            PARALLELISM="$2"
            shift 2
            ;;
        -b|--build)
            FORCE_BUILD=true
            shift
            ;;
        -h|--help)
            usage
            ;;
        --)
            shift
            while [[ $# -gt 0 ]]; do
                EXTRA_ARGS+=("$1")
                shift
            done
            break
            ;;
        *)
            EXTRA_ARGS+=("$1")
            shift
            ;;
    esac
done

# Chuẩn hóa client URL: nếu SERVER_IP là 0.0.0.0 (dùng cho Docker bind) thì client gọi localhost
FLINK_HOST="$SERVER_IP"
if [ "$FLINK_HOST" = "0.0.0.0" ]; then
    FLINK_HOST="localhost"
fi
FLINK_REST_URL="http://${FLINK_HOST}:${FLINK_PORT}"

# -----------------------------------------------------------------------------
# 3. Kiểm tra và Build file JAR nếu cần
# -----------------------------------------------------------------------------
if [ "$FORCE_BUILD" = true ] || [ ! -f "$JAR_PATH" ]; then
    echo "=========================================================="
    echo "🔨 Đang đóng gói Flink Job JAR với Maven..."
    echo "=========================================================="
    mvn clean package -pl flink-jobs -DskipTests -f "$PROJECT_ROOT/pom.xml"
    if [ ! -f "$JAR_PATH" ]; then
        echo "❌ Lỗi: Không tìm thấy file JAR sau khi build: $JAR_PATH" >&2
        exit 1
    fi
    echo "✅ Build JAR thành công: $JAR_PATH"
    echo ""
fi

# -----------------------------------------------------------------------------
# 4. Kiểm tra kết nối tới Flink JobManager
# -----------------------------------------------------------------------------
echo "=========================================================="
echo "🚀 Flink Job Submission: $MAIN_CLASS"
echo "   Endpoint:     $FLINK_REST_URL (SERVER_IP: $SERVER_IP, PORT: $FLINK_PORT)"
echo "   Main Class:   $MAIN_CLASS"
echo "   JAR File:     $JAR_PATH"
echo "   Parallelism:  $PARALLELISM"
if [ ${#EXTRA_ARGS[@]} -gt 0 ]; then
    echo "   Job Args:     ${EXTRA_ARGS[*]}"
fi
echo "=========================================================="
echo ""
echo "🔍 Đang kiểm tra kết nối tới Flink JobManager..."

OVERVIEW_JSON=$(curl -s -S --connect-timeout 5 "$FLINK_REST_URL/v1/overview" 2>/dev/null || echo "")

if [ -z "$OVERVIEW_JSON" ] || echo "$OVERVIEW_JSON" | grep -q '"errors"'; then
    echo "❌ Không thể kết nối tới Flink JobManager tại $FLINK_REST_URL!" >&2
    echo "" >&2
    echo "💡 Gợi ý khắc phục:" >&2
    echo "   1. Kiểm tra cluster Flink đã được khởi chạy chưa (ví dụ: docker compose up -d jobmanager taskmanager-1 taskmanager-2)" >&2
    echo "   2. Kiểm tra cấu hình SERVER_IP và FLINK_PORT trong file .env hoặc chỉ định qua: $0 -s <IP> -p <PORT>" >&2
    exit 1
fi

FLINK_VERSION=$(echo "$OVERVIEW_JSON" | python3 -c "import sys, json; data=json.load(sys.stdin); print(data.get('flink-version', 'N/A'))" 2>/dev/null || echo "N/A")
TOTAL_SLOTS=$(echo "$OVERVIEW_JSON" | python3 -c "import sys, json; data=json.load(sys.stdin); print(data.get('slots-total', 'N/A'))" 2>/dev/null || echo "N/A")
AVAILABLE_SLOTS=$(echo "$OVERVIEW_JSON" | python3 -c "import sys, json; data=json.load(sys.stdin); print(data.get('slots-available', 'N/A'))" 2>/dev/null || echo "N/A")
TASKMANAGERS=$(echo "$OVERVIEW_JSON" | python3 -c "import sys, json; data=json.load(sys.stdin); print(data.get('taskmanagers', 'N/A'))" 2>/dev/null || echo "N/A")

echo "✅ Đã kết nối thành công tới Flink Cluster!"
echo "   - Flink Version:    $FLINK_VERSION"
echo "   - TaskManagers:     $TASKMANAGERS"
echo "   - Total Slots:      $TOTAL_SLOTS"
echo "   - Available Slots:  $AVAILABLE_SLOTS"
echo ""

# Cảnh báo nếu available slots không đủ cho parallelism yêu cầu
if [[ "$AVAILABLE_SLOTS" =~ ^[0-9]+$ ]] && [ "$AVAILABLE_SLOTS" -lt "$PARALLELISM" ]; then
    echo "⚠️  Cảnh báo: Parallelism ($PARALLELISM) lớn hơn số slots còn trống ($AVAILABLE_SLOTS). Job có thể phải đợi slot."
    echo ""
fi

# -----------------------------------------------------------------------------
# 5. Upload file JAR lên Flink JobManager
# -----------------------------------------------------------------------------
echo "📤 Đang upload file JAR lên Flink cluster..."
UPLOAD_RESPONSE=$(curl -s -S -X POST -H "Expect:" -F "jarfile=@${JAR_PATH}" "${FLINK_REST_URL}/v1/jars/upload")

JAR_FILENAME=$(echo "$UPLOAD_RESPONSE" | python3 -c "import sys, json; data=json.load(sys.stdin); print(data.get('filename', ''))" 2>/dev/null || echo "")

if [ -z "$JAR_FILENAME" ]; then
    echo "❌ Upload JAR thất bại!" >&2
    echo "Chi tiết phản hồi từ server: $UPLOAD_RESPONSE" >&2
    exit 1
fi

JAR_ID="$(basename "$JAR_FILENAME")"
echo "✅ Upload JAR thành công!"
echo "   JAR ID: $JAR_ID"
echo ""

# -----------------------------------------------------------------------------
# 6. Kích hoạt chạy Flink Job
# -----------------------------------------------------------------------------
echo "⚡ Đang submit và kích hoạt Job: $MAIN_CLASS..."

RUN_PAYLOAD=$(python3 -c "
import sys, json

main_class = sys.argv[1]
parallelism = int(sys.argv[2])
args = sys.argv[3:]

# Đồng bộ tham số --parallelism vào ParameterTool nếu chưa có
if '--parallelism' not in args:
    args = ['--parallelism', str(parallelism)] + args

payload = {
    'entryClass': main_class,
    'parallelism': parallelism,
    'programArgsList': args
}
print(json.dumps(payload))
" "$MAIN_CLASS" "$PARALLELISM" ${EXTRA_ARGS[@]+"${EXTRA_ARGS[@]}"})

RUN_RESPONSE=$(curl -s -S -X POST \
    -H "Content-Type: application/json" \
    -d "$RUN_PAYLOAD" \
    "${FLINK_REST_URL}/v1/jars/${JAR_ID}/run")

JOB_ID=$(echo "$RUN_RESPONSE" | python3 -c "import sys, json; data=json.load(sys.stdin); print(data.get('jobid', ''))" 2>/dev/null || echo "")

if [ -z "$JOB_ID" ]; then
    echo "❌ Submit Job thất bại!" >&2
    echo "Chi tiết phản hồi từ server: $RUN_RESPONSE" >&2
    exit 1
fi

echo "🎉 Submit Job thành công!"
echo "   Job ID:  $JOB_ID"
echo "   Web UI:  ${FLINK_REST_URL}/#/job/${JOB_ID}"
echo ""

# -----------------------------------------------------------------------------
# 7. Kiểm tra trạng thái Job sau khi kích hoạt
# -----------------------------------------------------------------------------
echo "⏳ Đang kiểm tra trạng thái khởi động của Job..."
sleep 2

JOB_INFO=$(curl -s -S "${FLINK_REST_URL}/v1/jobs/${JOB_ID}" 2>/dev/null || echo "")
JOB_STATE=$(echo "$JOB_INFO" | python3 -c "import sys, json; data=json.load(sys.stdin); print(data.get('state', 'UNKNOWN'))" 2>/dev/null || echo "UNKNOWN")

case "$JOB_STATE" in
    RUNNING)
        echo "🟢 Trạng thái Job: RUNNING (Đang chạy bình thường)"
        ;;
    INITIALIZING|CREATED)
        echo "🟡 Trạng thái Job: $JOB_STATE (Đang khởi tạo các task)"
        ;;
    FAILED)
        echo "🔴 Trạng thái Job: FAILED (Job bị lỗi khi khởi động)"
        echo "💡 Vui lòng kiểm tra log chi tiết trên Flink Web UI hoặc JobManager container:"
        echo "   ${FLINK_REST_URL}/#/job/${JOB_ID}"
        ;;
    *)
        echo "ℹ️  Trạng thái Job: $JOB_STATE"
        ;;
esac

echo ""
echo "=========================================================="
echo "🎯 Để theo dõi Job trên Web UI, mở trình duyệt tại:"
echo "   ${FLINK_REST_URL}/#/job/${JOB_ID}"
echo "=========================================================="
