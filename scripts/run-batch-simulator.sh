#!/usr/bin/env bash
# =============================================================================
# scripts/run-batch-simulator.sh
# Khởi chạy Batch Event Data Simulator cho 7 nguồn batch (B1, B2, B4, B5)
#
# Ví dụ chạy:
#   1. Sinh tất cả 7 datasets (dry-run):
#      ./scripts/run-batch-simulator.sh --dry-run
#
#   2. Sinh và đẩy lên Kafka thật:
#      ./scripts/run-batch-simulator.sh
#
#   3. Sinh mẻ batch có 10% dữ liệu lỗi / bẩn để test DLQ (dlq_batch_events):
#      ./scripts/run-batch-simulator.sh --error-rate 10
#
#   4. Sinh mẻ batch bị Data Skew (80% bản ghi dồn vào top 5% Hot MSISDNs, mỗi dataset 100 bản ghi):
#      ./scripts/run-batch-simulator.sh --skew-rate 80 --records-per-dataset 100
#
#   5. Sinh kết hợp cả Data Skew và 15% lỗi:
#      ./scripts/run-batch-simulator.sh --skew-rate 70 --error-rate 15 --records-per-dataset 50
#
#   6. Ghi ra file JSONL:
#      ./scripts/run-batch-simulator.sh --output-dir local/data/batch/ --dry-run
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Đọc cấu hình từ .env nếu có
ENV_FILE="${ENV_FILE:-$PROJECT_ROOT/.env}"
if [ -f "$ENV_FILE" ]; then
    echo "📄 Đang tải cấu hình từ: $ENV_FILE"
    set -a
    # shellcheck disable=SC1090
    source "$ENV_FILE"
    set +a
fi

echo "=========================================================="
echo "📦 Chạy Batch Event Data Simulator (7 Batch Sources)"
echo "   Server IP: ${SERVER_IP:-localhost}"
echo "   Kafka Plain: ${SERVER_IP:-localhost}:${KAFKA_PLAIN_PORT:-${KAFKA_PORT:-9092}}"
echo "=========================================================="

# Tự động compile và chạy Java Main
mvn compile exec:java -pl data-simulator -f "$PROJECT_ROOT/pom.xml" \
    -Dexec.mainClass="com.vdf.streaming.event.BatchSimulatorMain" \
    -Dexec.args="--env $ENV_FILE $*"
