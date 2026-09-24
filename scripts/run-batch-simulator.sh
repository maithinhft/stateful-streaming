#!/usr/bin/env bash
# =============================================================================
# scripts/run-batch-simulator.sh
# Khởi chạy Batch Event Data Simulator cho 7 nguồn batch (B1, B2, B4, B5)
#
# Ví dụ chạy:
#   1. Sinh tất cả 7 datasets (dry-run):
#      ./scripts/run-batch-simulator.sh --dry-run
#
#   2. Sinh chỉ dataset blacklist + simfarm (B4):
#      ./scripts/run-batch-simulator.sh --datasets blacklist_qtrr,simfarm_3_tram --dry-run
#
#   3. Sinh và đẩy lên Kafka thật:
#      ./scripts/run-batch-simulator.sh
#
#   4. Ghi ra file JSONL:
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
