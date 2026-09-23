#!/usr/bin/env bash
# =============================================================================
# scripts/run-event-simulator.sh
# Khởi chạy Realtime Event Data Simulator cho 9 nguồn Kafka
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
echo "🚀 Chạy Realtime Event Data Simulator"
echo "   Server IP: ${SERVER_IP:-localhost}"
echo "   Kafka Plain: ${SERVER_IP:-localhost}:${KAFKA_PLAIN_PORT:-${KAFKA_PORT:-9092}}"
echo "   Kafka GSSAPI: ${SERVER_IP:-localhost}:${KAFKA_GSSAPI_PORT:-9094}"
echo "=========================================================="

# Chuyển tiếp toàn bộ tham số dòng lệnh vào Java Main
mvn exec:java -pl data-simulator -f "$PROJECT_ROOT/pom.xml" \
    -Dexec.mainClass="com.vdf.streaming.event.EventSimulatorMain" \
    -Dexec.args="--env $ENV_FILE $*"

