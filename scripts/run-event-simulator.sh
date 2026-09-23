#!/usr/bin/env bash
# =============================================================================
# scripts/run-event-simulator.sh
# Khởi chạy Realtime Event Data Simulator cho 9 nguồn Kafka
#
# Ví dụ chạy:
#   1. Chạy bình thường (stream 2 trans/s):
#      ./scripts/run-event-simulator.sh --mode stream --rate 2
#
#   2. Chạy tải cao 10.000 events/giây (~1.200 trans/s, 4 threads):
#      ./scripts/run-event-simulator.sh --mode stream --rate 1200 --threads 4
#
#   3. Chạy benchmark tải tối đa phần cứng (unlimited rate):
#      ./scripts/run-event-simulator.sh --mode batch --count 10000 --threads 4 --rate 0 --dry-run
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

# -----------------------------------------------------------------------------
# Tự động lấy client.keytab mới nhất từ container KDC (nếu có Docker)
# -----------------------------------------------------------------------------
mkdir -p "$PROJECT_ROOT/docker/krb5"
if command -v docker >/dev/null 2>&1; then
    KDC_CONTAINER="$(docker ps --format '{{.Names}}' 2>/dev/null | grep -E '(^|_)kdc($|_)' | head -n 1 || true)"
    if [ -n "$KDC_CONTAINER" ]; then
        echo "🔑 Đang đồng bộ client.keytab mới nhất từ container '$KDC_CONTAINER'..."
        if docker cp "$KDC_CONTAINER:/var/lib/secret/client.keytab" "$PROJECT_ROOT/docker/krb5/client.keytab" 2>/dev/null; then
            echo "   ✅ Đã cập nhật keytab: docker/krb5/client.keytab"
        fi
    elif [ -n "${SSH_USER:-}" ] && [ "${SERVER_IP:-localhost}" != "localhost" ] && [ "${SERVER_IP:-localhost}" != "127.0.0.1" ]; then
        echo "🔑 Đang đồng bộ client.keytab từ remote server ${SERVER_IP} qua SSH (${SSH_USER})..."
        ssh -o ConnectTimeout=3 -o BatchMode=yes "${SSH_USER}@${SERVER_IP}" "docker cp kdc:/var/lib/secret/client.keytab -" > "$PROJECT_ROOT/docker/krb5/client.keytab" 2>/dev/null && \
            echo "   ✅ Đã cập nhật keytab từ remote server." || true
    fi
fi

# Tự động compile và chạy Java Main
mvn compile exec:java -pl data-simulator -f "$PROJECT_ROOT/pom.xml" \
    -Dexec.mainClass="com.vdf.streaming.event.EventSimulatorMain" \
    -Dexec.args="--env $ENV_FILE $*"

