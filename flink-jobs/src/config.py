import os
# src/config.py

# ==========================================
# CẤU HÌNH KAFKA
# ==========================================
KAFKA_BROKER = "kafka:29092" 
KAFKA_GROUP_ID = "flink_consumer_group"

# Danh sách 6 topic khớp với file init/kafka/create-topics.sh
KAFKA_TOPICS = [
    "equities.prices",
    "crypto.trades",
    "crypto.market",
    "forex.rates",
    "macro.indicators",
    "fundamentals.filings"
]

# ==========================================
# CẤU HÌNH CLICKHOUSE
# ==========================================
# ClickHouse
CLICKHOUSE_HOST = "clickhouse" # Host vẫn giữ nguyên vì gọi qua tên service Docker
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_HTTP_PORT", "8123"))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "clickhouse") # Tự động lấy từ .env
CLICKHOUSE_DB = os.getenv("CLICKHOUSE_DB", "analytics")

# Thêm dòng này nếu đề bài bắt buộc phải có biến URL
#CLICKHOUSE_URL = f"http://{CLICKHOUSE_USER}:{CLICKHOUSE_PASSWORD}@{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/{CLICKHOUSE_DB}"