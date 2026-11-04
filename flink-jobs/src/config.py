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
CLICKHOUSE_HOST = "clickhouse"
CLICKHOUSE_PORT = 8123
CLICKHOUSE_USER = "default"
CLICKHOUSE_PASSWORD = "clickhouse" # Khớp với biến CLICKHOUSE_PASSWORD trong docker-compose
CLICKHOUSE_DB = "analytics"

# Thêm dòng này nếu đề bài bắt buộc phải có biến URL
CLICKHOUSE_URL = f"http://{CLICKHOUSE_USER}:{CLICKHOUSE_PASSWORD}@{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/{CLICKHOUSE_DB}"