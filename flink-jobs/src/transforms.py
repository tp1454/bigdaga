import json
import logging
from datetime import date, datetime, timezone

log = logging.getLogger(__name__)

# ==========================================
# 1. CÁC HÀM TIỆN ÍCH CHUYỂN ĐỔI THỜI GIAN
# ==========================================
def _ms_to_dt(ms):
    if not ms:
        return None
    return datetime.fromtimestamp(int(ms) / 1000, tz=timezone.utc)

def _iso_to_dt(s):
    if not s:
        return None
    return datetime.fromisoformat(str(s).replace("Z", "+00:00"))

def _to_date(s):
    if not s:
        return None
    return date.fromisoformat(str(s)[:10])


# ==========================================
# 2. HÀM XỬ LÝ ĐẶC BIỆT (1 JSON -> NHIỀU DÒNG)
# ==========================================
def _sec_rows(d):
    """Flatten one SEC submission into one row per recent filing."""
    recent = d.get("filings") or {}
    accs = recent.get("accessionNumber") or []
    dates = recent.get("filingDate") or []
    forms = recent.get("form") or []
    docs = recent.get("primaryDocument") or []
    
    # Sử dụng yield thay vì return mảng, để Flink có thể dùng flat_map
    for i, acc in enumerate(accs):
        yield (
            d.get("cik"), 
            d.get("entity_name"), 
            d.get("sic"), 
            acc,
            _to_date(dates[i] if i < len(dates) else None),
            forms[i] if i < len(forms) else None,
            docs[i] if i < len(docs) else None,
            d.get("source"),
        )


# ==========================================
# 3. TỪ ĐIỂN ÁNH XẠ TOPICS (MIRROR TỪ CONSUMER)
# ==========================================
# Cấu trúc: "tên_topic": ("tên_bảng_clickhouse", ["các_cột"], hàm_xử_lý)
# Các hàm lambda được bọc trong iter([...]) để biến 1 dòng dữ liệu thành 1 Generator (giống yield)
TOPICS = {
    "equities.prices": (
        "equities_prices",
        ["symbol", "ts", "open", "high", "low", "close", "volume"],
        lambda d: iter([(
            d["symbol"], 
            _iso_to_dt(d["ts"]), 
            float(d["open"]),
            float(d["high"]),
            float(d["low"]), 
            float(d["close"]), 
            int(d["volume"])
        )]),
    ),
    "crypto.trades": (
        "crypto_trades",
        ["symbol", "timestamp", "price", "quantity", "trade_id", "source"],
        lambda d: iter([(
            d["symbol"], 
            _ms_to_dt(d["timestamp"]), 
            float(d["price"]),
            float(d["quantity"]), 
            int(d["trade_id"]), 
            d["source"]
        )]),
    ),
    "crypto.market": (
        "crypto_market_data",
        ["symbol", "timestamp", "price", "market_cap", "volume_24h", "source"],
        lambda d: iter([(
            d["symbol"], 
            _ms_to_dt(d["timestamp"]), 
            float(d["price"]),
            d.get("market_cap"), 
            d.get("volume_24h"), 
            d["source"]
        )]),
    ),
    "forex.rates": (
        "forex_rates",
        ["pair", "timestamp", "rate", "source"],
        lambda d: iter([(
            d["pair"], 
            _ms_to_dt(d["timestamp"]), 
            float(d["rate"]), 
            d["source"]
        )]),
    ),
    "macro.indicators": (
        "macro_indicators",
        ["indicator", "timestamp", "value", "source"],
        lambda d: iter([(
            d["indicator"], 
            _to_date(d["timestamp"]), 
            float(d["value"]), 
            d["source"]
        )]),
    ),
    "fundamentals.filings": (
        "company_filings",
        ["cik", "entity_name", "sic", "accession_number", "filing_date", "form", "primary_document", "source"],
        _sec_rows,
    ),
}


# ==========================================
# 4. HÀM CHÍNH DÀNH CHO FLINK GỌI (FLAT_MAP)
# ==========================================
def parse_kafka_message(topic, raw_json):
    """
    Hàm này được sử dụng trong PyFlink FlatMap.
    Nhận vào tên Topic và chuỗi JSON thô.
    Trả về (yield) các Tuple: (Tên_Bảng, Dòng_Dữ_Liệu)
    """
    if topic not in TOPICS:
        return

    table_name, _, transform_fn = TOPICS[topic]
    
    try:
        record = json.loads(raw_json)
        # Gọi hàm biến đổi (Lambda hoặc _sec_rows) để lấy ra từng dòng dữ liệu
        for row in transform_fn(record):
            # Trả về tên bảng kèm theo dữ liệu để ClickHouseSink biết cần ghi vào đâu
            yield (table_name, row)
            
    except (KeyError, TypeError, ValueError, json.JSONDecodeError) as e:
        # Xử lý lỗi y hệt như file consumers gốc: Ghi log và bỏ qua bản ghi lỗi
        log.warning("Skip bad record on %s: %s", topic, e)
        return