import sys
import os

# Ép Python tìm kiếm module trong thư mục chứa code (Phải đặt trên cùng)
sys.path.append('/opt/flink/usrlib/src')

import logging
import clickhouse_connect
import threading
import time
from functools import partial
from pyflink.common import Types, WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaSource, 
    KafkaOffsetsInitializer
)
from pyflink.common.serialization import SimpleStringSchema

# Sử dụng MapFunction thay vì ProcessFunction để tránh lỗi Timer của PyFlink 1.18
from pyflink.datastream.functions import MapFunction

import config
import transforms

log = logging.getLogger(__name__)

# ==============================================================================
# 1. CUSTOM SINK (MapFunction + Background Thread for Time-based Flush)
# ==============================================================================
class ClickHouseSink(MapFunction):
    def __init__(self, batch_size=1000, flush_interval_sec=5):
        self.client = None
        self.batch_size = batch_size
        self.flush_interval_sec = flush_interval_sec
        self.last_flush_time = time.time()
        self.running = True
        
        self.columns_map = {table: cols for table, cols, _ in transforms.TOPICS.values()}
        self.buffers = {table: [] for table in self.columns_map.keys()}
        
        # Không khởi tạo lock/thread ở đây để tránh lỗi pickle
        self.lock = None
        self.thread = None

    def open(self, runtime_context):
        self.client = clickhouse_connect.get_client(
            host=config.CLICKHOUSE_HOST,
            port=config.CLICKHOUSE_PORT,
            username=config.CLICKHOUSE_USER,
            password=config.CLICKHOUSE_PASSWORD,
            database=config.CLICKHOUSE_DB
        )
        log.info("Successfully connected to ClickHouse!")

        # Khởi tạo lock và thread an toàn trên TaskManager (sau khi đã pickle xong)
        self.lock = threading.Lock()
        self.last_flush_time = time.time()
        self.thread = threading.Thread(target=self._time_based_flusher, daemon=True)
        self.thread.start()
        log.info(f"Background flusher started (Interval: {self.flush_interval_sec}s)")

    def _time_based_flusher(self):
        # Luồng chạy ngầm: Cứ 1 giây kiểm tra 1 lần, nếu quá thời gian thì xả
        while self.running:
            time.sleep(1)
            if self.lock and (time.time() - self.last_flush_time >= self.flush_interval_sec):
                with self.lock:
                    self.flush_all()
                    self.last_flush_time = time.time()

    def map(self, value):
        if value is None:
            return True
            
        table_name, row_data = value
        if table_name not in self.buffers:
            return True

        # Bọc thao tác ghi vào buffer bằng Lock để tránh xung đột với luồng phụ
        if self.lock:
            with self.lock:
                self.buffers[table_name].append(row_data)
                
                # Xả dữ liệu nếu đủ lô (batch_size)
                if len(self.buffers[table_name]) >= self.batch_size:
                    self.flush_table(table_name)
                    self.last_flush_time = time.time()
                
        return True

    def flush_table(self, table_name):
        rows = self.buffers[table_name]
        if not rows:
            return
        try:
            self.client.insert(
                table=table_name,
                data=rows,
                column_names=self.columns_map[table_name]
            )
            log.info(f"Successfully inserted {len(rows)} rows into table {table_name}")
            self.buffers[table_name] = [] 
        except Exception as e:
            log.error(f"Error inserting into table {table_name}: {e}")
            # Xóa bộ đệm khi lỗi để tránh lặp vô hạn và tràn RAM (theo góp ý của reviewer)
            log.warning(f"Cleared {len(rows)} failed rows from buffer for table {table_name} to protect system memory.")
            self.buffers[table_name] = []

    def flush_all(self):
        for table_name in self.buffers.keys():
            self.flush_table(table_name)

    def close(self):
        self.running = False
        if self.client:
            log.info("Flushing remaining buffered data before shutdown...")
            if self.lock:
                with self.lock:
                    self.flush_all()
            self.client.close()
            log.info("ClickHouse connection closed.")


# ==============================================================================
# 2. MAIN
# ==============================================================================
def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1) 

    all_streams = []
    
    for topic in config.KAFKA_TOPICS:
        log.info(f"Configuring Kafka Source for topic: {topic}")
        
        source = KafkaSource.builder() \
            .set_bootstrap_servers(config.KAFKA_BROKER) \
            .set_topics(topic) \
            .set_group_id(config.KAFKA_GROUP_ID) \
            .set_starting_offsets(KafkaOffsetsInitializer.latest()) \
            .set_value_only_deserializer(SimpleStringSchema()) \
            .build()

        ds = env.from_source(
            source, 
            watermark_strategy=WatermarkStrategy.no_watermarks(), 
            source_name=f"Kafka_{topic}"
        )
        
        parsed_ds = ds.flat_map(partial(transforms.parse_kafka_message, topic))
        all_streams.append(parsed_ds)

    if not all_streams:
        log.error("No topics configured. Aborting job.")
        return
        
    merged_stream = all_streams[0]
    for stream in all_streams[1:]:
        merged_stream = merged_stream.union(stream)

    # Sử dụng MapFunction với batch_size=1000 và thời gian xả là 5 giây
    merged_stream.map(
        ClickHouseSink(batch_size=1000, flush_interval_sec=5), 
        output_type=Types.BOOLEAN()
    )

    log.info("Starting Job: Kafka to ClickHouse...")
    env.execute("Kafka_to_ClickHouse_Pipeline")


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    main()